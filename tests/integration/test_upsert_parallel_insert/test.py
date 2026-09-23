from concurrent.futures import ThreadPoolExecutor

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", stay_alive=True)
settings = {
    "allow_experimental_unique_key": 1,
    "async_insert": 0,
    "max_threads": 4,
    "max_insert_threads": 4,
    "max_block_size": 1024,
    "max_insert_block_size": 1024,
    "min_insert_block_size_rows": 1024,
    "min_insert_block_size_bytes": 0,
}


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.mark.parametrize("versioned", [False, True])
def test_out_of_order_preparation_preserves_commit_order(versioned):
    table = "parallel_preparation_order"
    engine = "UpsertMergeTree(ver)" if versioned else "UpsertMergeTree"
    node.query(
        f"CREATE TABLE {table} (id UInt64,ver UInt64,value UInt64) "
        f"ENGINE={engine} ORDER BY value UNIQUE KEY id",
        settings=settings,
    )
    points = ["upsert_pause_first_preparation", "upsert_pause_ready_preparation"]
    for point in points:
        node.query(f"SYSTEM ENABLE FAILPOINT {point}")
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(
                node.query,
                f"INSERT INTO {table} SELECT number%1024,1,number FROM numbers(8192)",
                settings=settings,
            )
            try:
                for point in points:
                    node.query(f"SYSTEM WAIT FAILPOINT {point} PAUSE", timeout=30)
                # A later unit is already prepared while the first unit is paused.
                assert node.query(f"SELECT count() FROM {table}") == "0\n"
            finally:
                for point in reversed(points):
                    node.query(f"SYSTEM NOTIFY FAILPOINT {point}")
            future.result(timeout=60)
        assert node.query(
            f"SELECT count(),uniqExact(id),countIf(value!=id+7168) FROM {table}"
        ) == "1024\t1024\t0\n"
    finally:
        for point in points:
            node.query(f"SYSTEM DISABLE FAILPOINT {point}")
        node.query(f"DROP TABLE {table} SYNC")


def test_preparation_exception_does_not_fence_table():
    table = "parallel_preparation_exception"
    node.query(
        f"CREATE TABLE {table} (id UInt64,value UInt64) "
        "ENGINE=UpsertMergeTree ORDER BY id UNIQUE KEY id",
        settings=settings,
    )
    try:
        node.query("SYSTEM ENABLE FAILPOINT upsert_throw_during_preparation")
        error = node.query_and_get_error(
            f"INSERT INTO {table} SELECT number,number FROM numbers(16384)",
            settings=settings,
        )
        assert "FAULT_INJECTED" in error
        node.query("SYSTEM DISABLE FAILPOINT upsert_throw_during_preparation")
        node.query(
            f"INSERT INTO {table} SELECT number,number+1 FROM numbers(16384)",
            settings=settings,
        )
        assert node.query(
            f"SELECT count(),uniqExact(id),countIf(value!=id+1) FROM {table}"
        ) == "16384\t16384\t0\n"
        node.restart_clickhouse()
        assert node.query(f"SELECT count(),countIf(value!=id+1) FROM {table}") == "16384\t0\n"
    finally:
        node.query("SYSTEM DISABLE FAILPOINT upsert_throw_during_preparation")
        node.query(f"DROP TABLE {table} SYNC")


def test_cancel_joins_preparations_without_publication():
    table = "parallel_preparation_cancel"
    query_id = "upsert_parallel_cancel"
    node.query(
        f"CREATE TABLE {table} (id UInt64,value UInt64) "
        "ENGINE=UpsertMergeTree ORDER BY id UNIQUE KEY id",
        settings=settings,
    )
    points = ["upsert_pause_first_preparation", "upsert_pause_ready_preparation"]
    for point in points:
        node.query(f"SYSTEM ENABLE FAILPOINT {point}")
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(
                node.query_and_get_error,
                f"INSERT INTO {table} SELECT number,number FROM numbers(16384)",
                settings=settings,
                query_id=query_id,
            )
            try:
                for point in points:
                    node.query(f"SYSTEM WAIT FAILPOINT {point} PAUSE", timeout=30)
                node.query(f"KILL QUERY WHERE query_id='{query_id}' ASYNC")
            finally:
                for point in reversed(points):
                    node.query(f"SYSTEM NOTIFY FAILPOINT {point}")
            assert "QUERY_WAS_CANCELLED" in future.result(timeout=60)
        assert node.query(f"SELECT count() FROM {table}") == "0\n"
        node.query(f"INSERT INTO {table} VALUES (1,1)", settings=settings)
        assert node.query(f"SELECT * FROM {table}") == "1\t1\n"
    finally:
        for point in points:
            node.query(f"SYSTEM DISABLE FAILPOINT {point}")
        node.query(f"DROP TABLE {table} SYNC")


def test_parallel_commit_failure_and_multiunit_retry():
    table = "parallel_preparation_recovery"
    node.query(
        f"CREATE TABLE {table} (id UInt64,value UInt64) "
        "ENGINE=UpsertMergeTree ORDER BY value UNIQUE KEY id",
        settings=settings,
    )
    query = (
        f"INSERT INTO {table} SETTINGS insert_deduplication_token='multiunit-recovery' "
        "SELECT number%1024,number FROM numbers(16384)"
    )
    try:
        node.query("SYSTEM ENABLE FAILPOINT upsert_log_throw_after_sync")
        assert "FAULT_INJECTED" in node.query_and_get_error(query, settings=settings)
        node.restart_clickhouse(kill=True)
        assert node.query(f"SELECT count() FROM {table}") == "1024\n"
        node.query(query, settings=settings)
        expected = "1024\t1024\t0\n"
        check = f"SELECT count(),uniqExact(id),countIf(value!=id+15360) FROM {table}"
        assert node.query(check) == expected
        node.restart_clickhouse(kill=True)
        node.query(query, settings={**settings, "max_insert_threads": 1})
        assert node.query(check) == expected
    finally:
        node.query("SYSTEM DISABLE FAILPOINT upsert_log_throw_after_sync")
        node.query(f"DROP TABLE {table} SYNC")


def test_concurrent_clients_preserve_versions_and_replay():
    table = "parallel_preparation_clients"
    node.query(
        f"CREATE TABLE {table} (id UInt64,ver UInt64,value UInt64) "
        "ENGINE=UpsertMergeTree(ver) ORDER BY value UNIQUE KEY id",
        settings=settings,
    )

    def insert(version):
        node.query(
            f"INSERT INTO {table} SETTINGS insert_deduplication_token='client-{version}' "
            f"SELECT number%1024,{version},number+{version}*1000000 FROM numbers(8192)",
            settings=settings,
        )

    try:
        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(insert, range(1, 5)))
        assert node.query(
            f"SELECT count(),uniqExact(id),countIf(ver!=4 OR value!=id+4007168) FROM {table}"
        ) == "1024\t1024\t0\n"
        node.query(f"INSERT INTO {table} VALUES (0,5,999)", settings=settings)
        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(insert, range(1, 5)))
        assert node.query(f"SELECT ver,value FROM {table} WHERE id=0") == "5\t999\n"
        assert node.query(f"SELECT count(),uniqExact(id) FROM {table}") == "1024\t1024\n"
    finally:
        node.query(f"DROP TABLE {table} SYNC")


def test_preparation_memory_limit_does_not_fence_table():
    table = "parallel_preparation_memory"
    node.query(
        f"CREATE TABLE {table} (id UInt64,value String) "
        "ENGINE=UpsertMergeTree ORDER BY id UNIQUE KEY id",
        settings=settings,
    )
    limited = {
        **settings,
        "max_memory_usage": 8 * 1024 * 1024,
        "max_threads_min_free_memory_per_thread": 0,
        "max_insert_threads_min_free_memory_per_thread": 0,
    }
    try:
        error = node.query_and_get_error(
            f"INSERT INTO {table} SELECT number,repeat(toString(number%10),4096) "
            "FROM numbers(32768)",
            settings=limited,
        )
        assert "MEMORY_LIMIT_EXCEEDED" in error
        node.query(f"INSERT INTO {table} VALUES (99999,'ok')", settings=settings)
        assert node.query(f"SELECT value FROM {table} WHERE id=99999") == "ok\n"
    finally:
        node.query(f"DROP TABLE {table} SYNC")
