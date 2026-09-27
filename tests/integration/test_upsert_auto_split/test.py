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
    "max_threads_min_free_memory_per_thread": 0,
    "max_insert_threads_min_free_memory_per_thread": 0,
    "max_block_size": 131072,
    "min_insert_block_size_rows": 131072,
    "min_insert_block_size_bytes": 0,
    "use_strict_insert_block_limits": 0,
}


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def create_table(table, versioned):
    engine = "UpsertMergeTree(ver)" if versioned else "UpsertMergeTree"
    node.query(
        f"CREATE TABLE {table} (id UInt64, ver UInt64, value UInt64, payload String) "
        f"ENGINE={engine} ORDER BY value UNIQUE KEY id",
        settings=settings,
    )


def insert_query(table):
    return (
        f"INSERT INTO {table} SETTINGS insert_deduplication_token='split-retry' "
        "SELECT number%1024,1,number,repeat(toString(number%10),1024) FROM numbers(65536)"
    )


@pytest.mark.parametrize("versioned", [False, True])
def test_split_preparations_publish_in_input_order(versioned):
    table = "split_preparation_order"
    create_table(table, versioned)
    points = ["upsert_pause_first_preparation", "upsert_pause_ready_preparation"]
    for point in points:
        node.query(f"SYSTEM ENABLE FAILPOINT {point}")
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(node.query, insert_query(table), settings=settings)
            try:
                for point in points:
                    node.query(f"SYSTEM WAIT FAILPOINT {point} PAUSE", timeout=30)
                assert node.query(f"SELECT count() FROM {table}") == "0\n"
            finally:
                for point in reversed(points):
                    node.query(f"SYSTEM NOTIFY FAILPOINT {point}")
            future.result(timeout=60)
        assert node.query(
            f"SELECT count(),uniqExact(id),countIf(value!=id+64512) FROM {table}"
        ) == "1024\t1024\t0\n"
    finally:
        for point in points:
            node.query(f"SYSTEM DISABLE FAILPOINT {point}")
        node.query(f"DROP TABLE {table} SYNC")


def test_split_partial_commit_recovery_and_retry():
    table = "split_partial_recovery"
    create_table(table, False)
    try:
        node.query("SYSTEM ENABLE FAILPOINT upsert_log_throw_after_sync")
        error = node.query_and_get_error(insert_query(table), settings=settings)
        assert "FAULT_INJECTED" in error
        node.restart_clickhouse(kill=True)
        assert node.query(f"SELECT count() FROM {table}") == "1024\n"
        node.query(insert_query(table), settings=settings)
        assert node.query(
            f"SELECT count(),uniqExact(id),countIf(value!=id+64512) FROM {table}"
        ) == "1024\t1024\t0\n"
        node.query(f"INSERT INTO {table} VALUES (0,1,999999,'newer')", settings=settings)
        node.restart_clickhouse(kill=True)
        node.query(insert_query(table), settings={**settings, "max_insert_threads": 1})
        assert node.query(f"SELECT value,payload FROM {table} WHERE id=0") == "999999\tnewer\n"
    finally:
        node.query("SYSTEM DISABLE FAILPOINT upsert_log_throw_after_sync")
        node.query(f"DROP TABLE {table} SYNC")
