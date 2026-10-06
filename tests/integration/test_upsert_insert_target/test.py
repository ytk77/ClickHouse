from concurrent.futures import ThreadPoolExecutor
import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", stay_alive=True)
MIB = 1 << 20
# `numbers` supplies one stream. These limits squash each fixture into one upstream
# block, making the exact part counts measure the engine's own byte slicing.
settings = {
    "max_threads": 4,
    "max_insert_threads": 4,
    "max_threads_min_free_memory_per_thread": 0,
    "max_insert_threads_min_free_memory_per_thread": 0,
    "max_block_size": 2000000,
    "max_insert_block_size": 2000000,
    "min_insert_block_size_rows": 2000000,
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


def create(name, target=None):
    clause = "" if target is None else f" SETTINGS upsert_insert_target_bytes={target}"
    node.query(
        f"CREATE TABLE {name} (id UInt64, v UInt64, pad String) "
        f"ENGINE=UpsertMergeTree(v) ORDER BY id UNIQUE KEY id{clause}"
    )
    node.query(f"SYSTEM STOP MERGES {name}")


def parts(name):
    return (
        node.query(
            "SELECT name,rows,data_uncompressed_bytes FROM system.parts "
            f"WHERE active AND database=currentDatabase() AND table='{name}' ORDER BY name"
        )
        .strip()
        .splitlines()
    )


def insert_sql(name, rows, token="bulk"):
    return (
        f"INSERT INTO {name} SETTINGS insert_deduplication_token='{token}' "
        f"SELECT number,1,repeat(toString(number%10),4096) FROM numbers({rows})"
    )


@pytest.mark.parametrize("threads", [1, 4])
def test_wide_insert_produces_fewer_parts_and_preserves_updates(threads):
    rows = 49152
    tables = [f"insert_target_legacy_{threads}", f"insert_target_default_{threads}"]
    query_settings = {**settings, "max_insert_threads": threads}
    try:
        create(tables[0], 32 * MIB)
        create(tables[1])
        shapes = []
        for name in tables:
            node.query(insert_sql(name, rows), settings=query_settings)
            shape = parts(name)
            shapes.append(shape)
            assert (
                node.query(
                    f"SELECT count(),uniqExact(id),sum(id),sum(v),sum(length(pad)) FROM {name}"
                ).strip()
                == f"{rows}\t{rows}\t{rows*(rows-1)//2}\t{rows}\t{rows*4096}"
            )
        assert len(shapes[0]) >= 6, shapes[0]
        assert len(shapes[1]) == 1, shapes[1]
        assert int(shapes[1][0].split("\t")[2]) > 128 * MIB
        print(
            f"threads={threads}, legacy_parts={len(shapes[0])}, default_parts={shapes[1]}"
        )

        name = tables[1]
        node.query(f"INSERT INTO {name} VALUES (0,2,'new'),(1,0,'stale')")
        before = parts(name)
        node.query(insert_sql(name, rows), settings=query_settings)
        assert parts(name) == before
        expected = f"{rows}\t{rows+1}\t1\t1"
        check = (
            f"SELECT count(),sum(v),countIf(id=0 AND pad='new'),"
            f"countIf(id=1 AND v=1 AND length(pad)=4096) FROM {name}"
        )
        assert node.query(check).strip() == expected
        node.restart_clickhouse(kill=True)
        assert node.query(check).strip() == expected
    finally:
        for name in tables:
            node.query(f"DROP TABLE IF EXISTS {name} SYNC")


def test_target_range_and_alter_validation():
    name = "insert_target_validation"
    ordinary = "insert_target_ordinary"
    try:
        create(name)
        for target in (32 * MIB, 1 << 30):
            node.query(
                f"ALTER TABLE {name} MODIFY SETTING upsert_insert_target_bytes={target}"
            )
            assert f"upsert_insert_target_bytes = {target}" in node.query(
                f"SHOW CREATE TABLE {name}"
            )
        for target in (0, 32 * MIB - 1, (1 << 30) + 1):
            error = node.query_and_get_error(
                f"ALTER TABLE {name} MODIFY SETTING upsert_insert_target_bytes={target}"
            )
            assert "BAD_ARGUMENTS" in error and "upsert_insert_target_bytes" in error
            assert "upsert_insert_target_bytes = 1073741824" in node.query(
                f"SHOW CREATE TABLE {name}"
            )
            assert "BAD_ARGUMENTS" in node.query_and_get_error(
                "CREATE TABLE insert_target_invalid (id UInt64) ENGINE=UpsertMergeTree "
                f"ORDER BY id UNIQUE KEY id SETTINGS upsert_insert_target_bytes={target}"
            )
        node.query(f"CREATE TABLE {ordinary} (id UInt64) ENGINE=MergeTree ORDER BY id")
        assert "applies only to UpsertMergeTree" in node.query_and_get_error(
            f"ALTER TABLE {ordinary} MODIFY SETTING upsert_insert_target_bytes={64*MIB}"
        )
        node.query(f"INSERT INTO {name} VALUES (1,1,'still usable')")
        assert node.query(f"SELECT count() FROM {name}") == "1\n"
    finally:
        for table in (name, ordinary, "insert_target_invalid"):
            node.query(f"DROP TABLE IF EXISTS {table} SYNC")


@pytest.mark.parametrize("target", [None, 1024 * MIB])
def test_default_512_mib_splits_bulk_input_with_bounded_memory(target):
    name = "insert_target_bulk_default" if target is None else "insert_target_bulk_1g"
    rows = 163840
    query_id = name + "_insert"
    try:
        create(name, target)
        started = time.monotonic()
        node.query(
            insert_sql(name, rows),
            settings={**settings, "max_memory_usage": 4 << 30, "log_queries": 1},
            query_id=query_id,
            timeout=180,
        )
        elapsed = time.monotonic() - started
        shape = parts(name)
        assert len(shape) == (2 if target is None else 1), shape
        largest = max(int(part.split("\t")[2]) for part in shape)
        assert largest > 500 * MIB, shape
        assert (
            node.query(
                f"SELECT count(),uniqExact(id),sum(id),sum(length(pad)) FROM {name}"
            ).strip()
            == f"{rows}\t{rows}\t{rows*(rows-1)//2}\t{rows*4096}"
        )
        before = parts(name)
        node.query(insert_sql(name, rows), settings=settings, timeout=180)
        assert parts(name) == before
        node.query("SYSTEM FLUSH LOGS")
        memory = node.query(
            f"SELECT memory_usage FROM system.query_log WHERE query_id='{query_id}' "
            "AND type='QueryFinish' ORDER BY event_time_microseconds DESC LIMIT 1"
        ).strip()
        input_bytes = rows * (4096 + 16 + 8)
        expected_envelope = input_bytes + 3 * (target or 512 * MIB)
        assert memory and int(memory) < expected_envelope, (memory, expected_envelope)
        print(
            f"target={target or 512*MIB}, parts={shape}, peak_memory={memory}, seconds={elapsed:.3f}"
        )
    finally:
        node.query(f"DROP TABLE IF EXISTS {name} SYNC")


def test_multiple_oversized_preparations_preserve_commit_order():
    name = "insert_target_oversized_queue"
    rows = 131072
    try:
        create(name, 256 * MIB)
        node.query(
            f"INSERT INTO {name} SELECT number%65536,1,"
            f"repeat(toString(intDiv(number,65536)),4096) FROM numbers({rows})",
            settings={**settings, "max_memory_usage": 3 << 30},
            timeout=180,
        )
        shape = parts(name)
        assert sum(int(part.split("\t")[2]) > 128 * MIB for part in shape) >= 2, shape
        assert (
            node.query(
                f"SELECT count(),uniqExact(id),countIf(pad!=repeat('1',4096)) FROM {name}"
            ).strip()
            == "65536\t65536\t0"
        )
        print(f"oversized queue shape={shape}")
    finally:
        node.query(f"DROP TABLE IF EXISTS {name} SYNC")


@pytest.mark.parametrize("dense_keys", [False, True])
def test_larger_target_retains_row_and_lineage_bounds(dense_keys):
    name = "insert_target_independent_bounds"
    rows = 4096 if dense_keys else 1200000
    try:
        if dense_keys:
            node.query(
                f"CREATE TABLE {name} (id String, v UInt64) ENGINE=UpsertMergeTree "
                "ORDER BY id UNIQUE KEY id SETTINGS upsert_insert_target_bytes=268435456"
            )
        else:
            create(name, 256 * MIB)
        node.query(f"SYSTEM STOP MERGES {name}")
        expression = (
            "concat(toString(number),repeat('k',20000)),1"
            if dense_keys
            else "number,1,''"
        )
        node.query(
            f"INSERT INTO {name} SELECT {expression} FROM numbers({rows})",
            settings={**settings, "unique_key_max_encoded_size": 32768},
            timeout=180,
        )
        shape = parts(name)
        assert len(shape) > 1, shape
        assert sum(int(part.split("\t")[1]) for part in shape) == rows
        assert max(int(part.split("\t")[1]) for part in shape) <= 1048576
        assert (
            node.query(f"SELECT count(),uniqExact(id) FROM {name}").strip()
            == f"{rows}\t{rows}"
        )
        print(f"dense_keys={dense_keys}, shape={shape}")
    finally:
        node.query(f"DROP TABLE IF EXISTS {name} SYNC")


def test_large_unit_partial_commit_recovers_and_retries():
    name = "insert_target_recovery"
    rows = 24576
    query = insert_sql(name, rows)
    try:
        create(name, 64 * MIB)
        node.query("SYSTEM ENABLE FAILPOINT upsert_log_throw_after_sync")
        assert "FAULT_INJECTED" in node.query_and_get_error(query, settings=settings)
        node.restart_clickhouse(kill=True)
        node.query(f"SYSTEM STOP MERGES {name}")
        committed = int(node.query(f"SELECT count() FROM {name}"))
        assert 0 < committed < rows
        assert max(int(part.split("\t")[2]) for part in parts(name)) > 32 * MIB
        node.query(query, settings=settings)
        expected = f"{rows}\t{rows}\t{rows*(rows-1)//2}"
        check = f"SELECT count(),uniqExact(id),sum(id) FROM {name}"
        assert node.query(check).strip() == expected
        before = parts(name)
        node.query(query, settings={**settings, "max_insert_threads": 1})
        assert parts(name) == before
        node.restart_clickhouse(kill=True)
        assert node.query(check).strip() == expected
    finally:
        node.query("SYSTEM DISABLE FAILPOINT upsert_log_throw_after_sync")
        node.query(f"DROP TABLE IF EXISTS {name} SYNC")


def test_active_insert_keeps_target_and_changed_retry_fails_closed():
    name = "insert_target_snapshot"
    rows = 24576
    query = insert_sql(name, rows)
    point = "upsert_pause_first_preparation"
    try:
        create(name, 64 * MIB)
        node.query(f"SYSTEM ENABLE FAILPOINT {point}")
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(node.query, query, settings=settings)
            try:
                node.query(f"SYSTEM WAIT FAILPOINT {point} PAUSE", timeout=30)
                node.query(
                    f"ALTER TABLE {name} MODIFY SETTING upsert_insert_target_bytes={32*MIB}"
                )
            finally:
                node.query(f"SYSTEM NOTIFY FAILPOINT {point}")
            future.result(timeout=120)
        node.query(f"SYSTEM DISABLE FAILPOINT {point}")
        shape = parts(name)
        assert len(shape) == 2, shape
        assert int(shape[0].split("\t")[2]) > 32 * MIB
        error = node.query_and_get_error(query, settings=settings)
        assert (
            "deduplication token was reused with a different evaluated block" in error
        ), error
        assert parts(name) == shape
        node.query(
            f"ALTER TABLE {name} MODIFY SETTING upsert_insert_target_bytes={64*MIB}"
        )
        node.query(query, settings=settings)
        assert parts(name) == shape
    finally:
        node.query(f"SYSTEM DISABLE FAILPOINT {point}")
        node.query(f"DROP TABLE IF EXISTS {name} SYNC")


def test_large_units_forward_materialized_views_once():
    name = "insert_target_mv_source"
    destination = "insert_target_mv_count"
    view = "insert_target_mv"
    rows = 24576
    try:
        create(name, 64 * MIB)
        node.query(
            f"CREATE TABLE {destination} (n UInt64) ENGINE=SummingMergeTree ORDER BY tuple()"
        )
        node.query(
            f"CREATE MATERIALIZED VIEW {view} TO {destination} AS SELECT count() AS n FROM {name}"
        )
        for _ in range(2):
            node.query(insert_sql(name, rows), settings=settings)
            assert node.query(f"SELECT sum(n) FROM {destination}").strip() == str(rows)
        assert len(parts(name)) == 2
        assert node.query(f"SELECT count() FROM {name}").strip() == str(rows)
    finally:
        for table in (view, name, destination):
            node.query(f"DROP TABLE IF EXISTS {table} SYNC")
