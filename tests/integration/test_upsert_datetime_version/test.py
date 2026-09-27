import random
import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", stay_alive=True)
settings = {"allow_experimental_unique_key": 1, "async_insert": 0}


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def wait_for_one_part(table):
    deadline = time.monotonic() + 60
    while True:
        actual = node.query(
            "SELECT count() FROM system.parts "
            f"WHERE database=currentDatabase() AND table='{table}' AND active"
        ).strip()
        if actual == "1":
            return
        assert time.monotonic() < deadline, actual
        time.sleep(0.1)


@pytest.mark.parametrize("scale", range(10))
def test_precision_and_maintenance(scale):
    table = f"upsert_time_precision_{scale}"
    node.query(
        f"CREATE TABLE {table} (id UInt64, updatetime DateTime64({scale}, 'Asia/Taipei'), value String) "
        "ENGINE=UpsertMergeTree(updatetime) ORDER BY value UNIQUE KEY id",
        settings=settings,
    )
    nanos_per_tick = 10 ** (9 - scale)
    model = {}
    rng = random.Random(2979 + scale)
    try:
        for batch in range(8):
            # Distinct new keys guarantee mergeable parts even when all updates in a batch lose.
            rows = [(100 + batch, -1, f"new_{batch}")]
            rows += [(rng.randrange(8), rng.randrange(-4, 5), f"b{batch}r{row}") for row in range(24)]
            values = ",".join(f"({key},{tick},'{value}')" for key, tick, value in rows)
            node.query(
                f"INSERT INTO {table} SELECT id, fromUnixTimestamp64Nano(ticks*{nanos_per_tick}), value "
                f"FROM values('id UInt64,ticks Int64,value String',{values})",
                settings=settings,
            )
            for key, tick, value in rows:
                if key not in model or tick >= model[key][0]:
                    model[key] = tick, value
        wait_for_one_part(table)
        expected = "".join(
            f"{key}\t{tick*nanos_per_tick}\t{value}\n"
            for key, (tick, value) in sorted(model.items())
        )
        query = f"SELECT id,toUnixTimestamp64Nano(updatetime),value FROM {table} ORDER BY id"
        assert node.query(query) == expected
        node.query(f"DETACH TABLE {table} SYNC")
        node.query(f"ATTACH TABLE {table}")
        assert node.query(query) == expected
        # A nanosecond-scale source constant is converted to the declared scale before comparison.
        node.query(
            f"INSERT INTO {table} SELECT toUInt64(100),fromUnixTimestamp64Nano(toInt64(0)),'epoch'",
            settings=settings,
        )
        node.query(
            f"INSERT INTO {table} SELECT toUInt64(100),fromUnixTimestamp64Nano(toInt64(-{nanos_per_tick})),'stale'",
            settings=settings,
        )
        assert node.query(f"SELECT toUnixTimestamp64Nano(updatetime),value FROM {table} WHERE id=100") == "0\tepoch\n"
    finally:
        node.query(f"DROP TABLE IF EXISTS {table} SYNC")


@pytest.mark.parametrize("kind", ["DateTime", "DateTime64"])
def test_durable_time_version_recovers_and_replays(kind):
    table = "upsert_time_recovery"
    point = "upsert_log_throw_after_sync"
    if kind == "DateTime":
        version_type = "DateTime('Asia/Taipei')"
        expression = "toDateTime(toUInt32({}), 'UTC')"
        to_ticks = "toUnixTimestamp"
        initial, other, updated, newest = 10, 20, 11, 12
    else:
        version_type = "DateTime64(9, 'Asia/Taipei')"
        expression = "fromUnixTimestamp64Nano(toInt64({}))"
        to_ticks = "toUnixTimestamp64Nano"
        initial, other, updated, newest = -1, -2, 0, 1
    node.query(
        f"CREATE TABLE {table} (id UInt64, updatetime {version_type}, value String) "
        "ENGINE=UpsertMergeTree(updatetime) ORDER BY value UNIQUE KEY id",
        settings=settings,
    )
    try:
        node.query(
            f"INSERT INTO {table} VALUES (1,{expression.format(initial)},'old'),(2,{expression.format(other)},'other')",
            settings=settings,
        )
        retry = (
            f"INSERT INTO {table} SETTINGS insert_deduplication_token='time-retry' "
            f"VALUES (1,{expression.format(updated)},'updated'),(3,{expression.format(newest)},'new')"
        )
        node.query(f"SYSTEM ENABLE FAILPOINT {point}")
        assert "FAULT_INJECTED" in node.query_and_get_error(retry, settings=settings)
        assert "fenced" in node.query_and_get_error(f"SELECT * FROM {table}")
        node.restart_clickhouse(kill=True)
        query = f"SELECT id,{to_ticks}(updatetime),value FROM {table} ORDER BY id"
        assert node.query(query) == f"1\t{updated}\tupdated\n2\t{other}\tother\n3\t{newest}\tnew\n"
        node.query(f"INSERT INTO {table} VALUES (1,{expression.format(newest)},'newest')", settings=settings)
        node.query(retry, settings=settings)
        node.query(f"INSERT INTO {table} VALUES (1,{expression.format(initial)},'stale')", settings=settings)
        expected = f"1\t{newest}\tnewest\n2\t{other}\tother\n3\t{newest}\tnew\n"
        assert node.query(query) == expected
        node.restart_clickhouse(kill=True)
        assert node.query(query) == expected
    finally:
        node.query(f"SYSTEM DISABLE FAILPOINT {point}")
        node.query(f"DROP TABLE IF EXISTS {table} SYNC")


def test_datetime64_parallel_split_and_retry():
    table = "upsert_datetime64_split"
    parallel_settings = {
        **settings,
        "max_threads": 4,
        "max_insert_threads": 4,
        "max_threads_min_free_memory_per_thread": 0,
        "max_insert_threads_min_free_memory_per_thread": 0,
        "max_block_size": 131072,
        "min_insert_block_size_rows": 131072,
        "min_insert_block_size_bytes": 0,
        "use_strict_insert_block_limits": 0,
    }
    node.query(
        f"CREATE TABLE {table} (id UInt64, updatetime DateTime64(9), value UInt64, payload String) "
        "ENGINE=UpsertMergeTree(updatetime) ORDER BY value UNIQUE KEY id",
        settings=settings,
    )
    insert = (
        f"INSERT INTO {table} SETTINGS insert_deduplication_token='time-split' "
        "SELECT number%1024,fromUnixTimestamp64Nano(if(number<32768,toInt64(1),toInt64(-1))),"
        "number,repeat(toString(number%10),1024) FROM numbers(65536)"
    )
    try:
        node.query(insert, settings=parallel_settings, timeout=120)
        assert node.query(
            f"SELECT count(),uniqExact(id),countIf(value!=id+31744),countIf(toUnixTimestamp64Nano(updatetime)!=1) FROM {table}"
        ) == "1024\t1024\t0\t0\n"
        node.query(
            f"INSERT INTO {table} VALUES (0,fromUnixTimestamp64Nano(toInt64(2)),999999,'newest')",
            settings=settings,
        )
        node.restart_clickhouse(kill=True)
        node.query(insert, settings={**parallel_settings, "max_insert_threads": 1}, timeout=120)
        assert node.query(f"SELECT toUnixTimestamp64Nano(updatetime),value,payload FROM {table} WHERE id=0") == "2\t999999\tnewest\n"
        assert node.query(f"SELECT count(),uniqExact(id) FROM {table}") == "1024\t1024\n"
    finally:
        node.query(f"DROP TABLE IF EXISTS {table} SYNC")
