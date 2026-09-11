import random
import time
from concurrent.futures import ThreadPoolExecutor

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


def wait_parts(table, count, active=True):
    deadline = time.monotonic() + 60
    predicate = " AND active" if active else ""
    sql = f"SELECT count() FROM system.parts WHERE database=currentDatabase() AND table='{table}'{predicate}"
    while time.monotonic() < deadline:
        actual = node.query(sql).strip()
        if actual == str(count):
            return
        time.sleep(0.1)
    assert actual == str(count)


def test_merge_materializes_constant_partition_columns_and_preserves_full_rows():
    table = "v8_constant_partition_columns"
    node.query(
        f"CREATE TABLE {table} (p UInt8,id UInt64,ver UInt64,value String) "
        "ENGINE=UpsertMergeTree(ver) PARTITION BY p ORDER BY value UNIQUE KEY id",
        settings=settings,
    )
    rng = random.Random(7201)
    model = {}
    for batch in range(40):
        rows = []
        for ordinal in range(30):
            p, key, version = rng.randrange(3), rng.randrange(25), rng.randrange(20)
            value = f"b{batch}r{ordinal}"
            rows.append(f"({p},{key},{version},'{value}')")
            old = model.get((p, key))
            if old is None or version >= old[0]:
                model[p, key] = (version, value)
        node.query(f"INSERT INTO {table} VALUES " + ",".join(rows), settings=settings)
    expected = "".join(
        f"{p}\t{key}\t{version}\t{value}\n"
        for (p, key), (version, value) in sorted(model.items())
    )
    wait_parts(table, 3)
    assert node.query(f"SELECT * FROM {table} ORDER BY p,id") == expected
    node.query(f"DETACH TABLE {table} SYNC")
    node.query(f"ATTACH TABLE {table}")
    assert node.query(f"SELECT * FROM {table} ORDER BY p,id") == expected
    node.query(f"DROP TABLE {table} SYNC")


def test_late_kill_and_old_reader_during_automatic_merge():
    table = "v8_late_kill"
    node.query(
        f"CREATE TABLE {table} (id UInt64,ver UInt64,value UInt64) "
        "ENGINE=UpsertMergeTree(ver) ORDER BY value UNIQUE KEY id",
        settings=settings,
    )
    try:
        node.query(f"INSERT INTO {table} VALUES (1,10,10),(2,10,20)", settings=settings)
        node.query("SYSTEM ENABLE FAILPOINT upsert_pause_before_merge_commit")
        node.query(f"INSERT INTO {table} VALUES (3,10,30)", settings=settings)
        node.query("SYSTEM WAIT FAILPOINT upsert_pause_before_merge_commit PAUSE")
        node.query(f"INSERT INTO {table} VALUES (1,11,100)", settings=settings)
        node.query("SYSTEM NOTIFY FAILPOINT upsert_pause_before_merge_commit")
        wait_parts(table, 1)
        expected = "1\t11\t100\n2\t10\t20\n3\t10\t30\n"
        assert node.query(f"SELECT * FROM {table} ORDER BY id") == expected
        node.query("SYSTEM ENABLE FAILPOINT upsert_pause_after_read_snapshot")
        with ThreadPoolExecutor(max_workers=1) as pool:
            reader = pool.submit(node.query, f"SELECT * FROM {table} ORDER BY id")
            node.query("SYSTEM WAIT FAILPOINT upsert_pause_after_read_snapshot PAUSE")
            node.query(f"INSERT INTO {table} VALUES (1,12,200)", settings=settings)
            wait_parts(table, 1)
            assert node.query(
                f"SELECT count()>0 FROM system.parts WHERE database=currentDatabase() AND table='{table}' AND NOT active"
            ) == "1\n"
            node.query("SYSTEM NOTIFY FAILPOINT upsert_pause_after_read_snapshot")
            assert reader.result(timeout=30) == expected
        wait_parts(table, 1, active=False)
        node.query(f"DETACH TABLE {table} SYNC")
        node.query(f"ATTACH TABLE {table}")
        assert node.query(f"SELECT * FROM {table} ORDER BY id") == "1\t12\t200\n2\t10\t20\n3\t10\t30\n"
    finally:
        node.query("SYSTEM NOTIFY FAILPOINT upsert_pause_before_merge_commit")
        node.query("SYSTEM DISABLE FAILPOINT upsert_pause_before_merge_commit")
        node.query("SYSTEM NOTIFY FAILPOINT upsert_pause_after_read_snapshot")
        node.query("SYSTEM DISABLE FAILPOINT upsert_pause_after_read_snapshot")
    node.query(f"DROP TABLE {table} SYNC")
