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


def wait_result(sql, expected):
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        actual = node.query(sql)
        if actual == expected:
            return
        time.sleep(0.1)
    assert actual == expected


def create_table(table):
    node.query(
        f"CREATE TABLE {table} (id UInt64, ver UInt64, expires DateTime, value String) "
        "ENGINE=UpsertMergeTree(ver) ORDER BY value UNIQUE KEY id TTL expires",
        settings=settings,
    )


def test_expiration_preserves_concurrent_upsert_and_pinned_reader():
    table = "ttl_concurrent"
    create_table(table)
    node.query(f"SYSTEM STOP TTL MERGES {table}")
    node.query(
        f"INSERT INTO {table} VALUES (1,10,'2000-01-01','old'), (2,10,'2100-01-01','retained')",
        settings=settings,
    )
    try:
        node.query("SYSTEM ENABLE FAILPOINT upsert_pause_after_read_snapshot")
        with ThreadPoolExecutor(max_workers=1) as pool:
            reader = pool.submit(node.query, f"SELECT id,ver,value FROM {table} ORDER BY id")
            try:
                node.query("SYSTEM WAIT FAILPOINT upsert_pause_after_read_snapshot PAUSE")
                node.query("SYSTEM ENABLE FAILPOINT upsert_pause_before_merge_commit")
                node.query(f"SYSTEM START TTL MERGES {table}")
                node.query("SYSTEM WAIT FAILPOINT upsert_pause_before_merge_commit PAUSE")
                node.query(f"INSERT INTO {table} VALUES (1,11,'2100-01-01','new')", settings=settings)
                node.query("SYSTEM DISABLE FAILPOINT upsert_pause_before_merge_commit")
                wait_result(f"SELECT id,ver,value FROM {table} ORDER BY id", "1\t11\tnew\n2\t10\tretained\n")
                wait_result(
                    f"SELECT sum(rows) FROM system.parts WHERE database=currentDatabase() AND table='{table}' AND active",
                    "2\n",
                )
            finally:
                node.query("SYSTEM DISABLE FAILPOINT upsert_pause_after_read_snapshot")
            assert reader.result(timeout=30) == "1\t10\told\n2\t10\tretained\n"
    finally:
        node.query("SYSTEM DISABLE FAILPOINT upsert_pause_before_merge_commit")
        node.query("SYSTEM DISABLE FAILPOINT upsert_pause_after_read_snapshot")
        node.query(f"DROP TABLE {table} SYNC")


@pytest.mark.parametrize("point", ["upsert_log_throw_after_sync", "upsert_throw_after_rename"])
def test_expiration_recovers_after_durable_commit_failure(point):
    table = "ttl_recovery"
    create_table(table)
    node.query(f"SYSTEM STOP TTL MERGES {table}")
    node.query(
        f"INSERT INTO {table} VALUES (1,10,'2000-01-01','expired'), (2,10,'2100-01-01','retained')",
        settings=settings,
    )
    try:
        node.query("SYSTEM ENABLE FAILPOINT upsert_pause_before_merge_commit")
        node.query(f"SYSTEM START TTL MERGES {table}")
        node.query("SYSTEM WAIT FAILPOINT upsert_pause_before_merge_commit PAUSE")
        node.query(f"SYSTEM ENABLE FAILPOINT {point}")
        node.query("SYSTEM DISABLE FAILPOINT upsert_pause_before_merge_commit")
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            try:
                node.query(f"SELECT count() FROM {table}")
            except Exception as error:
                assert "fenced" in str(error)
                break
            time.sleep(0.1)
        else:
            pytest.fail("TTL maintenance did not reach the durable commit failure")
        node.restart_clickhouse(kill=True)
        assert node.query(f"SELECT id,ver,value FROM {table}") == "2\t10\tretained\n"
        node.query(f"INSERT INTO {table} VALUES (1,1,'2100-01-01','reinserted')", settings=settings)
        assert node.query(f"SELECT id,ver,value FROM {table} ORDER BY id") == "1\t1\treinserted\n2\t10\tretained\n"
    finally:
        node.query("SYSTEM DISABLE FAILPOINT upsert_pause_before_merge_commit")
        node.query(f"SYSTEM DISABLE FAILPOINT {point}")
        node.query(f"DROP TABLE {table} SYNC")


def test_single_part_expires_after_deadline_without_new_inserts():
    table = "ttl_deadline"
    create_table(table)
    try:
        node.query(f"INSERT INTO {table} SELECT 1,10,now()+INTERVAL 10 SECOND,'expires later'", settings=settings)
        assert node.query(f"SELECT count() FROM {table}") == "1\n"
        wait_result(f"SELECT count() FROM {table}", "0\n")
        node.restart_clickhouse(kill=True)
        assert node.query(f"SELECT count() FROM {table}") == "0\n"
    finally:
        node.query(f"DROP TABLE {table} SYNC")


def test_expiration_rewrites_a_healthy_target_sized_part():
    table = "ttl_large_part"
    create_table(table)
    try:
        node.query(f"SYSTEM STOP TTL MERGES {table}")
        node.query(
            f"INSERT INTO {table} SELECT number,10,toDateTime('2000-01-01'),repeat('x',900) "
            "FROM numbers(20000) SETTINGS max_threads=1,max_insert_threads=1",
            settings=settings,
        )
        assert node.query(f"SELECT count() FROM {table}") == "20000\n"
        assert node.query(
            f"SELECT count(),sum(data_uncompressed_bytes)>16777216 FROM system.parts "
            f"WHERE database=currentDatabase() AND table='{table}' AND active"
        ) == "1\t1\n"
        node.query(f"SYSTEM START TTL MERGES {table}")
        wait_result(f"SELECT count() FROM {table}", "0\n")
    finally:
        node.query(f"DROP TABLE {table} SYNC")
