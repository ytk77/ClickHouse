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


def create_table(table, ttl=""):
    node.query(
        f"CREATE TABLE {table} (id UInt64, ver UInt64, expires DateTime, category UInt8) "
        f"ENGINE=UpsertMergeTree(ver) ORDER BY category UNIQUE KEY id {ttl}", settings=settings,
    )


def wait_result(sql, expected):
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        actual = node.query(sql)
        if actual == expected:
            return
        time.sleep(0.1)
    assert actual == expected, (sql, actual, expected)


@pytest.mark.parametrize("command", ["REMOVE TTL", "MODIFY TTL expires + INTERVAL 200 YEAR"])
def test_alter_cancels_prepared_expiration(command):
    table = "alter_ttl_prepared"
    create_table(table, "TTL expires")
    try:
        node.query(f"SYSTEM STOP TTL MERGES {table}")
        node.query(f"INSERT INTO {table} VALUES (1,10,'2000-01-01',1)", settings=settings)
        node.query("SYSTEM ENABLE FAILPOINT upsert_pause_before_merge_commit")
        node.query(f"SYSTEM START TTL MERGES {table}")
        node.query("SYSTEM WAIT FAILPOINT upsert_pause_before_merge_commit PAUSE")
        node.query(f"ALTER TABLE {table} {command}")
        node.query("SYSTEM DISABLE FAILPOINT upsert_pause_before_merge_commit")
        # Detachment joins the old maintenance task, so its stale result must have been discarded.
        node.query(f"DETACH TABLE {table} SYNC")
        node.query(f"ATTACH TABLE {table}")
        assert node.query(f"SELECT id,ver FROM {table}") == "1\t10\n"
        node.query(f"ALTER TABLE {table} MODIFY TTL expires")
        wait_result(f"SELECT count() FROM {table}", "0\n")
    finally:
        node.query("SYSTEM DISABLE FAILPOINT upsert_pause_before_merge_commit")
        node.query(f"DROP TABLE {table} SYNC")


def test_inflight_insert_with_old_metadata_is_reevaluated():
    table = "alter_ttl_insert"
    create_table(table)
    try:
        node.query("SYSTEM ENABLE FAILPOINT upsert_pause_first_preparation")
        with ThreadPoolExecutor(max_workers=1) as pool:
            writer = pool.submit(
                node.query, f"INSERT INTO {table} VALUES (1,10,'2000-01-01',1),(2,10,'2100-01-01',2)",
                settings={**settings, "max_insert_threads": 2, "max_threads": 2},
            )
            try:
                node.query("SYSTEM WAIT FAILPOINT upsert_pause_first_preparation PAUSE")
                node.query(f"ALTER TABLE {table} MODIFY TTL expires")
            finally:
                node.query("SYSTEM DISABLE FAILPOINT upsert_pause_first_preparation")
            writer.result(timeout=30)
        wait_result(f"SELECT id FROM {table}", "2\n")
        node.restart_clickhouse(kill=True)
        assert node.query(f"SELECT id FROM {table}") == "2\n"
    finally:
        node.query("SYSTEM DISABLE FAILPOINT upsert_pause_first_preparation")
        node.query(f"DROP TABLE {table} SYNC")


@pytest.mark.parametrize("point,committed", [
    ("upsert_ttl_pause_after_manifest_upgrade", False),
    ("upsert_ttl_pause_after_metadata_commit", True),
])
@pytest.mark.parametrize("remove", [False, True])
def test_restart_at_ttl_metadata_commit(point, committed, remove):
    table = "alter_ttl_recovery"
    create_table(table, "TTL expires + INTERVAL 200 YEAR" if remove else "")
    node.query(f"INSERT INTO {table} VALUES (1,10,'2000-01-01',1)", settings=settings)
    try:
        node.query(f"SYSTEM ENABLE FAILPOINT {point}")
        with ThreadPoolExecutor(max_workers=1) as pool:
            writer = pool.submit(node.query, f"ALTER TABLE {table} " + ("REMOVE TTL" if remove else "MODIFY TTL expires"))
            try:
                node.query(f"SYSTEM WAIT FAILPOINT {point} PAUSE")
                node.restart_clickhouse(kill=True)
            finally:
                node.query(f"SYSTEM DISABLE FAILPOINT {point}")
            with pytest.raises(Exception):
                writer.result(timeout=30)
        has_ttl = "TTL " in node.query(f"SHOW CREATE TABLE {table}")
        assert has_ttl == ((not committed) if remove else committed)
        if committed and not remove:
            wait_result(f"SELECT count() FROM {table}", "0\n")
        else:
            assert node.query(f"SELECT count() FROM {table}") == "1\n"
        node.query(f"ALTER TABLE {table} MODIFY TTL expires + INTERVAL 200 YEAR")
        node.query(f"ALTER TABLE {table} REMOVE TTL")
    finally:
        node.query(f"SYSTEM DISABLE FAILPOINT {point}")
        node.query(f"DROP TABLE {table} SYNC")


def test_failure_after_metadata_commit_fences_until_restart():
    table = "alter_ttl_fenced"
    create_table(table)
    try:
        node.query(f"INSERT INTO {table} VALUES (1,10,'2000-01-01',1)", settings=settings)
        node.query("SYSTEM ENABLE FAILPOINT upsert_ttl_throw_after_metadata_commit")
        assert "FAULT_INJECTED" in node.query_and_get_error(f"ALTER TABLE {table} MODIFY TTL expires")
        assert "fenced" in node.query_and_get_error(f"SELECT * FROM {table}")
        node.restart_clickhouse(kill=True)
        wait_result(f"SELECT count() FROM {table}", "0\n")
        node.query(f"ALTER TABLE {table} REMOVE TTL")
        node.query(f"INSERT INTO {table} VALUES (1,1,'2000-01-01',1)", settings=settings)
        assert node.query(f"SELECT ver FROM {table}") == "1\n"
    finally:
        node.query("SYSTEM DISABLE FAILPOINT upsert_ttl_throw_after_metadata_commit")
        node.query(f"DROP TABLE {table} SYNC")


def test_invalid_alter_preserves_policy_and_table_availability():
    table = "alter_ttl_invalid"
    create_table(table, "TTL expires + INTERVAL 200 YEAR")
    try:
        original = node.query(f"SHOW CREATE TABLE {table}")
        for clause in ("MODIFY TTL expires RECOMPRESS CODEC(LZ4)", "MODIFY TTL missing_column", "MODIFY TTL expires, ADD COLUMN extra UInt8"):
            assert node.query_and_get_error(f"ALTER TABLE {table} {clause}")
            assert node.query(f"SHOW CREATE TABLE {table}") == original
        node.query(f"INSERT INTO {table} VALUES (1,10,'2000-01-01',1)", settings=settings)
        assert node.query(f"SELECT count() FROM {table}") == "1\n"
    finally:
        node.query(f"DROP TABLE {table} SYNC")


def test_ordinary_database_alter_survives_restart():
    database = "alter_ttl_ordinary"
    table = f"{database}.state"
    node.query(f"CREATE DATABASE {database} ENGINE=Ordinary", settings={"allow_deprecated_database_ordinary": 1})
    try:
        create_table(table)
        node.query(f"INSERT INTO {table} VALUES (1,10,'2000-01-01',1),(2,10,'2100-01-01',2)", settings=settings)
        node.query(f"ALTER TABLE {table} MODIFY TTL expires")
        wait_result(f"SELECT id FROM {table}", "2\n")
        node.restart_clickhouse(kill=True)
        assert node.query(f"SELECT id FROM {table}") == "2\n"
        node.query(f"ALTER TABLE {table} REMOVE TTL")
        node.query(f"INSERT INTO {table} VALUES (1,1,'2000-01-01',1)", settings=settings)
        node.restart_clickhouse(kill=True)
        assert node.query(f"SELECT id FROM {table} ORDER BY id") == "1\n2\n"
    finally:
        node.query(f"DROP DATABASE {database} SYNC")
