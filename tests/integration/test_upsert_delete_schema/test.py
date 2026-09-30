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


def create_table(table, versioned=True):
    engine = "UpsertMergeTree(ver)" if versioned else "UpsertMergeTree"
    node.query(
        f"CREATE TABLE {table} (id UInt64, ver UInt64, value String) "
        f"ENGINE={engine} ORDER BY id UNIQUE KEY id", settings=settings,
    )
    node.query(f"INSERT INTO {table} VALUES (1,10,'one'),(2,10,'two')", settings=settings)


@pytest.mark.parametrize("versioned", [False, True])
def test_delete_preserves_concurrent_insert(versioned):
    table = "delete_concurrent"
    create_table(table, versioned)
    try:
        node.query("SYSTEM ENABLE FAILPOINT upsert_pause_before_merge_commit")
        with ThreadPoolExecutor(max_workers=1) as pool:
            deletion = pool.submit(node.query, f"DELETE FROM {table} WHERE id=1")
            try:
                node.query("SYSTEM WAIT FAILPOINT upsert_pause_before_merge_commit PAUSE")
                assert node.query(f"SELECT count() FROM {table}") == "2\n"
                node.query(f"INSERT INTO {table} VALUES (1,11,'new'),(3,1,'three')", settings=settings)
            finally:
                node.query("SYSTEM DISABLE FAILPOINT upsert_pause_before_merge_commit")
            deletion.result(timeout=30)
        node.restart_clickhouse(kill=True)
        assert node.query(f"SELECT id,value FROM {table} ORDER BY id") == "1\tnew\n2\ttwo\n3\tthree\n"
    finally:
        node.query("SYSTEM DISABLE FAILPOINT upsert_pause_before_merge_commit")
        node.query(f"DROP TABLE {table} SYNC")


@pytest.mark.parametrize("point", ["upsert_log_throw_after_sync", "upsert_throw_after_rename"])
def test_delete_recovers_durable_commit(point):
    table = "delete_recovery"
    create_table(table)
    try:
        node.query(f"SYSTEM ENABLE FAILPOINT {point}")
        assert "FAULT_INJECTED" in node.query_and_get_error(f"DELETE FROM {table} WHERE id=1")
        node.query(f"SYSTEM DISABLE FAILPOINT {point}")
        assert "fenced" in node.query_and_get_error(f"SELECT * FROM {table}")
        node.restart_clickhouse(kill=True)
        assert node.query(f"SELECT id,value FROM {table}") == "2\ttwo\n"
        # Deleting a winner removes its version floor, including after recovery.
        node.query(f"INSERT INTO {table} VALUES (1,1,'again')", settings=settings)
        assert node.query(f"SELECT id,ver FROM {table} ORDER BY id") == "1\t1\n2\t10\n"
    finally:
        node.query(f"SYSTEM DISABLE FAILPOINT {point}")
        node.query(f"DROP TABLE {table} SYNC")


@pytest.mark.parametrize("point,committed", [
    ("upsert_ttl_pause_after_manifest_upgrade", False),
    ("upsert_ttl_pause_after_metadata_commit", True),
])
def test_drop_column_recovers_before_name_reuse(point, committed):
    table = "schema_recovery"
    create_table(table)
    try:
        node.query(f"SYSTEM ENABLE FAILPOINT {point}")
        with ThreadPoolExecutor(max_workers=1) as pool:
            alteration = pool.submit(node.query, f"ALTER TABLE {table} DROP COLUMN value")
            try:
                node.query(f"SYSTEM WAIT FAILPOINT {point} PAUSE")
                node.restart_clickhouse(kill=True)
            finally:
                node.query(f"SYSTEM DISABLE FAILPOINT {point}")
            with pytest.raises(Exception):
                alteration.result(timeout=30)
        if not committed:
            assert node.query(f"SELECT value FROM {table} ORDER BY id") == "one\ntwo\n"
            node.query(f"ALTER TABLE {table} DROP COLUMN value")
        node.query(f"ALTER TABLE {table} ADD COLUMN value UInt64 DEFAULT 123")
        assert node.query(f"SELECT id,value FROM {table} ORDER BY id") == "1\t123\n2\t123\n"
        node.restart_clickhouse(kill=True)
        assert node.query(f"SELECT id,value FROM {table} ORDER BY id") == "1\t123\n2\t123\n"
    finally:
        node.query(f"SYSTEM DISABLE FAILPOINT {point}")
        node.query(f"DROP TABLE {table} SYNC")


def test_column_alter_waits_for_prepared_insert():
    table = "schema_insert"
    create_table(table)
    try:
        node.query("SYSTEM ENABLE FAILPOINT upsert_pause_first_preparation")
        with ThreadPoolExecutor(max_workers=2) as pool:
            insertion = pool.submit(node.query, f"INSERT INTO {table} VALUES (3,1,'three')", settings=settings)
            alteration = None
            try:
                node.query("SYSTEM WAIT FAILPOINT upsert_pause_first_preparation PAUSE")
                alteration = pool.submit(node.query, f"ALTER TABLE {table} DROP COLUMN value", query_id="upsert_schema_wait")
                deadline = time.monotonic() + 30
                while node.query("SELECT count() FROM system.processes WHERE query_id='upsert_schema_wait'") != "1\n":
                    assert time.monotonic() < deadline
                    time.sleep(0.05)
                assert not alteration.done()
            finally:
                node.query("SYSTEM DISABLE FAILPOINT upsert_pause_first_preparation")
            insertion.result(timeout=30)
            alteration.result(timeout=30)
        node.query(f"ALTER TABLE {table} ADD COLUMN value String DEFAULT 'fresh'")
        assert node.query(f"SELECT id,value FROM {table} ORDER BY id") == "1\tfresh\n2\tfresh\n3\tfresh\n"
    finally:
        node.query("SYSTEM DISABLE FAILPOINT upsert_pause_first_preparation")
        node.query(f"DROP TABLE {table} SYNC")


def test_key_and_dependency_checks():
    table = "schema_validation"
    create_table(table)
    try:
        original = node.query(f"SHOW CREATE TABLE {table}")
        for command in ("DROP COLUMN id", "DROP COLUMN ver", "ADD COLUMN _upsert_bad UInt8"):
            assert node.query_and_get_error(f"ALTER TABLE {table} {command}")
            assert node.query(f"SHOW CREATE TABLE {table}") == original
        node.query(f"ALTER TABLE {table} ADD COLUMN derived UInt64 DEFAULT length(value)")
        assert node.query_and_get_error(f"ALTER TABLE {table} DROP COLUMN value")
        assert node.query(f"SELECT id,derived FROM {table} ORDER BY id") == "1\t3\n2\t3\n"
    finally:
        node.query(f"DROP TABLE {table} SYNC")


def test_reuse_names_in_one_alter():
    table = "schema_reuse"
    create_table(table)
    try:
        node.query(f"ALTER TABLE {table} ADD INDEX lookup value TYPE minmax GRANULARITY 1")
        node.query(f"ALTER TABLE {table} MATERIALIZE INDEX lookup")
        node.query(f"ALTER TABLE {table} DROP INDEX lookup, ADD INDEX lookup id TYPE minmax GRANULARITY 1")
        node.query(f"ALTER TABLE {table} DROP COLUMN value, ADD COLUMN value UInt64 DEFAULT 7")
        assert node.query(f"SELECT id,value FROM {table} ORDER BY id") == "1\t7\n2\t7\n"
        node.query(f"ALTER TABLE {table} MATERIALIZE INDEX lookup")
        assert node.query(f"SELECT id FROM {table} WHERE id=2 SETTINGS force_data_skipping_indices='lookup'") == "2\n"
        node.restart_clickhouse(kill=True)
        assert node.query(f"SELECT id,value FROM {table} ORDER BY id") == "1\t7\n2\t7\n"
    finally:
        node.query(f"DROP TABLE {table} SYNC")


def test_drop_index_recovers_before_name_reuse():
    table = "index_recovery"
    create_table(table)
    point = "upsert_ttl_pause_after_metadata_commit"
    try:
        node.query(f"ALTER TABLE {table} ADD INDEX lookup ver TYPE minmax GRANULARITY 1")
        node.query(f"ALTER TABLE {table} MATERIALIZE INDEX lookup")
        node.query(f"SYSTEM ENABLE FAILPOINT {point}")
        with ThreadPoolExecutor(max_workers=1) as pool:
            alteration = pool.submit(node.query, f"ALTER TABLE {table} DROP INDEX lookup")
            try:
                node.query(f"SYSTEM WAIT FAILPOINT {point} PAUSE")
                node.restart_clickhouse(kill=True)
            finally:
                node.query(f"SYSTEM DISABLE FAILPOINT {point}")
            with pytest.raises(Exception):
                alteration.result(timeout=30)
        node.query(f"ALTER TABLE {table} ADD INDEX lookup id TYPE minmax GRANULARITY 1")
        assert node.query(f"SELECT id FROM {table} WHERE id=1") == "1\n"
        node.query(f"ALTER TABLE {table} MATERIALIZE INDEX lookup")
        assert node.query(f"SELECT id FROM {table} WHERE id=2 SETTINGS force_data_skipping_indices='lookup'") == "2\n"
        node.restart_clickhouse(kill=True)
        assert node.query(f"SELECT id FROM {table} WHERE id=1") == "1\n"
    finally:
        node.query(f"SYSTEM DISABLE FAILPOINT {point}")
        node.query(f"DROP TABLE {table} SYNC")
