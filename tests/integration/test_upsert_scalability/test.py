import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", with_zookeeper=False)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_catalog_partition_pruning_and_retirement():
    node.query("DROP TABLE IF EXISTS scale_catalog SYNC")
    node.query("""
        CREATE TABLE scale_catalog (p UInt32, id UInt64, v UInt64)
        ENGINE=UpsertMergeTree(v) PARTITION BY p ORDER BY id UNIQUE KEY id
    """)
    node.query("""
        INSERT INTO scale_catalog SELECT number % 256, number, 1 FROM numbers(8192)
        SETTINGS max_partitions_per_insert_block=1024
    """)
    before = int(node.query("SELECT sum(value) FROM system.events WHERE event='UpsertSnapshotPartsVisited'"))
    node.query("INSERT INTO scale_catalog VALUES (17,17,1)")
    after = int(node.query("SELECT sum(value) FROM system.events WHERE event='UpsertSnapshotPartsVisited'"))
    assert 0 < after - before < 128
    assert node.query("SELECT count(), sum(id), sum(v) FROM scale_catalog").strip() == "8192\t33550336\t8192"
    assert node.query("SELECT count(), sum(v) FROM scale_catalog WHERE p=17").strip() == "32\t32"
    node.query("""
        INSERT INTO scale_catalog SELECT number % 256, number, 2 FROM numbers(8192)
        SETTINGS max_partitions_per_insert_block=1024
    """)
    assert node.query("SELECT count(), sum(v) FROM scale_catalog WHERE p IN (17, 18)").strip() == "64\t128"
    node.query("ALTER TABLE scale_catalog DROP PARTITION 17")
    assert node.query("SELECT count(), sum(v) FROM scale_catalog").strip() == "8160\t16320"
    assert node.query("SELECT count() FROM scale_catalog WHERE p=17").strip() == "0"
    node.query("TRUNCATE TABLE scale_catalog")
    assert node.query("SELECT count() FROM scale_catalog").strip() == "0"
    node.query("INSERT INTO scale_catalog VALUES (17,17,3)")
    assert node.query("SELECT * FROM scale_catalog").strip() == "17\t17\t3"
    node.query("DROP TABLE scale_catalog SYNC")


def test_resource_settings_and_parallel_recovery():
    node.query("DROP TABLE IF EXISTS scale_resources SYNC")
    node.query("""
        CREATE TABLE scale_resources (p UInt32, id UInt64, v UInt64)
        ENGINE=UpsertMergeTree(v) PARTITION BY p ORDER BY id UNIQUE KEY id
        SETTINGS upsert_index_cache_bytes=8388608, upsert_write_buffer_bytes=4194304,
        upsert_write_buffer_count=2, upsert_wal_bytes=8388608,
        upsert_background_jobs=2, upsert_max_open_files=32, upsert_recovery_threads=4,
        upsert_partitioned_filters=1
    """)
    node.query("INSERT INTO scale_resources SELECT number%32,number,1 FROM numbers(4096)")
    node.query("INSERT INTO scale_resources SELECT number%32,number,2 FROM numbers(1024)")
    expected = node.query("SELECT count(),sum(id),sum(v) FROM scale_resources")
    node.restart_clickhouse(kill=True)
    assert node.query("SELECT count(),sum(id),sum(v) FROM scale_resources") == expected
    assert expected.strip() == "4096\t8386560\t5120"
    node.query("INSERT INTO scale_resources VALUES (0,0,3)")
    assert node.query("SELECT v FROM scale_resources WHERE p=0 AND id=0").strip() == "3"
    node.query("DROP TABLE scale_resources SYNC")


def test_invalid_resource_budget():
    error = node.query_and_get_error("""
        CREATE TABLE scale_bad_budget (id UInt64)
        ENGINE=UpsertMergeTree ORDER BY id UNIQUE KEY id
        SETTINGS upsert_recovery_threads=0
    """)
    assert "Invalid UpsertMergeTree resource budgets" in error


def test_catalog_schema_and_projection():
    node.query("DROP TABLE IF EXISTS scale_schema SYNC")
    node.query("""
        CREATE TABLE scale_schema (id UInt64, v UInt64) ENGINE=UpsertMergeTree(v)
        ORDER BY id UNIQUE KEY id
    """)
    node.query("INSERT INTO scale_schema SELECT number,1 FROM numbers(20000)")
    node.query("ALTER TABLE scale_schema ADD COLUMN pad UInt64 DEFAULT id+v")
    node.query("ALTER TABLE scale_schema ADD PROJECTION by_version (SELECT * ORDER BY v,id)")
    node.query("ALTER TABLE scale_schema MATERIALIZE PROJECTION by_version")
    assert node.query("SELECT sum(pad) FROM scale_schema").strip() == "200010000"
    node.query("DELETE FROM scale_schema WHERE id%2=0")
    assert node.query("SELECT count(),sum(pad) FROM scale_schema").strip() == "10000\t100010000"
    node.query("ALTER TABLE scale_schema DROP PROJECTION by_version")
    node.query("ALTER TABLE scale_schema DROP COLUMN pad")
    assert node.query("SELECT count() FROM scale_schema").strip() == "10000"
    node.query("DROP TABLE scale_schema SYNC")


def test_catalog_pruned_and_full_scans_agree():
    node.query("DROP TABLE IF EXISTS scale_pruning SYNC")
    node.query("""
        CREATE TABLE scale_pruning (p UInt32,id UInt64,v UInt64)
        ENGINE=UpsertMergeTree(v) PARTITION BY p ORDER BY id UNIQUE KEY id
        SETTINGS upsert_mask_cache_bytes=0,upsert_recovery_threads=1,
            upsert_partitioned_filters=0
    """)
    node.query("INSERT INTO scale_pruning SELECT number%32,number,1 FROM numbers(4096)")
    node.query("INSERT INTO scale_pruning SELECT number%32,number,2 FROM numbers(2048)")
    node.query("DELETE FROM scale_pruning WHERE id%7=0")
    predicates = [
        "p=17",
        "p IN (0,7,17,31)",
        "p>=9 AND p<12",
        "(p=9 AND id>100) OR p=31",
        "p IN (SELECT number FROM numbers(4))",
        "p=99",
        "p=17 OR id=42",
    ]
    for predicate in predicates:
        sql = f"SELECT count(),sum(id),sum(v) FROM scale_pruning WHERE {predicate}"
        expected = node.query(sql, settings={"use_partition_pruning": 0})
        assert node.query(sql, settings={"use_partition_pruning": 1}) == expected
        assert node.query(sql, settings={"use_partition_pruning": 1, "enable_analyzer": 0}) == expected
    expected = node.query("SELECT count(),sum(id),sum(v) FROM scale_pruning")
    node.restart_clickhouse(kill=True)
    assert node.query("SELECT count(),sum(id),sum(v) FROM scale_pruning") == expected
    node.query("DROP TABLE scale_pruning SYNC")
