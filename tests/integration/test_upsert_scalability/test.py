import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", with_zookeeper=False, main_configs=["configs/upsert_defaults.xml"])


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
    before = int(node.query("SELECT sum(value) FROM system.events WHERE event='UpsertCatalogNodesAllocated'"))
    node.query("INSERT INTO scale_catalog VALUES (17,17,1)")
    after = int(node.query("SELECT sum(value) FROM system.events WHERE event='UpsertCatalogNodesAllocated'"))
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
    assert "upsert_recovery_threads = 0" in error
    assert "1..64" in error


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
    import random
    rng = random.Random(20261001)
    for _ in range(24):
        low = rng.randrange(40)
        high = low + rng.randrange(8)
        key = rng.randrange(4096)
        predicates.append(f"(p BETWEEN {low} AND {high}) OR (p={rng.randrange(40)} AND id>{key})")
    for predicate in predicates:
        sql = f"SELECT count(),sum(id),sum(v) FROM scale_pruning WHERE {predicate}"
        expected = node.query(sql, settings={"use_partition_pruning": 0})
        assert node.query(sql, settings={"use_partition_pruning": 1}) == expected
        assert node.query(sql, settings={"use_partition_pruning": 1, "enable_analyzer": 0}) == expected
    expected = node.query("SELECT count(),sum(id),sum(v) FROM scale_pruning")
    node.restart_clickhouse(kill=True)
    assert node.query("SELECT count(),sum(id),sum(v) FROM scale_pruning") == expected
    node.query("DROP TABLE scale_pruning SYNC")


def test_resource_errors_identify_setting_and_range():
    for setting, value, bounds in [
        ("upsert_index_cache_bytes", 0, "1048576.."),
        ("upsert_write_buffer_bytes", 0, "1048576.."),
        ("upsert_write_buffer_count", 1, "2..64"),
        ("upsert_background_jobs", 0, "1..128"),
        ("upsert_max_open_files", 19, "20..2147483647"),
        ("upsert_recovery_threads", 65, "1..64"),
        ("upsert_wal_bytes", 1, "16777216.."),
    ]:
        error = node.query_and_get_error(f"""
            CREATE TABLE scale_invalid_resource (id UInt64)
            ENGINE=UpsertMergeTree ORDER BY id UNIQUE KEY id
            SETTINGS {setting}={value}
        """)
        assert f"{setting} = {value}" in error
        assert bounds in error


def test_pruning_join_and_additional_filter_agree():
    node.query("DROP TABLE IF EXISTS scale_filtered SYNC")
    node.query("""
        CREATE TABLE scale_filtered (p UInt32,id UInt64,v UInt64)
        ENGINE=UpsertMergeTree(v) PARTITION BY p ORDER BY id UNIQUE KEY id
        SETTINGS upsert_mask_cache_bytes=128
    """)
    node.query("INSERT INTO scale_filtered SELECT number%31,number,1 FROM numbers(2048)")
    node.query("INSERT INTO scale_filtered SELECT number%31,number,2 FROM numbers(1024)")
    queries = [
        "SELECT count(),sum(v) FROM scale_filtered AS s INNER JOIN "
        "(SELECT number AS id FROM numbers(256)) AS n ON s.id=n.id WHERE s.p IN (1,3,7)",
        "SELECT count(),sum(v) FROM scale_filtered WHERE p BETWEEN 4 AND 11",
        "SELECT count(),sum(v) FROM scale_filtered WHERE p=2 OR id IN (1,7,123)",
    ]
    for analyzer in (0, 1):
        for sql in queries:
            settings = {"enable_analyzer": analyzer, "use_partition_pruning": 0}
            expected = node.query(sql, settings=settings)
            settings["use_partition_pruning"] = 1
            assert node.query(sql, settings=settings) == expected
    settings = {"additional_table_filters": "{'default.scale_filtered': 'p IN (3, 7)'}"}
    sql = "SELECT count(),sum(id),sum(v) FROM scale_filtered WHERE id<1000"
    assert node.query(sql, settings={**settings, "use_partition_pruning": 1}) == node.query(
        sql, settings={**settings, "use_partition_pruning": 0}
    )
    node.query("DROP TABLE scale_filtered SYNC")


def test_maintenance_ttl_beyond_one_slice():
    import time

    def wait_until_empty():
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            if node.query("SELECT count() FROM scale_ttl_slices").strip() == "0":
                return
            time.sleep(0.2)
        assert node.query("SELECT count() FROM scale_ttl_slices").strip() == "0"

    node.query("DROP TABLE IF EXISTS scale_ttl_slices SYNC")
    node.query("""
        CREATE TABLE scale_ttl_slices (p UInt32,id UInt64,ts DateTime)
        ENGINE=UpsertMergeTree PARTITION BY p ORDER BY id UNIQUE KEY id
        TTL ts + INTERVAL 1 SECOND
    """)
    node.query("""
        INSERT INTO scale_ttl_slices SELECT number,number,now()-INTERVAL 1 DAY FROM numbers(300)
        SETTINGS max_partitions_per_insert_block=1024
    """)
    wait_until_empty()
    # A publication after an idle scan must wake maintenance for the new part.
    node.query("INSERT INTO scale_ttl_slices VALUES (301,301,now()-INTERVAL 1 DAY)")
    wait_until_empty()
    node.query("DROP TABLE scale_ttl_slices SYNC")


def test_non_upsert_settings_validation_preserves_inherited_defaults():
    node.query("DROP TABLE IF EXISTS scale_plain SYNC")
    node.query("CREATE TABLE scale_plain (id UInt64) ENGINE=MergeTree ORDER BY id")
    node.query("INSERT INTO scale_plain VALUES (1)")
    assert node.query("SELECT * FROM scale_plain").strip() == "1"
    error = node.query_and_get_error(
        "ALTER TABLE scale_plain MODIFY SETTING upsert_recovery_threads=2"
    )
    assert "upsert_recovery_threads applies only to UpsertMergeTree" in error
    error = node.query_and_get_error(
        "CREATE TABLE scale_wrong_engine (id UInt64) ENGINE=MergeTree ORDER BY id "
        "SETTINGS upsert_index_cache_bytes=8388608"
    )
    assert "upsert_index_cache_bytes applies only to UpsertMergeTree" in error
    node.query("DETACH TABLE scale_plain")
    node.query("ATTACH TABLE scale_plain")
    assert node.query("SELECT * FROM scale_plain").strip() == "1"
    node.query("DROP TABLE scale_plain SYNC")


def test_legacy_non_upsert_metadata_remains_attachable():
    import uuid

    node.query("DROP TABLE IF EXISTS scale_legacy_settings SYNC")
    # Full ATTACH emulates metadata written by the earlier binary that accepted this setting.
    node.query(f"""
        ATTACH TABLE scale_legacy_settings UUID '{uuid.uuid4()}' (id UInt64) ENGINE=MergeTree ORDER BY id
        SETTINGS upsert_index_cache_bytes=8388608
    """)
    node.query("INSERT INTO scale_legacy_settings VALUES (7)")
    node.query("DETACH TABLE scale_legacy_settings")
    node.query("ATTACH TABLE scale_legacy_settings")
    assert node.query("SELECT * FROM scale_legacy_settings").strip() == "7"
    node.query("DROP TABLE scale_legacy_settings SYNC")


def test_maintenance_corruption_fences_and_recovery_revalidates():
    import time

    node.query("DROP TABLE IF EXISTS scale_corrupt_candidate SYNC")
    node.query("""
        CREATE TABLE scale_corrupt_candidate (id UInt64)
        ENGINE=UpsertMergeTree ORDER BY id UNIQUE KEY id
    """)
    before = int(node.query(
        "SELECT sum(value) FROM system.events WHERE event='UpsertMaintenanceCorruptionFailures'"
    ))
    node.query("SYSTEM ENABLE FAILPOINT upsert_maintenance_corrupt_candidate")
    try:
        node.query("INSERT INTO scale_corrupt_candidate VALUES (1)")
        deadline = time.monotonic() + 10
        fenced = False
        while time.monotonic() < deadline:
            try:
                node.query("SELECT count() FROM scale_corrupt_candidate")
            except Exception as error:
                assert "fenced" in str(error)
                fenced = True
                break
            time.sleep(0.1)
        assert fenced
        assert "fenced" in node.query_and_get_error("INSERT INTO scale_corrupt_candidate VALUES (2)")
        after = int(node.query(
            "SELECT sum(value) FROM system.events WHERE event='UpsertMaintenanceCorruptionFailures'"
        ))
        assert after > before
    finally:
        node.query("SYSTEM DISABLE FAILPOINT upsert_maintenance_corrupt_candidate")
    # The injected error changes no payload; recovery and first use must still verify it.
    node.restart_clickhouse(kill=True)
    assert node.query("SELECT * FROM scale_corrupt_candidate").strip() == "1"
    node.query("DROP TABLE scale_corrupt_candidate SYNC")
