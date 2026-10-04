import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node", with_zookeeper=False,
    main_configs=["../test_upsert_scalability/configs/upsert_defaults.xml"],
)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def event(name):
    return int(node.query(f"SELECT sum(value) FROM system.events WHERE event='{name}'"))


def create(name):
    node.query(f"DROP TABLE IF EXISTS {name} SYNC")
    node.query(f"""
        CREATE TABLE {name} (p UInt32, id UInt64, v UInt64, pad String)
        ENGINE=UpsertMergeTree(v) PARTITION BY p ORDER BY id UNIQUE KEY id
        SETTINGS min_bytes_for_wide_part=0
    """)
    node.query(f"INSERT INTO {name} SELECT number%2,number,1,repeat('x',256) FROM numbers(100)")


def check_partition_scoped_first_read(analyzer):
    name = "staged_recovery_read"
    try:
        create(name)
        node.query(f"DETACH TABLE {name}")
        node.query("SYSTEM ENABLE FAILPOINT upsert_pause_background_verification")
        metadata_before = event("UpsertRecoveryMetadataParts")
        full_before = event("UpsertRecoveryParts")
        node.query(f"ATTACH TABLE {name}")
        assert event("UpsertRecoveryMetadataParts") - metadata_before == 2
        assert event("UpsertRecoveryParts") == full_before
        before = event("UpsertPayloadVerificationParts")
        assert node.query(f"SELECT count(),sum(v) FROM {name} WHERE p=0", settings={"allow_experimental_analyzer": analyzer}).strip() == "50\t50"
        assert event("UpsertPayloadVerificationParts") - before == 1, (before, event("UpsertPayloadVerificationParts"))
        assert node.query(f"SELECT sum(length(pad)) FROM {name} WHERE p=0").strip() == "12800"
        assert event("UpsertPayloadVerificationParts") - before == 1, (before, event("UpsertPayloadVerificationParts"))
        assert node.query(f"SELECT count(),sum(v) FROM {name}").strip() == "100\t100"
        assert event("UpsertPayloadVerificationParts") - before == 2
        node.query(f"INSERT INTO {name} VALUES (0,0,2,'new')")
        assert node.query(f"SELECT count(),sum(v) FROM {name}").strip() == "100\t101"
    finally:
        node.query("SYSTEM DISABLE FAILPOINT upsert_pause_background_verification")
        node.query(f"DROP TABLE IF EXISTS {name} SYNC")


def test_metadata_recovery_and_partition_scoped_first_read():
    check_partition_scoped_first_read(1)


def test_legacy_analyzer_partition_scoped_first_read():
    check_partition_scoped_first_read(0)


def test_same_size_corruption_is_rejected_before_query_results():
    name = "staged_recovery_corrupt"
    try:
        create(name)
        path = node.query(f"SELECT path FROM system.parts WHERE database=currentDatabase() AND table='{name}' AND active AND partition_id='1'").strip()
        node.query(f"DETACH TABLE {name}")
        node.query("SYSTEM ENABLE FAILPOINT upsert_pause_background_verification")
        node.exec_in_container([
            "python3", "-c",
            "import pathlib,sys; p=pathlib.Path(sys.argv[1])/'pad.bin'; "
            "data=bytearray(p.read_bytes()); data[-1]^=1; p.write_bytes(data)",
            path,
        ])
        node.query(f"ATTACH TABLE {name}")
        assert node.query(f"SELECT count() FROM {name} WHERE p=0").strip() == "50"
        error = node.query_and_get_error(f"SELECT sum(length(pad)) FROM {name} WHERE p=1")
        assert "authenticated files mismatch" in error
        assert "verification failed" in node.query_and_get_error(f"INSERT INTO {name} VALUES (0,102,1,'new')")
    finally:
        node.query("SYSTEM DISABLE FAILPOINT upsert_pause_background_verification")
        node.query(f"DROP TABLE IF EXISTS {name} SYNC")


def test_background_verifies_without_a_data_query():
    name = "staged_recovery_background"
    try:
        create(name)
        node.query(f"DETACH TABLE {name}")
        node.query("SYSTEM ENABLE FAILPOINT upsert_pause_background_verification")
        node.query(f"ATTACH TABLE {name}")
        before = event("UpsertPayloadVerificationParts")
        node.query("SYSTEM DISABLE FAILPOINT upsert_pause_background_verification")
        deadline = time.monotonic() + 30
        while event("UpsertPayloadVerificationParts") - before < 2:
            assert time.monotonic() < deadline, "Background payload verification did not complete"
            time.sleep(0.1)
        after = event("UpsertPayloadVerificationParts")
        assert node.query(f"SELECT count(),sum(v) FROM {name}").strip() == "100\t100"
        assert event("UpsertPayloadVerificationParts") == after
    finally:
        node.query("SYSTEM DISABLE FAILPOINT upsert_pause_background_verification")
        node.query(f"DROP TABLE IF EXISTS {name} SYNC")


def test_background_corruption_fences_without_reading_the_bad_partition():
    name = "staged_recovery_background_corrupt"
    try:
        create(name)
        path = node.query(f"SELECT path FROM system.parts WHERE database=currentDatabase() AND table='{name}' AND active AND partition_id='1'").strip()
        node.query(f"DETACH TABLE {name}")
        node.query("SYSTEM ENABLE FAILPOINT upsert_pause_background_verification")
        node.exec_in_container([
            "python3", "-c",
            "import pathlib,sys; p=pathlib.Path(sys.argv[1])/'pad.bin'; "
            "data=bytearray(p.read_bytes()); data[-1]^=1; p.write_bytes(data)",
            path,
        ])
        node.query(f"ATTACH TABLE {name}")
        assert node.query(f"SELECT count() FROM {name} WHERE p=99").strip() == "0"
        node.query("SYSTEM DISABLE FAILPOINT upsert_pause_background_verification")
        deadline = time.monotonic() + 30
        while True:
            try:
                node.query(f"SELECT count() FROM {name} WHERE p=99")
            except Exception as error:
                assert "fenced" in str(error) or "verification failed" in str(error)
                break
            assert time.monotonic() < deadline, "Background corruption did not fence the table"
            time.sleep(0.1)
        error = node.query_and_get_error(f"INSERT INTO {name} VALUES (0,102,1,'new')")
        assert "fenced" in error or "verification failed" in error
    finally:
        node.query("SYSTEM DISABLE FAILPOINT upsert_pause_background_verification")
        node.query(f"DROP TABLE IF EXISTS {name} SYNC")
