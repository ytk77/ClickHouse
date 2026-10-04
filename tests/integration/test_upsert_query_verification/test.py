import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    with_zookeeper=False,
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


@pytest.mark.parametrize("analyzer", [0, 1])
def test_queries_and_projection_do_not_wait_for_payload_mutex(analyzer):
    name = "async_verification_read"
    outer = "upsert_pause_background_verification"
    inner = "upsert_pause_payload_verification"
    try:
        node.query(f"""
            CREATE TABLE {name} (
                p UInt32, id UInt64, v UInt64, pad String,
                PROJECTION by_pad (SELECT * ORDER BY pad,id)
            ) ENGINE=UpsertMergeTree(v) PARTITION BY p ORDER BY id UNIQUE KEY id
            SETTINGS min_bytes_for_wide_part=0
        """)
        node.query(
            f"INSERT INTO {name} SELECT number%2,number,1,repeat('x',256) FROM numbers(100)"
        )
        node.query(f"DETACH TABLE {name}")
        node.query(f"SYSTEM ENABLE FAILPOINT {outer}")
        node.query(f"SYSTEM ENABLE FAILPOINT {inner}")
        node.query(f"ATTACH TABLE {name}")
        before = event("UpsertPayloadVerificationParts")
        node.query(f"SYSTEM DISABLE FAILPOINT {outer}")
        node.query(f"SYSTEM WAIT FAILPOINT {inner} PAUSE", timeout=30)
        settings = {"allow_experimental_analyzer": analyzer, "max_execution_time": 5}
        assert (
            node.query(
                f"SELECT count(),sum(id),sum(v),sum(length(pad)) FROM {name}",
                settings=settings,
                timeout=10,
            ).strip()
            == "100\t4950\t100\t25600"
        )
        assert (
            node.query(
                f"SELECT sum(length(pad)) FROM {name} WHERE pad=repeat('x',256)",
                settings={**settings, "force_optimize_projection": 1},
                timeout=10,
            ).strip()
            == "25600"
        )
        assert (
            node.query(
                f"SELECT count(),sum(id) FROM mergeTreeProjection(currentDatabase(),'{name}','by_pad')",
                settings=settings,
                timeout=10,
            ).strip()
            == "100\t4950"
        )
        assert event("UpsertPayloadVerificationParts") == before
        # Maintenance may also wait at the pause after this insert; finally releases both tasks.
        node.query(f"INSERT INTO {name} VALUES (0,0,2,'new')", timeout=10)
        assert (
            node.query(
                f"SELECT count(),sum(v) FROM {name}", settings=settings, timeout=10
            ).strip()
            == "100\t101"
        )
        assert event("UpsertPayloadVerificationParts") == before
        node.query(f"SYSTEM WAIT FAILPOINT {inner} PAUSE", timeout=10)
    finally:
        node.query(f"SYSTEM DISABLE FAILPOINT {inner}")
        node.query(f"SYSTEM DISABLE FAILPOINT {outer}")
        node.query(f"DROP TABLE IF EXISTS {name} SYNC")


def test_partition_export_rejects_corrupt_source_before_authorizing_output():
    source = "async_verification_source"
    target = "async_verification_target"
    point = "upsert_pause_background_verification"
    try:
        for name in (source, target):
            node.query(
                f"CREATE TABLE {name} (p UInt32,id UInt64,v UInt64,pad String) ENGINE=UpsertMergeTree(v) PARTITION BY p ORDER BY id UNIQUE KEY id SETTINGS min_bytes_for_wide_part=0"
            )
        node.query(
            f"INSERT INTO {source} SELECT 1,number,1,repeat('x',256) FROM numbers(100)"
        )
        path = node.query(
            f"SELECT path FROM system.parts WHERE database=currentDatabase() AND table='{source}' AND active"
        ).strip()
        node.query(f"DETACH TABLE {source}")
        node.query(f"SYSTEM ENABLE FAILPOINT {point}")
        node.exec_in_container(
            [
                "python3",
                "-c",
                "import pathlib,sys; p=pathlib.Path(sys.argv[1])/'pad.bin'; "
                "data=bytearray(p.read_bytes()); size=p.stat().st_size; data[-1]^=1; p.write_bytes(data); assert p.stat().st_size==size",
                path,
            ]
        )
        node.query(f"ATTACH TABLE {source}")
        before = event("UpsertPayloadVerificationParts")
        error = node.query_and_get_error(
            f"ALTER TABLE {target} REPLACE PARTITION 1 FROM {source}"
        )
        assert "authenticated files mismatch" in error
        assert event("UpsertPayloadVerificationParts") == before
        assert node.query(f"SELECT count() FROM {target}").strip() == "0"
        assert "verification failed" in node.query_and_get_error(
            f"SELECT count() FROM {source}"
        )
    finally:
        node.query(f"SYSTEM DISABLE FAILPOINT {point}")
        node.query(f"DROP TABLE IF EXISTS {source} SYNC")
        node.query(f"DROP TABLE IF EXISTS {target} SYNC")
