from concurrent.futures import ThreadPoolExecutor

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


def event():
    return int(
        node.query(
            "SELECT sum(value) FROM system.events WHERE event='UpsertTruncateCompactions'"
        )
    )


def create(name):
    node.query(f"DROP TABLE IF EXISTS {name} SYNC")
    node.query(f"""
        CREATE TABLE {name} (id UInt64, v UInt64)
        ENGINE=UpsertMergeTree(v) ORDER BY id UNIQUE KEY id
        SETTINGS upsert_write_buffer_bytes=1048576, upsert_wal_bytes=4194304
    """)


def test_truncate_reload_and_restart():
    name = "truncate_reload"
    try:
        create(name)
        node.query(f"INSERT INTO {name} SELECT number,10 FROM numbers(100000)")
        before = event()
        node.query(f"TRUNCATE TABLE {name}")
        assert event() == before + 1
        assert node.query(f"SELECT count() FROM {name}").strip() == "0"
        node.query(f"INSERT INTO {name} SELECT number,1 FROM numbers(100000)")
        expected = "100000\t4999950000\t100000"
        assert (
            node.query(f"SELECT count(),sum(id),sum(v) FROM {name}").strip() == expected
        )
        node.restart_clickhouse(kill=True)
        assert (
            node.query(f"SELECT count(),sum(id),sum(v) FROM {name}").strip() == expected
        )
        assert event() == 0
    finally:
        node.query(f"DROP TABLE IF EXISTS {name} SYNC")


def test_truncate_excludes_reload_until_compaction_finishes():
    name = "truncate_admission"
    point = "upsert_pause_before_truncate_compaction"
    try:
        create(name)
        node.query(f"INSERT INTO {name} VALUES (1,10)")
        before = event()
        node.query(f"SYSTEM ENABLE FAILPOINT {point}")
        with ThreadPoolExecutor(max_workers=1) as pool:
            truncating = pool.submit(node.query, f"TRUNCATE TABLE {name}")
            try:
                node.query(f"SYSTEM WAIT FAILPOINT {point} PAUSE", timeout=30)
                assert event() == before
                error = node.query_and_get_error(
                    f"INSERT INTO {name} VALUES (1,1)",
                    settings={"lock_acquire_timeout": 1},
                    timeout=10,
                )
                assert "lock" in error.lower()
                assert not truncating.done()
            finally:
                node.query(f"SYSTEM DISABLE FAILPOINT {point}")
            truncating.result(timeout=30)
        assert event() == before + 1
        node.query(f"INSERT INTO {name} VALUES (1,1)")
        assert node.query(f"SELECT * FROM {name}").strip() == "1\t1"
    finally:
        node.query(f"SYSTEM DISABLE FAILPOINT {point}")
        node.query(f"DROP TABLE IF EXISTS {name} SYNC")


@pytest.mark.parametrize(
    "point,committed",
    [
        ("upsert_pause_before_state_write", False),
        ("upsert_pause_after_state_sync", True),
        ("upsert_pause_before_truncate_compaction", True),
    ],
)
def test_interrupted_truncate_recovery(point, committed):
    name = "truncate_recovery"
    try:
        create(name)
        node.query(f"INSERT INTO {name} VALUES (1,10)")
        node.query(f"SYSTEM ENABLE FAILPOINT {point}")
        with ThreadPoolExecutor(max_workers=1) as pool:
            truncating = pool.submit(node.query, f"TRUNCATE TABLE {name}")
            node.query(f"SYSTEM WAIT FAILPOINT {point} PAUSE", timeout=30)
            node.restart_clickhouse(kill=True)
            with pytest.raises(Exception):
                truncating.result(timeout=30)
        assert node.query(f"SELECT count() FROM {name}").strip() == (
            "0" if committed else "1"
        )
        assert event() == int(committed)
        node.query(f"INSERT INTO {name} VALUES (1,1)")
        assert node.query(f"SELECT v FROM {name}").strip() == (
            "1" if committed else "10"
        )
    finally:
        node.query(f"SYSTEM DISABLE FAILPOINT {point}")
        node.query(f"DROP TABLE IF EXISTS {name} SYNC")
