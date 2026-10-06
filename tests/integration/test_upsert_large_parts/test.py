import time

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


def wait_for_compaction(name, expected_rows, timeout=300, max_parts=1, min_largest_part_rows=0):
    deadline = time.monotonic() + timeout
    result = ""
    while time.monotonic() < deadline:
        result = node.query(
            f"SELECT count(), sum(rows), max(rows) FROM system.parts "
            f"WHERE active AND database=currentDatabase() AND table='{name}'"
        ).strip()
        count, physical_rows, largest_rows = map(int, result.split("\t"))
        if 0 < count <= max_parts and physical_rows == expected_rows and largest_rows >= min_largest_part_rows:
            return
        time.sleep(0.2)
    raise AssertionError(f"Compaction of {name} did not finish: {result}")


@pytest.mark.parametrize("wide", [True, False])
def test_large_part_survives_updates_deletion_and_restart(wide):
    name = "upsert_large_wide" if wide else "upsert_large_compact"
    rows = 1600000
    node.query(f"DROP TABLE IF EXISTS {name} SYNC")
    node.query(
        f"""CREATE TABLE {name} (id UInt64, v UInt64, settle Date, bet UInt64, pad String)
        ENGINE=UpsertMergeTree(v) ORDER BY id UNIQUE KEY id
        SETTINGS upsert_maintenance_target_bytes=1073741824,
        min_bytes_for_wide_part={0 if wide else 1099511627776}, min_rows_for_wide_part=0"""
    )
    try:
        node.query(f"SYSTEM STOP MERGES {name}")
        for start in range(0, rows, 200000):
            node.query(
                f"INSERT INTO {name} SELECT number, 1, toDate('2026-09-01')+number%30, "
                f"number%97, repeat('p',64) FROM numbers({start},200000)",
                settings={"max_threads": 1, "max_insert_threads": 1},
            )
        expected = node.query(f"SELECT count(),sum(id),sum(v),sum(bet) FROM {name}")
        grouped = node.query(f"SELECT settle,sum(bet) FROM {name} GROUP BY settle ORDER BY settle")
        node.query(f"SYSTEM START MERGES {name}")
        wait_for_compaction(name, rows)
        assert node.query(f"SELECT count(),sum(id),sum(v),sum(bet) FROM {name}") == expected
        assert node.query(f"SELECT settle,sum(bet) FROM {name} GROUP BY settle ORDER BY settle") == grouped
        assert node.query(
            f"SELECT part_type FROM system.parts WHERE active AND table='{name}' "
            "AND database=currentDatabase()"
        ).strip() == ("Wide" if wide else "Compact")
        node.restart_clickhouse(kill=True)
        assert node.query(f"SELECT count(),sum(id),sum(v),sum(bet) FROM {name}") == expected
        node.query(f"SYSTEM STOP MERGES {name}")
        node.query(
            f"INSERT INTO {name} SELECT number,2,toDate('2026-09-01')+number%30, "
            "number%97+1,repeat('q',64) FROM numbers(100000)"
        )
        assert node.query(f"SELECT count(),sum(v),countIf(pad=repeat('q',64)) FROM {name}").strip() == (
            "1600000\t1700000\t100000"
        )
        node.query(f"ALTER TABLE {name} DELETE WHERE id<800000")
        assert node.query(f"SELECT count(),min(id),max(id),sum(v) FROM {name}").strip() == (
            "800000\t800000\t1599999\t800000"
        )
        node.query(f"SYSTEM START MERGES {name}")
        wait_for_compaction(name, 800000, max_parts=3)
        node.restart_clickhouse(kill=True)
        assert node.query(f"SELECT count(),sum(id),sum(v) FROM {name}").strip() == (
            "800000\t959999600000\t800000"
        )
    finally:
        node.query(f"DROP TABLE IF EXISTS {name} SYNC")


def test_single_row_delete_splits_large_part_then_reconsolidates():
    # This case requires enough server/background headroom for an effective target of at least 256 MiB.
    name = "upsert_large_single_delete"
    rows = 1800000
    node.query(f"DROP TABLE IF EXISTS {name} SYNC")
    node.query(f"CREATE TABLE {name} (id UInt64,v UInt64,pad String) ENGINE=UpsertMergeTree(v) ORDER BY id UNIQUE KEY id")
    try:
        node.query(f"SYSTEM STOP MERGES {name}")
        for start in range(0, rows, 200000):
            node.query(f"INSERT INTO {name} SELECT number,1,repeat('x',64) FROM numbers({start},200000)", settings={"max_threads":1,"max_insert_threads":1})
        node.query(f"SYSTEM START MERGES {name}")
        wait_for_compaction(name, rows)
        node.query(f"SYSTEM STOP MERGES {name}")
        before = node.query(f"SELECT name FROM system.parts WHERE active AND table='{name}' AND database=currentDatabase()")
        node.query(f"ALTER TABLE {name} DELETE WHERE id>={rows}", settings={"max_memory_usage":1073741824,"max_threads":1})
        assert node.query(f"SELECT name FROM system.parts WHERE active AND table='{name}' AND database=currentDatabase()") == before
        node.query(f"ALTER TABLE {name} DELETE WHERE id=0", settings={"max_memory_usage":1073741824,"max_threads":1})
        assert node.query(f"SELECT count(),sum(id),sum(v) FROM {name}").strip() == "1799999\t1619999100000\t1799999"
        mutation_parts = int(node.query(f"SELECT count() FROM system.parts WHERE active AND table='{name}' AND database=currentDatabase()"))
        assert mutation_parts > 2
        node.query(f"SYSTEM START MERGES {name}")
        # Healthy compaction can retain a small tail in its own size tier.
        wait_for_compaction(name, rows-1, max_parts=2, min_largest_part_rows=1048577)
        node.restart_clickhouse(kill=True)
        assert node.query(f"SELECT count(),sum(id),sum(v) FROM {name}").strip() == "1799999\t1619999100000\t1799999"
    finally:
        node.query(f"DROP TABLE IF EXISTS {name} SYNC")


def test_large_delete_interruption_preserves_completed_units():
    name = "upsert_large_interrupted_delete"
    rows = 1600000
    node.query(f"DROP TABLE IF EXISTS {name} SYNC")
    node.query(f"CREATE TABLE {name} (id UInt64,v UInt64,pad String) ENGINE=UpsertMergeTree(v) ORDER BY id UNIQUE KEY id")
    try:
        node.query(f"SYSTEM STOP MERGES {name}")
        for start in range(0, rows, 200000):
            node.query(f"INSERT INTO {name} SELECT number,1,repeat('x',64) FROM numbers({start},200000)", settings={"max_threads":1,"max_insert_threads":1})
        node.query(f"SYSTEM START MERGES {name}")
        wait_for_compaction(name, rows)
        node.query(f"SYSTEM STOP MERGES {name}")
        node.query("SYSTEM ENABLE FAILPOINT upsert_log_throw_after_sync")
        assert "failure after disk-state sync" in node.query_and_get_error(f"ALTER TABLE {name} DELETE WHERE id<1000000", settings={"max_memory_usage":1073741824,"max_threads":1}).lower()
        node.query("SYSTEM DISABLE FAILPOINT upsert_log_throw_after_sync")
        node.restart_clickhouse(kill=True)
        node.query(f"SYSTEM STOP MERGES {name}")
        count, distinct = map(int,node.query(f"SELECT count(),uniqExact(id) FROM {name}").strip().split("\t"))
        # `ORDER BY id` puts low IDs first, so the first durable unit applies some deletions.
        assert 600000 < count < rows
        assert count == distinct
        node.query(f"ALTER TABLE {name} DELETE WHERE id<1000000", settings={"max_memory_usage":1073741824,"max_threads":1})
        assert node.query(f"SELECT count(),sum(id),sum(v) FROM {name}").strip() == "600000\t779999700000\t600000"
        node.query(f"SYSTEM START MERGES {name}")
        wait_for_compaction(name, 600000)
    finally:
        node.query("SYSTEM DISABLE FAILPOINT upsert_log_throw_after_sync")
        node.query(f"DROP TABLE IF EXISTS {name} SYNC")


def test_large_target_setting_persists():
    name = "upsert_large_target"
    node.query(f"DROP TABLE IF EXISTS {name} SYNC")
    node.query(
        f"CREATE TABLE {name} (id UInt64) ENGINE=UpsertMergeTree ORDER BY id UNIQUE KEY id"
    )
    try:
        for target in (1073741824, 4294967296, 8589934592):
            node.query(f"ALTER TABLE {name} MODIFY SETTING upsert_maintenance_target_bytes={target}")
            assert f"upsert_maintenance_target_bytes = {target}" in node.query(f"SHOW CREATE TABLE {name}")
        before = node.query(f"SHOW CREATE TABLE {name}")
        assert "8589934592" in node.query_and_get_error(
            f"ALTER TABLE {name} MODIFY SETTING upsert_maintenance_target_bytes=8589934593"
        )
        assert node.query(f"SHOW CREATE TABLE {name}") == before
        node.restart_clickhouse()
        assert node.query(f"SHOW CREATE TABLE {name}") == before
    finally:
        node.query(f"DROP TABLE IF EXISTS {name} SYNC")


def test_large_payload_recovers_first_durable_relocation_before_rename():
    name = "upsert_large_interrupted"
    node.query(f"DROP TABLE IF EXISTS {name} SYNC")
    node.query(
        f"CREATE TABLE {name} (id UInt64,v UInt64) ENGINE=UpsertMergeTree(v) "
        "ORDER BY id UNIQUE KEY id SETTINGS upsert_maintenance_target_bytes=1073741824"
    )
    try:
        node.query(f"SYSTEM STOP MERGES {name}")
        for start in range(0, 1600000, 200000):
            node.query(
                f"INSERT INTO {name} SELECT number,1 FROM numbers({start},200000)",
                settings={"max_threads": 1, "max_insert_threads": 1},
            )
        before = int(node.query("SELECT sum(value) FROM system.events WHERE event='UpsertMaintenanceFailures'"))
        node.query("SYSTEM ENABLE FAILPOINT upsert_log_throw_after_sync")
        node.query(f"SYSTEM START MERGES {name}")
        deadline = time.monotonic() + 180
        while int(node.query("SELECT sum(value) FROM system.events WHERE event='UpsertMaintenanceFailures'")) <= before:
            assert time.monotonic() < deadline
            time.sleep(0.1)
        node.query("SYSTEM DISABLE FAILPOINT upsert_log_throw_after_sync")
        node.restart_clickhouse(kill=True)
        assert node.query(f"SELECT count(),sum(id),sum(v) FROM {name}").strip() == (
            "1600000\t1279999200000\t1600000"
        )
        wait_for_compaction(name, 1600000)
        node.restart_clickhouse(kill=True)
        assert node.query(f"SELECT count(),sum(v) FROM {name}").strip() == "1600000\t1600000"
    finally:
        node.query("SYSTEM DISABLE FAILPOINT upsert_log_throw_after_sync")
        node.query(f"DROP TABLE IF EXISTS {name} SYNC")


def test_large_lineage_import_budget_rejection_preserves_detached_payload():
    name = "upsert_large_import"
    node.query(f"DROP TABLE IF EXISTS {name} SYNC")
    node.query(
        f"CREATE TABLE {name} (id UInt64,v UInt64,p UInt8) ENGINE=UpsertMergeTree(v) "
        "PARTITION BY p ORDER BY id UNIQUE KEY id "
        "SETTINGS upsert_maintenance_target_bytes=1073741824"
    )
    try:
        node.query(f"SYSTEM STOP MERGES {name}")
        for start in range(0, 1600000, 200000):
            node.query(
                f"INSERT INTO {name} SELECT number,1,1 FROM numbers({start},200000)",
                settings={"max_threads": 1, "max_insert_threads": 1},
            )
        node.query(f"SYSTEM START MERGES {name}")
        wait_for_compaction(name, 1600000)
        part_name = node.query(
            f"SELECT name FROM system.parts WHERE active AND database=currentDatabase() AND table='{name}'"
        ).strip()
        node.query(f"ALTER TABLE {name} DETACH PARTITION 1")
        assert node.query(f"SELECT count() FROM {name}").strip() == "0"
        assert "atomic partition import exceeds its state budget" in node.query_and_get_error(
            f"ALTER TABLE {name} ATTACH PARTITION 1"
        )
        assert node.query(f"SELECT count() FROM {name}").strip() == "0"
        assert node.query(
            f"SELECT count() FROM system.detached_parts WHERE database=currentDatabase() "
            f"AND table='{name}' AND name='{part_name}'"
        ).strip() == "1"
        node.restart_clickhouse(kill=True)
        assert node.query(f"SELECT count() FROM {name}").strip() == "0"
        assert node.query(
            f"SELECT count() FROM system.detached_parts WHERE database=currentDatabase() "
            f"AND table='{name}' AND name='{part_name}'"
        ).strip() == "1"
    finally:
        node.query(f"DROP TABLE IF EXISTS {name} SYNC")
