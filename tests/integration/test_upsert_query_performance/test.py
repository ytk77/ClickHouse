import time
from concurrent.futures import ThreadPoolExecutor

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


def event(name):
    return int(node.query(f"SELECT sum(value) FROM system.events WHERE event='{name}'"))


def wait_for(sql, predicate, timeout=300):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = node.query(sql).strip()
        if predicate(result):
            return result
        time.sleep(0.1)
    raise AssertionError(f"{sql}: last result {result}")


def create(name, extra=""):
    node.query(f"DROP TABLE IF EXISTS {name} SYNC")
    node.query(
        f"""CREATE TABLE {name} (id UInt64, v UInt64, affiliate Nullable(String), uid String, pad String)
        ENGINE=UpsertMergeTree(v) ORDER BY id UNIQUE KEY id
        SETTINGS min_bytes_for_wide_part=0, min_rows_for_wide_part=0 {extra}"""
    )


def test_background_rewrite_evaluates_defaults_and_conditional_ttl():
    for wide_part_threshold in (0, 10000000):
        name = "query_rewrite_context"
        node.query(
            f"""CREATE TABLE {name} (id UInt64, expires DateTime, category UInt8)
            ENGINE=UpsertMergeTree ORDER BY id UNIQUE KEY id
            TTL expires DELETE WHERE category=1
            SETTINGS min_bytes_for_wide_part={wide_part_threshold}, min_rows_for_wide_part=0"""
        )
        try:
            node.query(f"SYSTEM STOP MERGES {name}")
            node.query(
                f"INSERT INTO {name} VALUES (1,1,1),(2,1,2)",
                settings={"log_queries": 1},
            )
            node.query(
                f"INSERT INTO {name} VALUES (3,4102444800,1),(4,1,2)",
                settings={"log_queries": 1},
            )
            node.query(f"""ALTER TABLE {name}
                ADD COLUMN s Nullable(String) DEFAULT if(id%2=0,NULL,toString(id+1)),
                ADD COLUMN m UInt64 MATERIALIZED id*10""")
            node.query(f"SYSTEM START MERGES {name}")
            wait_for(f"SELECT count() FROM {name}", lambda x: x == "3")
            wait_for(
                f"SELECT count() FROM system.parts WHERE active AND database=currentDatabase() AND table='{name}'",
                lambda x: x == "1",
            )
            assert node.query(f"SELECT id,s,m FROM {name} ORDER BY id") == (
                "2\t\\N\t20\n3\t4\t30\n4\t\\N\t40\n"
            )
        finally:
            node.query(f"DROP TABLE {name} SYNC")


def test_target_settings_persist_and_reject_invalid_values():
    assert (
        node.query(
            "SELECT count() FROM system.settings_changes ARRAY JOIN changes AS c "
            "WHERE type='MergeTree' AND c.name='upsert_maintenance_target_bytes' "
            "AND c.new_value='268435456'"
        ).strip()
        == "1"
    )
    create("query_settings")
    node.query(
        "ALTER TABLE query_settings MODIFY SETTING upsert_maintenance_target_bytes=134217728, prewarm_mark_cache=1"
    )
    node.query("INSERT INTO query_settings VALUES (1,1,'a','u','p')")
    before = node.query("SHOW CREATE TABLE query_settings")
    assert "upsert_maintenance_target_bytes = 134217728" in before
    for value in (0, 33554431, 268435457):
        error = node.query_and_get_error(
            f"ALTER TABLE query_settings MODIFY SETTING upsert_maintenance_target_bytes={value}"
        )
        assert "upsert_maintenance_target_bytes" in error
        assert "33554432" in error and "268435456" in error
        assert node.query("SHOW CREATE TABLE query_settings") == before
    node.restart_clickhouse()
    assert node.query("SHOW CREATE TABLE query_settings") == before
    assert node.query("SELECT count() FROM query_settings").strip() == "1"
    node.query("DROP TABLE query_settings SYNC")
    for value in (0, 268435457):
        error = node.query_and_get_error(
            f"""CREATE TABLE query_bad_target (id UInt64)
            ENGINE=UpsertMergeTree ORDER BY id UNIQUE KEY id SETTINGS upsert_maintenance_target_bytes={value}"""
        )
        assert "upsert_maintenance_target_bytes" in error


def test_prewarm_insert_import_and_failure_keeps_commit():
    create(
        "query_warm",
        ", prewarm_mark_cache=1, columns_to_prewarm_mark_cache='affiliate,uid'",
    )
    node.query("SYSTEM STOP MERGES query_warm")
    before = event("UpsertPartCachePrewarms")
    node.query(
        "INSERT INTO query_warm SELECT number,1,toString(number%10),toString(number),repeat('x',64) FROM numbers(10000)"
    )
    assert event("UpsertPartCachePrewarms") > before
    expected = node.query(
        "SELECT affiliate,count(),sum(cityHash64(uid)) FROM query_warm GROUP BY affiliate ORDER BY affiliate",
        query_id="query_prewarm_insert",
    )
    node.query("SYSTEM FLUSH LOGS")
    assert (
        node.query(
            "SELECT ProfileEvents['MarkCacheMisses'] FROM system.query_log WHERE type='QueryFinish' AND query_id='query_prewarm_insert' ORDER BY event_time_microseconds DESC LIMIT 1"
        ).strip()
        == "0"
    )
    create("query_import", ", prewarm_mark_cache=1")
    node.query("SYSTEM STOP MERGES query_import")
    before = event("UpsertPartCachePrewarms")
    node.query("ALTER TABLE query_import ATTACH PARTITION ID 'all' FROM query_warm")
    assert event("UpsertPartCachePrewarms") > before
    assert (
        node.query(
            "SELECT affiliate,count(),sum(cityHash64(uid)) FROM query_import GROUP BY affiliate ORDER BY affiliate"
        )
        == expected
    )
    failures = event("UpsertPartCachePrewarmFailures")
    node.query("SYSTEM ENABLE FAILPOINT upsert_throw_during_cache_prewarm")
    try:
        node.query("INSERT INTO query_warm VALUES (10000,2,'b','new','p')")
    finally:
        node.query("SYSTEM DISABLE FAILPOINT upsert_throw_during_cache_prewarm")
    assert event("UpsertPartCachePrewarmFailures") == failures + 1
    assert (
        node.query("SELECT count(),max(id) FROM query_warm").strip() == "10001\t10000"
    )
    node.restart_clickhouse(kill=True)
    assert (
        node.query("SELECT count(),max(id) FROM query_warm").strip() == "10001\t10000"
    )
    node.query("DROP TABLE query_warm SYNC")
    node.query("DROP TABLE query_import SYNC")


def test_larger_parts_concurrent_overwrite_delete_and_recovery():
    create(
        "query_large",
        ", upsert_maintenance_target_bytes=134217728, prewarm_mark_cache=1",
    )
    node.query("SYSTEM STOP MERGES query_large")
    # Each original part exceeds the old healthy threshold. The new target must combine them.
    for i in range(8):
        node.query(
            f"INSERT INTO query_large SELECT number+{i*50000},1,toString(number%100),toString(number+{i*50000}),repeat('x',500) FROM numbers(50000)"
        )
    initial_parts = int(
        node.query(
            "SELECT count() FROM system.parts WHERE active AND database=currentDatabase() AND table='query_large'"
        )
    )
    prewarms = event("UpsertPartCachePrewarms")
    node.query("SYSTEM ENABLE FAILPOINT upsert_pause_before_merge_commit")
    node.query("SYSTEM START MERGES query_large")
    try:
        node.query("SYSTEM WAIT FAILPOINT upsert_pause_before_merge_commit PAUSE")
        with ThreadPoolExecutor(max_workers=4) as pool:
            list(
                pool.map(
                    lambda i: node.query(
                        f"INSERT INTO query_large SELECT number*4+{i},2,'new',toString(number*4+{i}),repeat('y',500) FROM numbers(2500)"
                    ),
                    range(4),
                )
            )
    finally:
        node.query("SYSTEM DISABLE FAILPOINT upsert_pause_before_merge_commit")
    wait_for(
        "SELECT max(data_uncompressed_bytes) FROM system.parts WHERE active AND database=currentDatabase() AND table='query_large'",
        lambda x: int(x) > 33554432,
    )
    wait_for(
        "SELECT count() FROM system.parts WHERE active AND database=currentDatabase() AND table='query_large'",
        lambda x: int(x) < initial_parts,
    )
    assert event("UpsertPartCachePrewarms") > prewarms
    assert (
        node.query("SELECT count(),sum(v),uniqExact(id) FROM query_large").strip()
        == "400000\t410000\t400000"
    )
    node.query("DELETE FROM query_large WHERE id%10=0")
    expected = node.query(
        "SELECT affiliate,count(),sum(cityHash64(uid)),sum(v) FROM query_large GROUP BY affiliate ORDER BY affiliate"
    )
    assert (
        node.query("SELECT count(),sum(v) FROM query_large").strip() == "360000\t369000"
    )
    node.restart_clickhouse(kill=True)
    assert (
        node.query(
            "SELECT affiliate,count(),sum(cityHash64(uid)),sum(v) FROM query_large GROUP BY affiliate ORDER BY affiliate"
        )
        == expected
    )
    node.query("DROP TABLE query_large SYNC")


def test_settings_failures_before_and_after_metadata():
    create("query_setting_fault")
    node.query("INSERT INTO query_setting_fault VALUES (1,1,'a','u','p')")
    before = node.query("SHOW CREATE TABLE query_setting_fault")
    node.query("SYSTEM ENABLE FAILPOINT upsert_settings_throw_before_metadata")
    try:
        assert "FAULT_INJECTED" in node.query_and_get_error(
            "ALTER TABLE query_setting_fault MODIFY SETTING upsert_maintenance_target_bytes=134217728"
        )
    finally:
        node.query("SYSTEM DISABLE FAILPOINT upsert_settings_throw_before_metadata")
    assert node.query("SHOW CREATE TABLE query_setting_fault") == before
    node.query("INSERT INTO query_setting_fault VALUES (2,1,'a','u','p')")
    node.query("SYSTEM ENABLE FAILPOINT upsert_settings_throw_after_metadata")
    try:
        assert "FAULT_INJECTED" in node.query_and_get_error(
            "ALTER TABLE query_setting_fault MODIFY SETTING upsert_maintenance_target_bytes=134217728"
        )
    finally:
        node.query("SYSTEM DISABLE FAILPOINT upsert_settings_throw_after_metadata")
    assert "fenced" in node.query_and_get_error(
        "SELECT count() FROM query_setting_fault"
    )
    node.restart_clickhouse(kill=True)
    assert "upsert_maintenance_target_bytes = 134217728" in node.query(
        "SHOW CREATE TABLE query_setting_fault"
    )
    assert node.query("SELECT count() FROM query_setting_fault").strip() == "2"
    node.query("DROP TABLE query_setting_fault SYNC")


def test_prewarm_column_selection():
    create(
        "query_warm_columns",
        ",prewarm_mark_cache=1,columns_to_prewarm_mark_cache='affiliate'",
    )
    node.query("SYSTEM STOP MERGES query_warm_columns")
    node.query(
        "INSERT INTO query_warm_columns SELECT number,1,toString(number%10),toString(number),repeat('x',64) FROM numbers(10000)"
    )
    node.query(
        "SELECT sum(cityHash64(affiliate)) FROM query_warm_columns",
        query_id="query_warm_affiliate",
    )
    node.query(
        "SELECT sum(cityHash64(uid)) FROM query_warm_columns", query_id="query_cold_uid"
    )
    node.query("SYSTEM FLUSH LOGS")
    assert (
        node.query(
            "SELECT ProfileEvents['MarkCacheMisses'] FROM system.query_log WHERE type='QueryFinish' AND query_id='query_warm_affiliate' ORDER BY event_time_microseconds DESC LIMIT 1"
        ).strip()
        == "0"
    )
    assert (
        int(
            node.query(
                "SELECT ProfileEvents['MarkCacheMisses'] FROM system.query_log WHERE type='QueryFinish' AND query_id='query_cold_uid' ORDER BY event_time_microseconds DESC LIMIT 1"
            )
        )
        > 0
    )
    node.query("DROP TABLE query_warm_columns SYNC")


def test_short_strings_compaction_converges():
    node.query("DROP TABLE IF EXISTS query_short_strings SYNC")
    node.query("""CREATE TABLE query_short_strings (id UInt64) ENGINE=UpsertMergeTree
        ORDER BY id UNIQUE KEY id SETTINGS upsert_maintenance_target_bytes=268435456""")
    node.query("SYSTEM STOP MERGES query_short_strings")
    for start in (0, 200000):
        node.query(
            f"INSERT INTO query_short_strings SELECT number FROM numbers({start},200000)"
        )
    columns = ",".join(
        f"ADD COLUMN s{i} Nullable(String) DEFAULT if(id%2=0,NULL,'')"
        for i in range(96)
    )
    node.query(f"ALTER TABLE query_short_strings {columns}")
    before = event("UpsertMaintenanceSaturatedSkips")
    node.query("SYSTEM START MERGES query_short_strings")
    wait_for(
        "SELECT sum(value) FROM system.events WHERE event='UpsertMaintenanceSaturatedSkips'",
        lambda x: int(x) > before,
        timeout=600,
    )

    def assert_converged():
        # All other upsert tables in this isolated module were dropped synchronously.
        previous = event("UpsertStateWrites")
        visited = event("UpsertMaintenanceCandidates")
        parts = int(
            node.query(
                "SELECT count() FROM system.parts WHERE active AND table='query_short_strings' AND database=currentDatabase()"
            )
        )
        deadline = time.monotonic() + 600
        while time.monotonic() < deadline:
            current = event("UpsertStateWrites")
            candidates = event("UpsertMaintenanceCandidates")
            if current != previous:
                previous, visited = current, candidates
            elif candidates >= visited + 3 * parts:
                return
            time.sleep(0.1)
        raise AssertionError(
            "Rewrite publications did not stop over three candidate scans"
        )

    assert_converged()
    assert (
        node.query(
            "SELECT count(),sum(id),countIf(isNull(s0)) FROM query_short_strings"
        ).strip()
        == "400000\t79999800000\t200000"
    )
    node.query("DETACH TABLE query_short_strings")
    node.query("ATTACH TABLE query_short_strings")
    assert_converged()
    assert (
        node.query(
            "SELECT count(),sum(id),countIf(isNull(s0)) FROM query_short_strings"
        ).strip()
        == "400000\t79999800000\t200000"
    )
    node.query("DROP TABLE query_short_strings SYNC")


def test_alter_waits_for_background_rewrite():
    create("query_alter_wait", ",upsert_maintenance_target_bytes=268435456")
    node.query("SYSTEM STOP MERGES query_alter_wait")
    for i in range(4):
        node.query(
            f"INSERT INTO query_alter_wait SELECT number+{i*50000},1,'a',toString(number),repeat('x',500) FROM numbers(50000)"
        )
    node.query("SYSTEM ENABLE FAILPOINT upsert_pause_before_merge_commit")
    node.query("SYSTEM START MERGES query_alter_wait")
    pool = ThreadPoolExecutor(max_workers=1)
    future = None
    try:
        node.query("SYSTEM WAIT FAILPOINT upsert_pause_before_merge_commit PAUSE")
        future = pool.submit(
            node.query,
            "ALTER TABLE query_alter_wait MODIFY SETTING upsert_maintenance_target_bytes=134217728",
            query_id="query_alter_waiting",
        )
        wait_for(
            "SELECT count() FROM system.processes WHERE query_id='query_alter_waiting'",
            lambda x: int(x) == 1,
        )
        assert not future.done()
        node.query("INSERT INTO query_alter_wait VALUES (200000,1,'a','u','p')")
    finally:
        node.query("SYSTEM DISABLE FAILPOINT upsert_pause_before_merge_commit")
        pool.shutdown(wait=True)
    if future is not None:
        future.result()
    assert (
        node.query("SELECT count(),uniqExact(id) FROM query_alter_wait").strip()
        == "200001\t200001"
    )
    assert "upsert_maintenance_target_bytes = 134217728" in node.query(
        "SHOW CREATE TABLE query_alter_wait"
    )
    node.query("DROP TABLE query_alter_wait SYNC")


def test_target_increase_revisits_budget_cut_parts():
    node.query("DROP TABLE IF EXISTS query_target_increase SYNC")
    node.query("""CREATE TABLE query_target_increase (id UInt64) ENGINE=UpsertMergeTree
        ORDER BY id UNIQUE KEY id SETTINGS upsert_maintenance_target_bytes=134217728""")
    node.query("SYSTEM STOP MERGES query_target_increase")
    for start in (0, 100000):
        node.query(
            f"INSERT INTO query_target_increase SELECT number FROM numbers({start},100000)"
        )
    columns = ",".join(
        f"ADD COLUMN s{i} Nullable(String) DEFAULT if(id%2=0,NULL,'')"
        for i in range(96)
    )
    node.query(f"ALTER TABLE query_target_increase {columns}")
    skipped = event("UpsertMaintenanceSaturatedSkips")
    node.query("SYSTEM START MERGES query_target_increase")
    wait_for(
        "SELECT sum(value) FROM system.events WHERE event='UpsertMaintenanceSaturatedSkips'",
        lambda x: int(x) > skipped,
        timeout=600,
    )
    node.query(
        "ALTER TABLE query_target_increase MODIFY SETTING upsert_maintenance_target_bytes=268435456"
    )
    wait_for(
        "SELECT count() FROM system.parts WHERE active AND table='query_target_increase' AND database=currentDatabase()",
        lambda x: int(x) == 1,
        timeout=600,
    )
    assert (
        node.query(
            "SELECT count(),sum(id),countIf(isNull(s0)) FROM query_target_increase"
        ).strip()
        == "200000\t19999900000\t100000"
    )
    node.query("DROP TABLE query_target_increase SYNC")
