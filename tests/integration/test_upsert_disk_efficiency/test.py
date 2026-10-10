import time
from concurrent.futures import ThreadPoolExecutor

import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node", stay_alive=True, main_configs=["configs/storage.xml"],
    tmpfs=["/upsert_disk_efficiency:size=3G"],
)
SETTINGS = {"async_insert": 0}


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def query(sql):
    return node.query(sql, settings=SETTINGS)


def poll(predicate, timeout=90):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.1)
    raise AssertionError("Condition did not become true before deadline")


def create(name, extra="", compact=False):
    query(f"CREATE TABLE {name} (p UInt8,id UInt64,v UInt64) "
          "ENGINE=UpsertMergeTree(v) PARTITION BY p ORDER BY id UNIQUE KEY id "
          "SETTINGS upsert_lineage_compression=1, upsert_state_compression=1, "
          f"upsert_lineage_compact={int(compact)}" + extra)
    query(f"SYSTEM STOP MERGES {name}")


def test_mixed_lineage_chunked_output_mutation_and_partition_transfer():
    names = ["disk_mixed", "disk_raw_import", "disk_v3_import", "disk_parallel"]
    try:
        create(names[0], ", upsert_lineage_compression_level=6")
        query("ALTER TABLE disk_mixed MODIFY SETTING upsert_lineage_compression=0")
        query("INSERT INTO disk_mixed SELECT 1,number,1 FROM numbers(600000)")
        query("ALTER TABLE disk_mixed MODIFY SETTING upsert_lineage_compression=1")
        query("INSERT INTO disk_mixed SELECT 1,number+600000,1 FROM numbers(600000)")
        query("SYSTEM START MERGES disk_mixed")
        poll(lambda: query("SELECT count(),max(rows) FROM system.parts "
                           "WHERE database=currentDatabase() AND table='disk_mixed' AND active") == "1\t1200000\n")
        assert query("SELECT count(),uniqExact(id),sum(v) FROM disk_mixed") == "1200000\t1200000\t1200000\n"
        node.restart_clickhouse(kill=True)
        assert query("SELECT count(),uniqExact(id),sum(v) FROM disk_mixed") == "1200000\t1200000\t1200000\n"
        query("ALTER TABLE disk_mixed DELETE WHERE id=42")
        assert query("SELECT count(),uniqExact(id),sum(v) FROM disk_mixed") == "1199999\t1199999\t1199999\n"
        create(names[1])
        query("ALTER TABLE disk_raw_import MODIFY SETTING upsert_lineage_compression=0")
        query("ALTER TABLE disk_raw_import ATTACH PARTITION 1 FROM disk_mixed")
        assert query("SELECT count(),uniqExact(id),sum(v) FROM disk_raw_import") == "1199999\t1199999\t1199999\n"
        create(names[2])
        query("ALTER TABLE disk_v3_import REPLACE PARTITION 1 FROM disk_raw_import")
        assert query("SELECT count(),uniqExact(id),sum(v) FROM disk_v3_import") == "1199999\t1199999\t1199999\n"
        create(names[3])
        with ThreadPoolExecutor(max_workers=16) as pool:
            list(pool.map(lambda i: query(f"INSERT INTO disk_parallel SELECT 1,number+{i*4096},{i+1} FROM numbers(4096)"), range(16)))
        assert query("SELECT count(),uniqExact(id),sum(v) FROM disk_parallel") == "65536\t65536\t557056\n"
        node.restart_clickhouse(kill=True)
        assert query("SELECT count(),uniqExact(id),sum(v) FROM disk_parallel") == "65536\t65536\t557056\n"
    finally:
        for name in names:
            query(f"DROP TABLE IF EXISTS {name} SYNC")


@pytest.mark.parametrize("compact", [False, True])
def test_v3_ambiguous_insert_recovers_and_retry_keeps_winners(compact):
    name = "disk_recovery"
    try:
        create(name, ", upsert_lineage_compression_level=6", compact=compact)
        query(f"INSERT INTO {name} VALUES (1,1,1)")
        query("SYSTEM ENABLE FAILPOINT upsert_log_throw_after_sync")
        retry = f"INSERT INTO {name} SETTINGS insert_deduplication_token='disk-v3-retry' VALUES (1,1,2),(1,2,2)"
        assert "FAULT_INJECTED" in node.query_and_get_error(retry, settings=SETTINGS)
        assert "fenced" in node.query_and_get_error(f"SELECT * FROM {name}")
        node.restart_clickhouse(kill=True)
        query(retry)
        assert query(f"SELECT count(),uniqExact(id),sum(v) FROM {name}") == "2\t2\t4\n"
    finally:
        query("SYSTEM DISABLE FAILPOINT upsert_log_throw_after_sync")
        query(f"DROP TABLE IF EXISTS {name} SYNC")


def test_disk_backoff_preflight_and_removal_below_insert_floor():
    names = ["space_merge", "space_remove", "space_preflight"]
    filler = "/upsert_disk_efficiency/space_gate_owned_filler"

    def fill_to(desired):
        node.exec_in_container(["python3", "-c",
            "import os; p=" + repr(filler) + "; "
            "os.path.exists(p) and os.unlink(p); "
            "s=os.statvfs('/upsert_disk_efficiency'); n=s.f_bavail*s.f_frsize-" + str(desired) + "; "
            "assert n>0; f=os.open(p,os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600); "
            "os.posix_fallocate(f,0,n); os.fsync(f); os.close(f)"])

    try:
        for name in names:
            create(name, ", storage_policy='limited'")
            for i in range(2):
                query(f"INSERT INTO {name} SELECT 1,number+{i*1000},1 FROM numbers(1000)")
        before = int(query("SELECT value FROM system.events WHERE event='UpsertMaintenanceDiskBackoffs'") or 0)
        fill_to(900 << 20)
        assert "NOT_ENOUGH_SPACE" in node.query_and_get_error("INSERT INTO space_preflight VALUES (1,99999,2)", settings=SETTINGS)
        query("SYSTEM START MERGES space_merge")
        poll(lambda: int(query("SELECT value FROM system.events WHERE event='UpsertMaintenanceDiskBackoffs'") or 0) > before)
        assert query("SELECT count(),uniqExact(id),sum(v) FROM space_merge") == "2000\t2000\t2000\n"
        query("ALTER TABLE space_remove DROP PARTITION 1")
        query("TRUNCATE TABLE space_preflight")
        assert query("SELECT count() FROM space_remove") == "0\n"
        assert query("SELECT count() FROM space_preflight") == "0\n"
        # This narrow schema passes the advisory floor and native column reservation;
        # the 20,000-row state batch then exceeds the remaining preflight slack.
        fill_to((1 << 30) + (128 << 10))
        error = node.query_and_get_error("INSERT INTO space_preflight SELECT 1,number,2 FROM numbers(20000)", settings=SETTINGS)
        assert "NOT_ENOUGH_SPACE" in error and "state write needs space" in error
        assert query("SELECT count() FROM space_preflight") == "0\n"
        node.exec_in_container(["rm", "-f", filler])
        poll(lambda: query("SELECT count() FROM system.parts WHERE database=currentDatabase() AND table='space_merge' AND active") == "1\n")
        query("INSERT INTO space_preflight VALUES (1,1,2)")
        assert query("SELECT count(),sum(v) FROM space_preflight") == "1\t2\n"
    finally:
        node.exec_in_container(["rm", "-f", filler])
        for name in names:
            query(f"DROP TABLE IF EXISTS {name} SYNC")



def test_rewrite_codec_matches_native_size_policy_and_explicit_override():
    names = ["codec_upsert", "codec_native", "codec_import", "codec_explicit"]

    def codecs(name):
        return query(f"SELECT DISTINCT default_compression_codec FROM system.parts "
                     f"WHERE database=currentDatabase() AND table='{name}' AND active ORDER BY 1")

    def contents(name):
        return query(f"SELECT count(),uniqExact(id),sum(cityHash64(id,v,payload)) FROM {name}")

    try:
        for name in names:
            engine = "MergeTree" if name == "codec_native" else "UpsertMergeTree(v)"
            key = "" if name == "codec_native" else " UNIQUE KEY id"
            settings = "" if name == "codec_native" else " SETTINGS upsert_lineage_compression=1"
            if name == "codec_explicit":
                settings += ", default_compression_codec='LZ4'"
            query(f"CREATE TABLE {name} (p UInt8,id UInt64,v UInt64,payload String) "
                  f"ENGINE={engine} PARTITION BY p ORDER BY id{key}{settings}")
            query(f"SYSTEM STOP MERGES {name}")
        for name in [names[0], names[1], names[3]]:
            for start in [0, 8192]:
                query(f"INSERT INTO {name} SELECT 1,number+{start},1,"
                      f"hex(cityHash64(number+{start})) FROM numbers(8192)")
            assert codecs(name) == "LZ4\n"
        expected = contents(names[0])
        assert contents(names[1]) == expected and contents(names[3]) == expected
        for name in [names[0], names[1], names[3]]:
            query(f"SYSTEM START MERGES {name}")
        query("OPTIMIZE TABLE codec_native FINAL")
        for name in [names[0], names[3]]:
            poll(lambda name=name: query(f"SELECT count() FROM system.parts WHERE database=currentDatabase() "
                                         f"AND table='{name}' AND active") == "1\n")
        assert codecs(names[0]) == "ZSTD(1)\n"
        assert codecs(names[1]) == "ZSTD(1)\n"
        assert codecs(names[3]) == "LZ4\n"
        assert contents(names[0]) == expected and contents(names[1]) == expected
        query("ALTER TABLE codec_upsert DELETE WHERE id=0")
        assert codecs(names[0]) == "ZSTD(1)\n"
        expected_after_delete = contents(names[0])
        assert expected_after_delete.startswith("16383\t16383\t")
        query("ALTER TABLE codec_import ATTACH PARTITION 1 FROM codec_upsert")
        assert codecs(names[2]) == "ZSTD(1)\n"
        assert contents(names[2]) == expected_after_delete
    finally:
        for name in names:
            query(f"DROP TABLE IF EXISTS {name} SYNC")


@pytest.mark.parametrize("compact", [False, True])
@pytest.mark.parametrize("point", ["upsert_log_throw_after_sync", "upsert_throw_after_rename"])
def test_v3_ttl_rewrite_recovers_after_ambiguous_durable_failure(point, compact):
    name = "disk_ttl_recovery"
    try:
        query(f"CREATE TABLE {name} (id UInt64,v UInt64,expires DateTime) "
              "ENGINE=UpsertMergeTree(v) ORDER BY id UNIQUE KEY id TTL expires "
              f"SETTINGS upsert_lineage_compression=1,upsert_lineage_compression_level=6,upsert_state_compression=1,upsert_lineage_compact={int(compact)}")
        query(f"SYSTEM STOP TTL MERGES {name}")
        query(f"INSERT INTO {name} VALUES (1,10,'2000-01-01'),(2,10,'2100-01-01')")
        query("SYSTEM ENABLE FAILPOINT upsert_pause_before_merge_commit")
        query(f"SYSTEM START TTL MERGES {name}")
        query("SYSTEM WAIT FAILPOINT upsert_pause_before_merge_commit PAUSE")
        query(f"SYSTEM ENABLE FAILPOINT {point}")
        query("SYSTEM DISABLE FAILPOINT upsert_pause_before_merge_commit")

        def fenced():
            try:
                query(f"SELECT count() FROM {name}")
            except Exception as error:
                assert "fenced" in str(error)
                return True
            return False

        poll(fenced)
        node.restart_clickhouse(kill=True)
        assert query(f"SELECT id,v FROM {name} ORDER BY id") == "2\t10\n"
        part_path = query(f"SELECT path FROM system.parts WHERE database=currentDatabase() "
                          f"AND table='{name}' AND active").strip()
        node.exec_in_container(["python3", "-c", "from pathlib import Path; "
            "assert (Path(" + repr(part_path) + ")/'upsert_rows.bin').read_bytes()[:8]==b'UMPROW03'"])
        query(f"INSERT INTO {name} SETTINGS insert_deduplication_token='ttl-v3-retry' "
              "VALUES (1,1,'2100-01-01')")
        query(f"INSERT INTO {name} SETTINGS insert_deduplication_token='ttl-v3-retry' "
              "VALUES (1,1,'2100-01-01')")
        assert query(f"SELECT id,v FROM {name} ORDER BY id") == "1\t1\n2\t10\n"
    finally:
        query("SYSTEM DISABLE FAILPOINT upsert_pause_before_merge_commit")
        query(f"SYSTEM DISABLE FAILPOINT {point}")
        query(f"DROP TABLE IF EXISTS {name} SYNC")



def test_compact_mixed_formats_restart_sweep_and_large_rewrite():
    names = ["compact_mixed", "compact_import", "compact_parallel"]
    try:
        create(names[0])
        query("ALTER TABLE compact_mixed MODIFY SETTING upsert_lineage_compression=0")
        query("INSERT INTO compact_mixed SELECT 1,number,1 FROM numbers(400000)")
        query("ALTER TABLE compact_mixed MODIFY SETTING upsert_lineage_compression=1")
        query("INSERT INTO compact_mixed SELECT 1,number+400000,1 FROM numbers(400000)")
        query("ALTER TABLE compact_mixed MODIFY SETTING upsert_lineage_compact=1")
        query("INSERT INTO compact_mixed SELECT 1,number+800000,1 FROM numbers(400000)")
        paths = query("SELECT path FROM system.parts WHERE database=currentDatabase() "
                      "AND table='compact_mixed' AND active").splitlines()
        assert len(paths) == 3
        node.exec_in_container(["python3", "-c", "from pathlib import Path; "
            "headers=[(Path(p)/'upsert_rows.bin').read_bytes()[:32] for p in " + repr(paths) + "]; "
            "assert sorted((h[:8],h[10] if h[:8]==b'UMPROW03' else -1) for h in headers) "
            "==[(b'UMPROW01',-1),(b'UMPROW03',0),(b'UMPROW03',1)]"])
        # Verify all three immutable sources before restart; maintenance then reloads their planner headers.
        poll(lambda: int(query("SELECT value FROM system.events WHERE event='UpsertPayloadVerificationParts'") or 0) >= 3)
        node.restart_clickhouse(kill=True)
        assert query("SELECT count(),uniqExact(id),sum(v) FROM compact_mixed") == "1200000\t1200000\t1200000\n"
        poll(lambda: int(query("SELECT value FROM system.events WHERE event='UpsertPayloadVerificationParts'") or 0) >= 1)
        query("SYSTEM START MERGES compact_mixed")
        poll(lambda: query("SELECT count(),max(rows) FROM system.parts WHERE database=currentDatabase() "
                           "AND table='compact_mixed' AND active") == "1\t1200000\n")
        query("ALTER TABLE compact_mixed DELETE WHERE id=42")
        assert query("SELECT count(),uniqExact(id),sum(v) FROM compact_mixed") == "1199999\t1199999\t1199999\n"
        create(names[1])
        query("ALTER TABLE compact_import ATTACH PARTITION 1 FROM compact_mixed")
        assert query("SELECT count(),sum(v) FROM compact_import") == "1199999\t1199999\n"
        query("ALTER TABLE compact_import MODIFY SETTING upsert_lineage_compact=1")
        query("ALTER TABLE compact_import REPLACE PARTITION 1 FROM compact_mixed")
        assert query("SELECT count(),sum(v) FROM compact_import") == "1199999\t1199999\n"
        create(names[2], compact=True)
        with ThreadPoolExecutor(max_workers=16) as pool:
            list(pool.map(lambda i: query(f"INSERT INTO compact_parallel SELECT 1,number+{i*4096},1 FROM numbers(4096)"), range(16)))
        node.restart_clickhouse(kill=True)
        assert query("SELECT count(),uniqExact(id),sum(v) FROM compact_parallel") == "65536\t65536\t65536\n"
    finally:
        for name in names:
            query(f"DROP TABLE IF EXISTS {name} SYNC")
