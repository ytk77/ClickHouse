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
            settings = ""
            if name == "codec_explicit":
                settings = " SETTINGS default_compression_codec='LZ4'"
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
