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


@pytest.fixture
def table():
    name = "upsert_memory_accounting"
    node.query(f"""
        CREATE TABLE {name} (id UInt64, v UInt64, payload String)
        ENGINE = UpsertMergeTree(v) ORDER BY id UNIQUE KEY id
        SETTINGS upsert_index_cache_bytes = 8388608,
            upsert_write_buffer_bytes = 1048576,
            upsert_write_buffer_count = 2, upsert_wal_bytes = 2097152,
            upsert_background_jobs = 1, upsert_recovery_threads = 1
    """)
    try:
        yield name
    finally:
        node.query(f"DROP TABLE {name} SYNC")


def test_long_insert_does_not_accumulate_memtable_memory(table):
    rows = 1000000
    node.query(
        f"""
        INSERT INTO {table}
        SELECT number, 1, repeat('x', 64) FROM numbers({rows})
        """,
        settings={
            "max_memory_usage": 134217728,
            "max_threads": 2,
            "max_insert_threads": 1,
            "max_block_size": 8192,
            "max_insert_block_size": 8192,
            "min_insert_block_size_rows": 8192,
            "min_insert_block_size_bytes": 0,
        },
        timeout=300,
    )
    expected = f"{rows}\t{rows * (rows - 1) // 2}\t{rows}\t{rows * 64}"
    sql = f"SELECT count(), sum(id), sum(v), sum(length(payload)) FROM {table}"
    assert node.query(sql).strip() == expected
    node.restart_clickhouse()
    assert node.query(sql).strip() == expected


def test_insert_temporary_memory_limit_is_enforced(table):
    error = node.query_and_get_error(
        f"""
        INSERT INTO {table}
        SELECT number, 1, repeat('x', 1024) FROM numbers(65536)
        """,
        settings={
            "max_memory_usage": 4194304,
            "max_threads": 1,
            "max_insert_threads": 1,
            "max_block_size": 65536,
            "max_insert_block_size": 65536,
            "min_insert_block_size_rows": 65536,
            "min_insert_block_size_bytes": 0,
        },
    )
    assert "MEMORY_LIMIT_EXCEEDED" in error
    node.query(f"INSERT INTO {table} VALUES (9999999, 2, 'retry')")
    assert node.query(f"SELECT v, payload FROM {table} WHERE id = 9999999").strip() == "2\tretry"
