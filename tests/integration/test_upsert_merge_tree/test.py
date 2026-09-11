import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", stay_alive=True)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.mark.parametrize(
    "point",
    [
        "upsert_log_throw_after_append",
        "upsert_log_throw_after_sync",
        "upsert_throw_after_rename",
        "upsert_throw_before_publish",
    ],
)
def test_durable_commit_recovers_after_process_kill(point):
    table = f"recovery_{point}"
    settings = {"allow_experimental_unique_key": 1, "async_insert": 0}
    node.query(
        f"CREATE TABLE {table} (id UInt64, value UInt64) "
        "ENGINE=UpsertMergeTree ORDER BY value UNIQUE KEY id",
        settings=settings,
    )
    node.query(f"INSERT INTO {table} VALUES (1,10),(2,20)", settings=settings)
    node.query(f"SYSTEM ENABLE FAILPOINT {point}")
    retry = (
        f"INSERT INTO {table} SETTINGS insert_deduplication_token='fault-unit' "
        "VALUES (1,30),(3,40)"
    )
    assert "FAULT_INJECTED" in node.query_and_get_error(retry, settings=settings)
    assert "fenced" in node.query_and_get_error(f"SELECT * FROM {table}")
    assert "fenced" in node.query_and_get_error(
        f"INSERT INTO {table} VALUES (4,50)", settings=settings
    )

    node.restart_clickhouse(kill=True)
    assert node.query(f"SELECT * FROM {table} ORDER BY id") == "1\t30\n2\t20\n3\t40\n"
    node.query(f"INSERT INTO {table} VALUES (1,60)", settings=settings)
    node.query(retry, settings=settings)
    expected = "1\t60\n2\t20\n3\t40\n"
    assert node.query(f"SELECT * FROM {table} ORDER BY id") == expected

    node.restart_clickhouse(kill=True)
    assert node.query(f"SELECT * FROM {table} ORDER BY id") == expected
    assert node.query(f"SELECT count(),uniqExact(id) FROM {table}") == "3\t3\n"
    node.query(f"DROP TABLE {table} SYNC")
