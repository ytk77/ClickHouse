import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", stay_alive=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_persisted_source_and_view_restart(started_cluster):
    node.query("CREATE DATABASE mv_startup")
    try:
        node.query(
            "CREATE TABLE mv_startup.cache (id UInt64, ver UInt64, value String) "
            "ENGINE=OverwriteCache(ver) KEYS(id) SETTINGS persist_mode='sync'"
        )
        node.query(
            "CREATE MATERIALIZED VIEW mv_startup.mv ENGINE=ReplacingMergeTree(ver) "
            "ORDER BY id AS SELECT * FROM mv_startup.cache"
        )
        node.query("INSERT INTO mv_startup.cache VALUES (1,1,'old'),(2,1,'deleted')")
        node.query("INSERT INTO mv_startup.cache VALUES (1,2,'new')")
        node.query("DELETE FROM mv_startup.cache WHERE id=2")
        for _ in range(2):
            node.restart_clickhouse()
            assert node.query(
                "SELECT * FROM mv_startup.cache WHERE id IN (1,2) ORDER BY id"
            ) == "1\t2\tnew\n"
            assert node.query("SELECT * FROM mv_startup.mv FINAL ORDER BY id") == (
                "1\t2\tnew\n2\t1\tdeleted\n"
            )
        node.query("DETACH TABLE mv_startup.mv SYNC")
        node.query("ATTACH TABLE mv_startup.mv")
        node.query("INSERT INTO mv_startup.cache VALUES (3,1,'after restart')")
        assert node.query("SELECT value FROM mv_startup.mv WHERE id=3") == "after restart\n"

        node.query(
            "CREATE TABLE mv_startup.upsert (id UInt64, ver UInt64, value String) "
            "ENGINE=UpsertMergeTree(ver) ORDER BY id UNIQUE KEY id",
            settings={"allow_experimental_unique_key": 1},
        )
        node.query("CREATE TABLE mv_startup.target (id UInt64, ver UInt64, value String) ENGINE=Memory")
        error = node.query_and_get_error(
            "CREATE MATERIALIZED VIEW mv_startup.unsupported TO mv_startup.target "
            "AS SELECT * FROM mv_startup.upsert"
        )
        assert "QUERY_IS_NOT_SUPPORTED_IN_MATERIALIZED_VIEW" in error, error
    finally:
        node.query("DROP DATABASE mv_startup SYNC")
