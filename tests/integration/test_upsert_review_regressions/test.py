import shlex
import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", stay_alive=True)
settings = {"allow_experimental_unique_key": 1, "async_insert": 0}


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_cleanup_preserves_ambiguous_commit_until_recovery():
    table = "upsert_fenced_cleanup"
    point = "upsert_log_throw_after_append"
    node.query(
        f"CREATE TABLE {table} (id UInt64, value UInt64) "
        "ENGINE=UpsertMergeTree ORDER BY id UNIQUE KEY id "
        "SETTINGS temporary_directories_lifetime=1, "
        "merge_tree_clear_old_temporary_directories_interval_seconds=0, "
        "cleanup_delay_period=1, cleanup_delay_period_random_add=0, max_cleanup_delay_period=2",
        settings=settings,
    )
    try:
        node.query(f"SYSTEM STOP CLEANUP {table}")
        node.query(f"INSERT INTO {table} VALUES (1,10),(2,20)", settings=settings)
        path = node.query(
            "SELECT data_paths[1] FROM system.tables "
            f"WHERE database=currentDatabase() AND name='{table}'"
        ).strip()
        node.query(f"SYSTEM ENABLE FAILPOINT {point}")
        retry = (
            f"INSERT INTO {table} SETTINGS insert_deduplication_token='cleanup-retry' "
            "VALUES (1,30),(3,40)"
        )
        assert "FAULT_INJECTED" in node.query_and_get_error(retry, settings=settings)
        node.query(f"SYSTEM DISABLE FAILPOINT {point}")
        assert "fenced" in node.query_and_get_error(f"SELECT * FROM {table}")

        # Age the payload and an ordinary orphan. Observing orphan removal proves cleanup ran.
        prefix = shlex.quote(path)
        payloads = node.exec_in_container(
            ["bash", "-ec", f'for p in {prefix}tmp_upsert_*; do test -d "$p"; printf "%s\\n" "$p"; done'],
            privileged=True,
        ).splitlines()
        assert len(payloads) == 1, payloads
        orphan = shlex.quote(path + "tmp_review_orphan")
        node.exec_in_container(
            ["bash", "-ec", f"mkdir -- {orphan}; touch -t 200001010000 -- {prefix}tmp_* {prefix}tmp_*/*"],
            privileged=True,
        )
        node.query(f"SYSTEM START CLEANUP {table}")
        deadline = time.monotonic() + 30
        while True:
            present = node.exec_in_container(
                ["bash", "-ec", f"if test -e {orphan}; then echo present; else echo gone; fi"],
                privileged=True,
            ).strip()
            if present == "gone":
                break
            assert time.monotonic() < deadline, "Temporary-directory cleanup did not run"
            time.sleep(0.1)
        for payload in payloads:
            node.exec_in_container(["test", "-d", payload], privileged=True)

        node.restart_clickhouse(kill=True)
        assert node.query(f"SELECT * FROM {table} ORDER BY id") == "1\t30\n2\t20\n3\t40\n"
        node.query(f"INSERT INTO {table} VALUES (1,50)", settings=settings)
        node.query(retry, settings=settings)
        assert node.query(f"SELECT * FROM {table} ORDER BY id") == "1\t50\n2\t20\n3\t40\n"
    finally:
        node.query(f"SYSTEM DISABLE FAILPOINT {point}")
        node.query(f"DROP TABLE IF EXISTS {table} SYNC")


def test_rename_keeps_the_state_database_path_valid():
    ordinary = "upsert_rename_ordinary"
    atomic = "upsert_rename_atomic"
    node.query(
        f"CREATE DATABASE {ordinary} ENGINE=Ordinary",
        settings={"allow_deprecated_database_ordinary": 1},
    )
    node.query(f"CREATE DATABASE {atomic} ENGINE=Atomic")
    try:
        for database in (ordinary, atomic):
            node.query(
                f"CREATE TABLE {database}.before (id UInt64, v UInt64) "
                "ENGINE=UpsertMergeTree ORDER BY id UNIQUE KEY id",
                settings=settings,
            )
            node.query(f"INSERT INTO {database}.before VALUES (1,10)", settings=settings)

        assert "SUPPORT_IS_DISABLED" in node.query_and_get_error(
            f"RENAME TABLE {ordinary}.before TO {ordinary}.after"
        )
        node.query(f"INSERT INTO {ordinary}.before VALUES (1,20),(2,30)", settings=settings)
        node.query(f"RENAME TABLE {atomic}.before TO {atomic}.after")
        node.query(f"INSERT INTO {atomic}.after VALUES (1,20),(2,30)", settings=settings)
        for table in (f"{ordinary}.before", f"{atomic}.after"):
            assert node.query(f"SELECT * FROM {table} ORDER BY id") == "1\t20\n2\t30\n"
            node.query(f"DETACH TABLE {table} SYNC")
            node.query(f"ATTACH TABLE {table}")
            assert node.query(f"SELECT * FROM {table} ORDER BY id") == "1\t20\n2\t30\n"
    finally:
        node.query(f"DROP DATABASE IF EXISTS {ordinary} SYNC")
        node.query(f"DROP DATABASE IF EXISTS {atomic} SYNC")


def test_maintenance_bounds_state_and_reclaims_dead_sources():
    table = "upsert_maintenance_state_budget"
    insert_settings = {
        **settings,
        "max_threads": 1,
        "max_insert_threads": 1,
        "max_block_size": 1048576,
        "max_insert_block_size": 1048576,
        "min_insert_block_size_rows": 0,
        "min_insert_block_size_bytes": 0,
    }
    node.query(
        f"CREATE TABLE {table} (a UInt64,b UInt32,p String,value UInt8) "
        "ENGINE=UpsertMergeTree PARTITION BY p ORDER BY (a,b) UNIQUE KEY (a,b)",
        settings=settings,
    )

    def wait_parts(count):
        deadline = time.monotonic() + 60
        while True:
            actual = node.query(
                "SELECT count() FROM system.parts "
                f"WHERE database=currentDatabase() AND table='{table}' AND active"
            ).strip()
            if actual == str(count):
                return
            assert time.monotonic() < deadline, (actual, count)
            time.sleep(0.1)

    try:
        for value in (1, 2):
            node.query(
                f"INSERT INTO {table} SELECT number,1,'a',{value} FROM numbers(1048576)",
                settings=insert_settings,
                timeout=120,
            )
        node.query(f"INSERT INTO {table} VALUES (1048576,1,'a',2)", settings=settings)
        node.query(f"INSERT INTO {table} VALUES (1048577,1,'a',2)", settings=settings)
        # Small sources still consolidate after large healthy parts are treated as complete.
        wait_parts(3)
        assert node.query(f"SELECT count(),uniqExact((a,b)),min(value) FROM {table}") == "1048578\t1048578\t2\n"
        node.query(
            f"INSERT INTO {table} SELECT number,1,'a',3 FROM numbers(524288)",
            settings=insert_settings,
            timeout=120,
        )
        # The conservative budget must still permit reclamation of fully dead large sources.
        wait_parts(3)
        assert node.query(f"SELECT count(),countIf(value=3) FROM {table}") == "1048578\t524288\n"
        assert not node.contains_in_log("UpsertMergeTree maintenance failed.*pending state batch")
    finally:
        node.query(f"DROP TABLE IF EXISTS {table} SYNC")
