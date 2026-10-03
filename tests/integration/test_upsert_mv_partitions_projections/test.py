import json
import time
from concurrent.futures import ThreadPoolExecutor

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


def test_mv_token_retry():
    for dedup_views in (0, 1):
        node.query(
            "CREATE TABLE mv_src (id UInt64, value UInt64) ENGINE=UpsertMergeTree ORDER BY id UNIQUE KEY id"
        )
        node.query("CREATE TABLE mv_dst (c UInt64) ENGINE=MergeTree ORDER BY tuple()")
        node.query(
            "CREATE MATERIALIZED VIEW mv_view TO mv_dst AS SELECT count() AS c FROM mv_src"
        )
        try:
            settings = {
                "insert_deduplication_token": "retry",
                "deduplicate_blocks_in_dependent_materialized_views": dedup_views,
            }
            for _ in range(2):
                node.query("INSERT INTO mv_src VALUES (1,10),(2,20)", settings=settings)
            assert node.query("SELECT count() FROM mv_src") == "2\n"
            actual = node.query("SELECT sum(c),count() FROM mv_dst")
            assert actual == "2\t1\n", actual
        finally:
            node.query("DROP TABLE mv_view SYNC")
            node.query("DROP TABLE mv_src SYNC")
            node.query("DROP TABLE mv_dst SYNC")


def create_partitioned(name, versioned=True):
    engine = "UpsertMergeTree(ver)" if versioned else "UpsertMergeTree"
    node.query(
        f"CREATE TABLE {name} (p UInt8,id UInt64,ver UInt64,value String) ENGINE={engine} PARTITION BY p ORDER BY id UNIQUE KEY id"
    )


def drop(*tables):
    for table in tables:
        node.query(f"DROP TABLE IF EXISTS {table} SYNC")


def files(code, *args):
    return node.exec_in_container(["python3", "-c", code, *args])


def table_path(table):
    return node.query(
        f"SELECT data_paths[1] FROM system.tables WHERE database=currentDatabase() AND name='{table}'"
    ).strip()


def test_mv_partial_partition_retry():
    create_partitioned("mv_partial")
    node.query("CREATE TABLE mv_counts (c UInt64) ENGINE=MergeTree ORDER BY tuple()")
    node.query(
        "CREATE MATERIALIZED VIEW mv_partial_view TO mv_counts AS SELECT count() c FROM mv_partial"
    )
    try:
        settings = {
            "insert_deduplication_token": "partitions",
            "deduplicate_blocks_in_dependent_materialized_views": 1,
        }
        node.query("INSERT INTO mv_partial VALUES (1,1,1,'a')", settings=settings)
        node.query(
            "INSERT INTO mv_partial VALUES (1,1,1,'a'),(2,2,1,'b')", settings=settings
        )
        node.query(
            "INSERT INTO mv_partial VALUES (1,1,1,'a'),(2,2,1,'b')", settings=settings
        )
        actual = node.query("SELECT sum(c),count() FROM mv_counts")
        assert actual == "2\t2\n", actual
    finally:
        drop("mv_partial_view", "mv_partial", "mv_counts")


def test_mv_async_and_upsert_target():
    for wait in (0, 1):
        create_partitioned("mv_async_src")
        create_partitioned("mv_async_dst")
        node.query(
            "CREATE MATERIALIZED VIEW mv_async_view TO mv_async_dst AS SELECT * FROM mv_async_src"
        )
        try:
            settings = {
                "async_insert": 1,
                "wait_for_async_insert": 0,
                "wait_for_part_commit_in_dependent_materialized_views": wait,
                "insert_deduplication_token": "async-mv",
            }
            for _ in range(2):
                node.query(
                    "INSERT INTO mv_async_src VALUES (1,1,1,'a')", settings=settings
                )
            assert node.query("SELECT value FROM mv_async_dst") == "a\n"
            node.restart_clickhouse(kill=True)
            assert node.query("SELECT value FROM mv_async_dst") == "a\n"
        finally:
            drop("mv_async_view", "mv_async_src", "mv_async_dst")


def test_partition_operations():
    create_partitioned("parts_a")
    create_partitioned("parts_b")
    try:
        node.query("INSERT INTO parts_a VALUES (1,1,2,'new'),(2,2,1,'two')")
        node.query("INSERT INTO parts_a VALUES (1,1,1,'old')")
        node.query("ALTER TABLE parts_a DETACH PARTITION 1")
        assert node.query("SELECT value FROM parts_a") == "two\n"
        node.query("ALTER TABLE parts_a ATTACH PARTITION 1")
        assert node.query("SELECT value FROM parts_a WHERE p=1") == "new\n"
        node.query("ALTER TABLE parts_b ATTACH PARTITION 1 FROM parts_a")
        assert node.query("SELECT value FROM parts_b") == "new\n"
        node.query(
            "INSERT INTO parts_b VALUES (1,3,1,'extra')",
            settings={"insert_deduplication_token": "replace-token"},
        )
        node.query("ALTER TABLE parts_b REPLACE PARTITION 1 FROM parts_a")
        assert node.query("SELECT value FROM parts_b") == "new\n"
        node.query(
            "INSERT INTO parts_b VALUES (1,3,1,'extra')",
            settings={"insert_deduplication_token": "replace-token"},
        )
        assert node.query("SELECT count() FROM parts_b") == "2\n"
        node.query("ALTER TABLE parts_a MOVE PARTITION 2 TO TABLE parts_b")
        assert node.query("SELECT count() FROM parts_a WHERE p=2") == "0\n"
        assert node.query("SELECT value FROM parts_b WHERE p=2") == "two\n"
        node.query("ALTER TABLE parts_b DROP PARTITION 1")
        node.restart_clickhouse(kill=True)
        assert node.query("SELECT value FROM parts_b") == "two\n"
    finally:
        drop("parts_a", "parts_b")


def test_transfer_allows_mv_inserts_and_unrelated_partitions():
    create_partitioned("transfer_a")
    create_partitioned("transfer_b")
    create_partitioned("unrelated")
    node.query("INSERT INTO transfer_a VALUES (1,1,1,'original')")
    node.query("INSERT INTO unrelated VALUES (1,1,1,'other')")
    node.query(
        "CREATE MATERIALIZED VIEW transfer_view TO transfer_b AS SELECT * FROM transfer_a"
    )
    point = "upsert_partition_pause_after_prepare"
    try:
        node.query(f"SYSTEM ENABLE FAILPOINT {point}")
        with ThreadPoolExecutor(max_workers=1) as pool:
            transfer = pool.submit(
                node.query,
                "ALTER TABLE transfer_a MOVE PARTITION 1 TO TABLE transfer_b",
            )
            try:
                node.query(f"SYSTEM WAIT FAILPOINT {point} PAUSE")
                node.query(
                    "INSERT INTO transfer_a VALUES (1,1,2,'concurrent'),(1,2,1,'new-key')",
                    timeout=10,
                    settings={"lock_acquire_timeout": 2},
                )
                node.query("ALTER TABLE unrelated DROP PARTITION 1", timeout=10)
                assert not transfer.done()
            finally:
                node.query(f"SYSTEM DISABLE FAILPOINT {point}")
            transfer.result(timeout=30)
        expected = "1\tconcurrent\n2\tnew-key\n"
        assert node.query("SELECT id,value FROM transfer_a ORDER BY id") == expected
        assert node.query("SELECT id,value FROM transfer_b ORDER BY id") == expected
        node.restart_clickhouse(kill=True)
        assert node.query("SELECT id,value FROM transfer_a ORDER BY id") == expected
    finally:
        node.query(f"SYSTEM DISABLE FAILPOINT {point}")
        drop("transfer_view", "transfer_a", "transfer_b", "unrelated")


def test_attach_allows_inserts():
    create_partitioned("attach_busy")
    node.query("INSERT INTO attach_busy VALUES (1,1,1,'old')")
    node.query("ALTER TABLE attach_busy DETACH PARTITION 1")
    point = "upsert_partition_pause_after_prepare"
    try:
        node.query(f"SYSTEM ENABLE FAILPOINT {point}")
        with ThreadPoolExecutor(max_workers=1) as pool:
            attaching = pool.submit(
                node.query, "ALTER TABLE attach_busy ATTACH PARTITION 1"
            )
            try:
                node.query(f"SYSTEM WAIT FAILPOINT {point} PAUSE")
                node.query(
                    "INSERT INTO attach_busy VALUES (1,1,2,'new')",
                    timeout=10,
                    settings={"lock_acquire_timeout": 2},
                )
                assert not attaching.done()
            finally:
                node.query(f"SYSTEM DISABLE FAILPOINT {point}")
            attaching.result(timeout=30)
        assert node.query("SELECT value FROM attach_busy") == "new\n"
    finally:
        node.query(f"SYSTEM DISABLE FAILPOINT {point}")
        drop("attach_busy")


def test_attach_recovery():
    for point, committed in (
        ("upsert_partition_pause_after_attach_rename", False),
        ("upsert_pause_after_state_sync", True),
    ):
        create_partitioned("attach_recover")
        node.query("INSERT INTO attach_recover VALUES (1,1,1,'one')")
        node.query("ALTER TABLE attach_recover DETACH PARTITION 1")
        try:
            node.query(f"SYSTEM ENABLE FAILPOINT {point}")
            with ThreadPoolExecutor(max_workers=1) as pool:
                attaching = pool.submit(
                    node.query, "ALTER TABLE attach_recover ATTACH PARTITION 1"
                )
                node.query(f"SYSTEM WAIT FAILPOINT {point} PAUSE")
                node.restart_clickhouse(kill=True)
                with pytest.raises(Exception):
                    attaching.result(timeout=30)
            assert node.query("SELECT count() FROM attach_recover") == (
                "1\n" if committed else "0\n"
            )
            if not committed:
                node.query("ALTER TABLE attach_recover ATTACH PARTITION 1")
            assert node.query("SELECT value FROM attach_recover") == "one\n"
            # Successful `ATTACH` schedules cleanup after publication.
            detached_query = (
                "SELECT name,reason FROM system.detached_parts "
                "WHERE database=currentDatabase() AND table='attach_recover' ORDER BY name"
            )
            deadline = time.monotonic() + 60
            while True:
                detached = node.query(detached_query)
                if not detached:
                    break
                assert time.monotonic() < deadline, detached
                time.sleep(0.1)
        finally:
            node.query(f"SYSTEM DISABLE FAILPOINT {point}")
            drop("attach_recover")


def test_detach_uncommitted_clone():
    create_partitioned("detach_recover", versioned=False)
    node.query("INSERT INTO detach_recover VALUES (1,1,1,'old')")
    point = "upsert_partition_pause_after_detach_clone"
    try:
        node.query(f"SYSTEM ENABLE FAILPOINT {point}")
        with ThreadPoolExecutor(max_workers=1) as pool:
            detaching = pool.submit(
                node.query, "ALTER TABLE detach_recover DETACH PARTITION 1"
            )
            node.query(f"SYSTEM WAIT FAILPOINT {point} PAUSE")
            node.restart_clickhouse(kill=True)
            with pytest.raises(Exception):
                detaching.result(timeout=30)
        assert node.query("SELECT value FROM detach_recover") == "old\n"
        assert "active source identity" in node.query_and_get_error(
            "ALTER TABLE detach_recover ATTACH PARTITION 1"
        )
        node.query(
            "ALTER TABLE detach_recover DROP DETACHED PARTITION 1",
            settings={"allow_drop_detached": 1},
        )
        node.query("INSERT INTO detach_recover VALUES (1,1,2,'new')")
        assert node.query("SELECT value FROM detach_recover") == "new\n"
    finally:
        node.query(f"SYSTEM DISABLE FAILPOINT {point}")
        drop("detach_recover")


def test_move_interrupted_between_tables():
    create_partitioned("move_recover_a")
    create_partitioned("move_recover_b")
    node.query("INSERT INTO move_recover_a VALUES (1,1,1,'one')")
    point = "upsert_partition_pause_after_move_import"
    try:
        node.query(f"SYSTEM ENABLE FAILPOINT {point}")
        with ThreadPoolExecutor(max_workers=1) as pool:
            moving = pool.submit(
                node.query,
                "ALTER TABLE move_recover_a MOVE PARTITION 1 TO TABLE move_recover_b",
            )
            node.query(f"SYSTEM WAIT FAILPOINT {point} PAUSE")
            node.restart_clickhouse(kill=True)
            with pytest.raises(Exception):
                moving.result(timeout=30)
        for name in ("move_recover_a", "move_recover_b"):
            assert node.query(f"SELECT value FROM {name}") == "one\n"
        node.query(
            "ALTER TABLE move_recover_a MOVE PARTITION 1 TO TABLE move_recover_b"
        )
        assert node.query("SELECT count() FROM move_recover_a") == "0\n"
        assert node.query("SELECT count() FROM move_recover_b") == "1\n"
    finally:
        node.query(f"SYSTEM DISABLE FAILPOINT {point}")
        drop("move_recover_a", "move_recover_b")


def test_detached_recovery_name_collision():
    create_partitioned("name_collision")
    node.query("INSERT INTO name_collision VALUES (1,1,1,'one')")
    node.query("ALTER TABLE name_collision DETACH PARTITION 1")
    path = table_path("name_collision")
    point = "upsert_partition_pause_after_attach_rename"
    try:
        node.query(f"SYSTEM ENABLE FAILPOINT {point}")
        with ThreadPoolExecutor(max_workers=1) as pool:
            attaching = pool.submit(
                node.query, "ALTER TABLE name_collision ATTACH PARTITION 1"
            )
            node.query(f"SYSTEM WAIT FAILPOINT {point} PAUSE")
            files(
                "import pathlib,shutil,sys; d=pathlib.Path(sys.argv[1])/'detached'; p=next(d.glob('attaching_*')); shutil.copytree(p,d/p.name[10:])",
                path,
            )
            node.restart_clickhouse(kill=True)
            with pytest.raises(Exception):
                attaching.result(timeout=30)
        assert node.query("SELECT count() FROM name_collision") == "0\n"
        names = json.loads(
            files(
                "import pathlib,json,sys; print(json.dumps([p.name for p in (pathlib.Path(sys.argv[1])/'detached').iterdir()]))",
                path,
            )
        )
        assert len(names) == 2 and any(
            name.startswith("upsert_recovered_") for name in names
        ), names
        node.query("ALTER TABLE name_collision ATTACH PARTITION 1")
        assert node.query("SELECT value FROM name_collision") == "one\n"
        recovered = next(name for name in names if name.startswith("upsert_recovered_"))
        assert recovered in node.query(
            "SELECT name FROM system.detached_parts WHERE table='name_collision'"
        )
        node.query("ALTER TABLE name_collision DROP PARTITION 1")
        files(
            "import pathlib,sys; d=pathlib.Path(sys.argv[1])/'detached'; old=d/sys.argv[2]; target=d/sys.argv[2].split('_',3)[3]; assert not target.exists(); old.rename(target)",
            path,
            recovered,
        )
        node.query("ALTER TABLE name_collision ATTACH PARTITION 1")
        node.restart_clickhouse(kill=True)
        assert node.query("SELECT value FROM name_collision") == "one\n"
    finally:
        node.query(f"SYSTEM DISABLE FAILPOINT {point}")
        drop("name_collision")


def test_freeze_restore_and_unfreeze():
    create_partitioned("frozen")
    node.query("INSERT INTO frozen VALUES (1,1,1,'old'),(1,2,1,'two')")
    node.query("INSERT INTO frozen VALUES (1,1,2,'new')")
    path = table_path("frozen")
    try:
        node.query("ALTER TABLE frozen FREEZE WITH NAME 'upsert_review'")
        node.query("TRUNCATE TABLE frozen")
        files(
            "import pathlib,shutil,sys; p=pathlib.Path(sys.argv[1]); root=p.parents[2]; backup=root/'shadow/upsert_review'/p.relative_to(root); detached=p/'detached'; detached.mkdir(exist_ok=True); [shutil.copytree(x,detached/x.name) for x in backup.iterdir() if x.is_dir()]",
            path,
        )
        node.query("ALTER TABLE frozen ATTACH PARTITION 1")
        assert (
            node.query("SELECT id,value FROM frozen ORDER BY id") == "1\tnew\n2\ttwo\n"
        )
        node.restart_clickhouse(kill=True)
        assert (
            node.query("SELECT id,value FROM frozen ORDER BY id") == "1\tnew\n2\ttwo\n"
        )
        node.query("ALTER TABLE frozen UNFREEZE WITH NAME 'upsert_review'")
    finally:
        drop("frozen")


def test_import_preserves_admitted_key_limit():
    for table in ("wide_key_a", "wide_key_b"):
        node.query(
            f"CREATE TABLE {table} (id String,value UInt64) ENGINE=UpsertMergeTree ORDER BY value UNIQUE KEY id"
        )
    try:
        node.query(
            "INSERT INTO wide_key_a SELECT repeat('k',6000),1",
            settings={"unique_key_max_encoded_size": 10000},
        )
        node.query(
            "ALTER TABLE wide_key_b ATTACH PARTITION tuple() FROM wide_key_a",
            settings={"unique_key_max_encoded_size": 10},
        )
        node.query("ALTER TABLE wide_key_b DETACH PARTITION tuple()")
        node.query(
            "ALTER TABLE wide_key_b ATTACH PARTITION tuple()",
            settings={"unique_key_max_encoded_size": 10},
        )
        assert node.query("SELECT length(id),value FROM wide_key_b") == "6000\t1\n"
    finally:
        drop("wide_key_a", "wide_key_b")


def test_projection_mixed_visibility():
    for aggregate in (False, True):
        projection = (
            "SELECT p,sum(value),count() GROUP BY p"
            if aggregate
            else "SELECT * ORDER BY value"
        )
        node.query(
            f"CREATE TABLE projected (p UInt8,id UInt64,ver UInt64,value UInt64,pad String,PROJECTION by_value ({projection})) ENGINE=UpsertMergeTree(ver) PARTITION BY p ORDER BY id UNIQUE KEY id"
        )
        try:
            node.query(
                "INSERT INTO projected SELECT 1,number,1,number,repeat('x',2000) FROM numbers(20000)",
                settings={
                    "max_insert_block_size": 20000,
                    "min_insert_block_size_rows": 20000,
                },
            )
            node.query("INSERT INTO projected VALUES (1,0,2,999999,'new')")
            query = "SELECT p,sum(value),count() FROM projected GROUP BY p ORDER BY p"
            expected = node.query(query, settings={"optimize_use_projections": 0})
            assert expected == "1\t200989999\t20000\n", expected
            actual = node.query(
                query,
                settings={
                    "force_optimize_projection": 1,
                    "optimize_use_implicit_projections": 0,
                },
            )
            assert actual == expected, actual
            plan = node.query(
                "EXPLAIN projections=1 " + query,
                settings={
                    "force_optimize_projection": 1,
                    "optimize_use_implicit_projections": 0,
                },
            )
            assert "ReadFromMergeTree (by_value)" in plan, plan
            assert "ReadFromMergeTree (default.projected)" in plan, plan
            assert (
                node.query("SELECT count(),min(id),max(id) FROM projected")
                == "20000\t0\t19999\n"
            )
            node.query(
                "ALTER TABLE projected MATERIALIZE PROJECTION by_value IN PARTITION 1"
            )
            assert node.query(query) == expected
            node.query("ALTER TABLE projected DROP PROJECTION by_value")
            node.query(f"ALTER TABLE projected ADD PROJECTION by_value ({projection})")
            node.query("ALTER TABLE projected MATERIALIZE PROJECTION by_value")
            node.restart_clickhouse(kill=True)
            assert node.query(query) == expected
        finally:
            drop("projected")


def test_drop_part_preserves_tokens_without_resurrection():
    create_partitioned("part_tokens")
    try:
        node.query("INSERT INTO part_tokens VALUES (1,1,1,'old')")
        settings = {"insert_deduplication_token": "part-token"}
        node.query("INSERT INTO part_tokens VALUES (1,1,2,'new')", settings=settings)
        part = node.query(
            "SELECT name FROM system.parts WHERE active AND table='part_tokens' ORDER BY max_block_number DESC LIMIT 1"
        ).strip()
        node.query(f"ALTER TABLE part_tokens DROP PART '{part}'")
        assert node.query("SELECT count() FROM part_tokens WHERE id=1") == "0\n"
        node.query("INSERT INTO part_tokens VALUES (1,1,2,'new')", settings=settings)
        assert node.query("SELECT count() FROM part_tokens WHERE id=1") == "0\n"
        node.query("ALTER TABLE part_tokens DROP PARTITION 1")
        node.query("INSERT INTO part_tokens VALUES (1,1,2,'new')", settings=settings)
        assert node.query("SELECT value FROM part_tokens") == "new\n"
    finally:
        drop("part_tokens")


def test_partition_budget_rejection_preserves_tables():
    for name in ("budget_source", "budget_target"):
        node.query(
            f"CREATE TABLE {name} (id String,value UInt64) ENGINE=UpsertMergeTree ORDER BY value UNIQUE KEY id"
        )
    try:
        node.query(
            "INSERT INTO budget_source SELECT concat(toString(number),repeat('k',5000)),number FROM numbers(30000)",
            settings={"unique_key_max_encoded_size": 10000},
            timeout=180,
        )
        node.query("INSERT INTO budget_target VALUES ('sentinel',999)")
        error = node.query_and_get_error(
            "ALTER TABLE budget_target ATTACH PARTITION tuple() FROM budget_source",
            timeout=180,
        )
        assert "state budget" in error, error
        assert node.query("SELECT count() FROM budget_source") == "30000\n"
        assert node.query("SELECT id FROM budget_target") == "sentinel\n"
        node.query("INSERT INTO budget_target VALUES ('still-usable',1000)")
        node.restart_clickhouse(kill=True)
        assert node.query("SELECT count() FROM budget_target") == "2\n"
    finally:
        drop("budget_source", "budget_target")


def test_mv_multiunit_retry():
    node.query(
        "CREATE TABLE multi_src (id UInt64,pad String) ENGINE=UpsertMergeTree ORDER BY id UNIQUE KEY id"
    )
    node.query("CREATE TABLE multi_dst (c UInt64) ENGINE=MergeTree ORDER BY tuple()")
    node.query(
        "CREATE MATERIALIZED VIEW multi_view TO multi_dst AS SELECT count() c FROM multi_src"
    )
    try:
        settings = {
            "insert_deduplication_token": "multi",
            "max_insert_threads": 4,
            "max_block_size": 5000,
            "max_insert_block_size": 5000,
            "min_insert_block_size_rows": 5000,
        }
        for _ in range(2):
            node.query(
                "INSERT INTO multi_src SELECT number,repeat('m',10000) FROM numbers(5000)",
                settings=settings,
            )
        assert node.query("SELECT count() FROM multi_src") == "5000\n"
        actual = node.query("SELECT sum(c) FROM multi_dst")
        assert actual == "5000\n", actual
    finally:
        drop("multi_view", "multi_src", "multi_dst")


def test_other_mergetree_projection_regression():
    node.query(
        "CREATE TABLE plain_projection (id UInt64,value UInt64,PROJECTION by_value (SELECT * ORDER BY value)) ENGINE=MergeTree ORDER BY id"
    )
    try:
        node.query(
            "INSERT INTO plain_projection SELECT number,10000-number FROM numbers(10000)"
        )
        query = "SELECT sum(id) FROM plain_projection WHERE value<100"
        expected = node.query(query, settings={"optimize_use_projections": 0})
        assert node.query(query, settings={"force_optimize_projection": 1}) == expected
        node.query("ALTER TABLE plain_projection DROP PROJECTION by_value")
        node.query(
            "ALTER TABLE plain_projection ADD PROJECTION by_value (SELECT * ORDER BY value)"
        )
        node.query(
            "ALTER TABLE plain_projection MATERIALIZE PROJECTION by_value",
            settings={"mutations_sync": 2},
        )
        assert node.query(query, settings={"force_optimize_projection": 1}) == expected
    finally:
        drop("plain_projection")


def test_mv_forwards_submitted_rows_and_cascaded_retries():
    create_partitioned("events_src")
    create_partitioned("events_middle")
    node.query(
        "CREATE TABLE events_counts (c UInt64) ENGINE=MergeTree ORDER BY tuple()"
    )
    node.query(
        "CREATE MATERIALIZED VIEW events_first TO events_middle AS SELECT * FROM events_src"
    )
    node.query(
        "CREATE MATERIALIZED VIEW events_second TO events_counts AS SELECT count() c FROM events_middle"
    )
    try:
        settings = {
            "insert_deduplication_token": "submitted-rows",
            "deduplicate_blocks_in_dependent_materialized_views": 1,
        }
        for _ in range(2):
            node.query(
                "INSERT INTO events_src VALUES (1,1,3,'new'),(1,1,1,'old'),(1,2,1,'two')",
                settings=settings,
            )
        assert (
            node.query("SELECT id,value FROM events_middle ORDER BY id")
            == "1\tnew\n2\ttwo\n"
        )
        assert node.query("SELECT sum(c) FROM events_counts") == "3\n"
        node.query(
            "INSERT INTO events_src VALUES (1,1,2,'loser')",
            settings={"insert_deduplication_token": "loser"},
        )
        assert node.query("SELECT sum(c) FROM events_counts") == "4\n"
        assert node.query("SELECT value FROM events_src WHERE id=1") == "new\n"
    finally:
        drop(
            "events_first",
            "events_second",
            "events_src",
            "events_middle",
            "events_counts",
        )


def test_replace_preserves_concurrent_winners_and_tokens():
    create_partitioned("replace_source")
    create_partitioned("replace_target")
    node.query("INSERT INTO replace_source VALUES (1,1,1,'imported')")
    node.query("INSERT INTO replace_target VALUES (1,1,1,'original'),(1,2,1,'removed')")
    point = "upsert_partition_pause_after_prepare"
    settings = {"insert_deduplication_token": "concurrent-replace"}
    try:
        node.query(f"SYSTEM ENABLE FAILPOINT {point}")
        with ThreadPoolExecutor(max_workers=1) as pool:
            replacement = pool.submit(
                node.query,
                "ALTER TABLE replace_target REPLACE PARTITION 1 FROM replace_source",
            )
            try:
                node.query(f"SYSTEM WAIT FAILPOINT {point} PAUSE")
                node.query(
                    "INSERT INTO replace_target VALUES (1,1,2,'concurrent'),(1,3,1,'fresh')",
                    settings=settings,
                    timeout=10,
                )
                node.query(
                    "INSERT INTO replace_source VALUES (1,4,1,'after-snapshot')",
                    timeout=10,
                )
            finally:
                node.query(f"SYSTEM DISABLE FAILPOINT {point}")
            replacement.result(timeout=30)
        expected = "1\tconcurrent\n3\tfresh\n"
        assert node.query("SELECT id,value FROM replace_target ORDER BY id") == expected
        node.query("DELETE FROM replace_target WHERE id=3")
        node.query(
            "INSERT INTO replace_target VALUES (1,1,2,'concurrent'),(1,3,1,'fresh')",
            settings=settings,
        )
        node.restart_clickhouse(kill=True)
        assert (
            node.query("SELECT id,value FROM replace_target ORDER BY id")
            == "1\tconcurrent\n"
        )
    finally:
        node.query(f"SYSTEM DISABLE FAILPOINT {point}")
        drop("replace_source", "replace_target")


def test_large_partition_removal_with_concurrent_inserts():
    # Encoded winner keys alone exceed the 128 MiB state batch budget.
    for operation, same_partition in (
        ("DROP", False),
        ("DETACH", True),
        ("REPLACE", True),
    ):
        for table in ("large_remove", "large_import"):
            node.query(
                f"CREATE TABLE {table} (p UInt8,id String,ver UInt64,value String) ENGINE=UpsertMergeTree(ver) PARTITION BY p ORDER BY id UNIQUE KEY id"
            )
        point = "upsert_partition_pause_after_prepare"
        seed_settings = {"insert_deduplication_token": "old-seed"}
        concurrent_settings = {"insert_deduplication_token": "new-seed"}
        try:
            node.query(
                "INSERT INTO large_remove VALUES (1,'seed',1,'old')",
                settings=seed_settings,
            )
            node.query(
                "INSERT INTO large_remove SELECT 1,concat(repeat('k',200),toString(number)),1,'old' FROM numbers(800000)",
                timeout=240,
            )
            node.query("INSERT INTO large_import VALUES (1,'replacement',1,'imported')")
            sql = "ALTER TABLE large_remove " + (
                "REPLACE PARTITION 1 FROM large_import"
                if operation == "REPLACE"
                else f"{operation} PARTITION 1"
            )
            node.query(f"SYSTEM ENABLE FAILPOINT {point}")
            with ThreadPoolExecutor(max_workers=1) as pool:
                removal = pool.submit(node.query, sql, timeout=240)
                try:
                    node.query(f"SYSTEM WAIT FAILPOINT {point} PAUSE", timeout=60)
                    partition = 1 if same_partition else 2
                    node.query(
                        f"INSERT INTO large_remove VALUES ({partition},'concurrent',2,'new')",
                        settings=concurrent_settings,
                        timeout=10,
                    )
                    if same_partition:
                        node.query(
                            "INSERT INTO large_remove SELECT 1,concat(repeat('k',200),'0'),2,'new-winner'",
                            timeout=10,
                        )
                finally:
                    node.query(f"SYSTEM DISABLE FAILPOINT {point}")
                removal.result(timeout=240)
            assert node.query("SELECT count() FROM large_remove WHERE p=1") == (
                "3\n" if operation == "REPLACE" else "2\n" if same_partition else "0\n"
            )
            if same_partition:
                assert (
                    node.query(
                        "SELECT value FROM large_remove WHERE id=concat(repeat('k',200),'0')"
                    )
                    == "new-winner\n"
                )
            node.query(
                "INSERT INTO large_remove VALUES (1,'seed',1,'old')",
                settings=seed_settings,
            )
            assert node.query("SELECT count() FROM large_remove WHERE id='seed'") == (
                "0\n" if same_partition else "1\n"
            )
            node.query("DELETE FROM large_remove WHERE id='concurrent'")
            node.query(
                f"INSERT INTO large_remove VALUES ({partition},'concurrent',2,'new')",
                settings=concurrent_settings,
            )
            assert (
                node.query("SELECT count() FROM large_remove WHERE id='concurrent'")
                == "0\n"
            )
            node.restart_clickhouse(kill=True)
            assert node.query("SELECT count() FROM large_remove WHERE value='old'") == (
                "0\n" if same_partition else "1\n"
            )
        finally:
            node.query(f"SYSTEM DISABLE FAILPOINT {point}")
            drop("large_remove", "large_import")


def test_partition_precommit_failure_leaves_table_usable():
    create_partitioned("precommit_source")
    create_partitioned("precommit_target")
    try:
        for start in (0, 10000):
            node.query(
                f"INSERT INTO precommit_source SELECT 1,number,1,concat(toString(number),repeat('x',2000)) FROM numbers({start},10000)"
            )
        node.query("INSERT INTO precommit_target VALUES (1,99999,1,'sentinel')")
        for point in (
            "upsert_throw_during_part_precommit",
            "upsert_throw_before_part_persist",
        ):
            node.query(f"SYSTEM ENABLE FAILPOINT {point}")
            try:
                assert "FAULT_INJECTED" in node.query_and_get_error(
                    "ALTER TABLE precommit_target REPLACE PARTITION 1 FROM precommit_source"
                )
            finally:
                node.query(f"SYSTEM DISABLE FAILPOINT {point}")
            assert node.query("SELECT value FROM precommit_target") == "sentinel\n"
            assert node.query("SELECT count() FROM precommit_source") == "20000\n"
            assert (
                node.query(
                    "SELECT count() FROM system.parts WHERE table='precommit_target' AND active"
                )
                == "1\n"
            )
            node.query("INSERT INTO precommit_target VALUES (2,1,1,'usable')")
            node.query("ALTER TABLE precommit_target DROP PARTITION 2")
        node.query(
            "ALTER TABLE precommit_target REPLACE PARTITION 1 FROM precommit_source"
        )
        node.restart_clickhouse(kill=True)
        assert node.query("SELECT count() FROM precommit_target") == "20000\n"
    finally:
        drop("precommit_source", "precommit_target")


def test_insert_precommit_failure_preserves_retry():
    create_partitioned("precommit_insert")
    point = "upsert_throw_before_part_persist"
    settings = {"insert_deduplication_token": "precommit-token"}
    try:
        node.query(f"SYSTEM ENABLE FAILPOINT {point}")
        try:
            assert "FAULT_INJECTED" in node.query_and_get_error(
                "INSERT INTO precommit_insert VALUES (1,1,1,'one')", settings=settings
            )
        finally:
            node.query(f"SYSTEM DISABLE FAILPOINT {point}")
        assert node.query("SELECT count() FROM precommit_insert") == "0\n"
        node.query(
            "INSERT INTO precommit_insert VALUES (1,1,1,'one')", settings=settings
        )
        node.restart_clickhouse(kill=True)
        assert node.query("SELECT value FROM precommit_insert") == "one\n"
    finally:
        drop("precommit_insert")


def test_replace_unversioned_import_wins_same_key():
    create_partitioned("replace_plain_source", versioned=False)
    create_partitioned("replace_plain_target", versioned=False)
    node.query("INSERT INTO replace_plain_source VALUES (1,1,1,'imported')")
    node.query("INSERT INTO replace_plain_target VALUES (1,1,1,'old')")
    point = "upsert_partition_pause_after_prepare"
    try:
        node.query(f"SYSTEM ENABLE FAILPOINT {point}")
        with ThreadPoolExecutor(max_workers=1) as pool:
            replacing = pool.submit(
                node.query,
                "ALTER TABLE replace_plain_target REPLACE PARTITION 1 FROM replace_plain_source",
            )
            try:
                node.query(f"SYSTEM WAIT FAILPOINT {point} PAUSE")
                node.query(
                    "INSERT INTO replace_plain_target VALUES (1,1,2,'concurrent'),(1,2,1,'untouched')",
                    timeout=10,
                )
            finally:
                node.query(f"SYSTEM DISABLE FAILPOINT {point}")
            replacing.result(timeout=30)
        node.restart_clickhouse(kill=True)
        assert (
            node.query("SELECT id,value FROM replace_plain_target ORDER BY id")
            == "1\timported\n2\tuntouched\n"
        )
    finally:
        node.query(f"SYSTEM DISABLE FAILPOINT {point}")
        drop("replace_plain_source", "replace_plain_target")


def test_partition_survivor_budget_failure_is_atomic():
    node.query(
        "CREATE TABLE survivor_budget (p UInt8,id String,value UInt64) ENGINE=UpsertMergeTree PARTITION BY p ORDER BY id UNIQUE KEY id"
    )
    node.query("INSERT INTO survivor_budget VALUES (1,'old',0)")
    point = "upsert_partition_pause_after_prepare"
    try:
        node.query(f"SYSTEM ENABLE FAILPOINT {point}")
        with ThreadPoolExecutor(max_workers=1) as pool:
            removal = pool.submit(
                node.query_and_get_error,
                "ALTER TABLE survivor_budget DROP PARTITION 1",
                timeout=240,
            )
            try:
                node.query(f"SYSTEM WAIT FAILPOINT {point} PAUSE")
                node.query(
                    "INSERT INTO survivor_budget SELECT 1,concat(repeat('k',200),toString(number)),number FROM numbers(800000)",
                    timeout=240,
                )
            finally:
                node.query(f"SYSTEM DISABLE FAILPOINT {point}")
            assert "LIMIT_EXCEEDED" in removal.result(timeout=240)
        assert node.query("SELECT count() FROM survivor_budget") == "800001\n"
        assert node.query("SELECT value FROM survivor_budget WHERE id='old'") == "0\n"
        node.query("INSERT INTO survivor_budget VALUES (2,'usable',1)")
        node.restart_clickhouse(kill=True)
        assert node.query("SELECT count() FROM survivor_budget") == "800002\n"
        node.query("ALTER TABLE survivor_budget DROP PARTITION 1")
        assert node.query("SELECT id FROM survivor_budget") == "usable\n"
    finally:
        node.query(f"SYSTEM DISABLE FAILPOINT {point}")
        drop("survivor_budget")


def test_partition_range_removal_recovers_survivors():
    for replace in (False, True):
        create_partitioned("range_recover")
        create_partitioned("range_import")
        node.query("INSERT INTO range_recover VALUES (1,1,1,'old'),(1,2,1,'removed')")
        node.query(
            "INSERT INTO range_import VALUES (1,1,1,'imported-old'),(1,4,1,'imported')"
        )
        prepare = "upsert_partition_pause_after_prepare"
        durable = "upsert_pause_after_state_sync"
        try:
            node.query(f"SYSTEM ENABLE FAILPOINT {prepare}")
            with ThreadPoolExecutor(max_workers=1) as pool:
                command = (
                    "REPLACE PARTITION 1 FROM range_import"
                    if replace
                    else "DROP PARTITION 1"
                )
                removal = pool.submit(
                    node.query, "ALTER TABLE range_recover " + command
                )
                try:
                    node.query(f"SYSTEM WAIT FAILPOINT {prepare} PAUSE")
                    node.query(
                        "INSERT INTO range_recover VALUES (1,1,2,'new-winner'),(1,3,1,'survivor')"
                    )
                    node.query(f"SYSTEM ENABLE FAILPOINT {durable}")
                    node.query(f"SYSTEM DISABLE FAILPOINT {prepare}")
                    node.query(f"SYSTEM WAIT FAILPOINT {durable} PAUSE")
                    node.restart_clickhouse(kill=True)
                    with pytest.raises(Exception):
                        removal.result(timeout=30)
                finally:
                    node.query(f"SYSTEM DISABLE FAILPOINT {prepare}")
                    node.query(f"SYSTEM DISABLE FAILPOINT {durable}")
            expected = "1\tnew-winner\n3\tsurvivor\n" + (
                "4\timported\n" if replace else ""
            )
            assert (
                node.query("SELECT id,value FROM range_recover ORDER BY id") == expected
            )
            node.query("INSERT INTO range_recover VALUES (1,2,1,'reinsert')")
            assert (
                node.query("SELECT value FROM range_recover WHERE id=2") == "reinsert\n"
            )
        finally:
            drop("range_recover", "range_import")
