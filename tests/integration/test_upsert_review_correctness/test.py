import time
import uuid

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance('node', with_zookeeper=True, main_configs=['configs/cluster.xml'])


@pytest.fixture(scope='module', autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def wait_count(table, expected, seconds=45):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if int(node.query(f'SELECT count() FROM {table}')) == expected:
            return
        time.sleep(0.1)
    assert int(node.query(f'SELECT count() FROM {table}')) == expected


def test_tuple_key_overwrite_delete_and_restart():
    node.query('DROP TABLE IF EXISTS review_tuple SYNC')
    node.query('''CREATE TABLE review_tuple (k Tuple(UInt64, Tuple(String, Int32)), v UInt64)
        ENGINE=UpsertMergeTree(v) ORDER BY k UNIQUE KEY k''')
    node.query("INSERT INTO review_tuple VALUES ((1, ('ab', -1)), 1), ((1, ('a', -1)), 2), ((1, ('ab', -1)), 3)")
    assert node.query('SELECT k, v FROM review_tuple ORDER BY k').strip() == "(1,('a',-1))\t2\n(1,('ab',-1))\t3"
    node.query("DELETE FROM review_tuple WHERE k = tuple(1, tuple('a', -1))")
    node.restart_clickhouse()
    assert node.query('SELECT v FROM review_tuple').strip() == '3'
    node.query('DROP TABLE review_tuple SYNC')


def test_native_transfer_rejects_masked_source():
    for t in ('review_transfer_source', 'review_transfer_target'):
        node.query(f'DROP TABLE IF EXISTS {t} SYNC')
    node.query('CREATE TABLE review_transfer_source (id UInt64, v UInt64) ENGINE=UpsertMergeTree(v) ORDER BY id UNIQUE KEY id')
    node.query('CREATE TABLE review_transfer_target (id UInt64, v UInt64) ENGINE=MergeTree ORDER BY id')
    node.query('INSERT INTO review_transfer_source VALUES (1,1)')
    node.query('INSERT INTO review_transfer_source VALUES (1,2)')
    node.query('DELETE FROM review_transfer_source WHERE id=1')
    assert 'UNIQUE KEY' in node.query_and_get_error("ALTER TABLE review_transfer_target ATTACH PARTITION ID 'all' FROM review_transfer_source")
    assert node.query('SELECT count() FROM review_transfer_target').strip() == '0'
    for t in ('review_transfer_source', 'review_transfer_target'):
        node.query(f'DROP TABLE {t} SYNC')


def test_replicated_transfer_rejects_masked_source():
    for t in ('review_rep_source', 'review_rep_target'):
        node.query(f'DROP TABLE IF EXISTS {t} SYNC')
    node.query('CREATE TABLE review_rep_source (id UInt64, v UInt64) ENGINE=UpsertMergeTree(v) ORDER BY id UNIQUE KEY id')
    node.query(f"CREATE TABLE review_rep_target (id UInt64, v UInt64) ENGINE=ReplicatedMergeTree('/clickhouse/review/{uuid.uuid4()}', 'r1') ORDER BY id")
    node.query('INSERT INTO review_rep_source VALUES (1,1)')
    node.query('INSERT INTO review_rep_source VALUES (1,2)')
    node.query('DELETE FROM review_rep_source WHERE id=1')
    for action in ('ATTACH', 'REPLACE'):
        assert 'visibility-aware' in node.query_and_get_error(f"ALTER TABLE review_rep_target {action} PARTITION ID 'all' FROM review_rep_source")
    assert node.query('SELECT count() FROM review_rep_target').strip() == '0'
    for t in ('review_rep_source', 'review_rep_target'):
        node.query(f'DROP TABLE {t} SYNC')


def test_insert_deduplicate_disabled():
    node.query('DROP TABLE IF EXISTS review_no_dedup SYNC')
    node.query('CREATE TABLE review_no_dedup (id UInt64, v UInt64) ENGINE=UpsertMergeTree(v) ORDER BY id UNIQUE KEY id')
    for version in (1, 2):
        node.query(f"INSERT INTO review_no_dedup SETTINGS insert_deduplicate=0, insert_deduplication_token='same' VALUES (1,{version})")
    assert node.query('SELECT v FROM review_no_dedup').strip() == '2'
    node.query('DROP TABLE review_no_dedup SYNC')


def test_due_part_after_healthy_prefix_exceeds_budget():
    node.query('DROP TABLE IF EXISTS review_ttl_budget SYNC')
    node.query('''CREATE TABLE review_ttl_budget (id UInt64, expires DateTime)
        ENGINE=UpsertMergeTree ORDER BY id UNIQUE KEY id TTL expires''')
    node.query('SYSTEM STOP MERGES review_ttl_budget')
    node.query('SYSTEM STOP TTL MERGES review_ttl_budget')
    try:
        node.query("INSERT INTO review_ttl_budget SELECT number, now()+INTERVAL 1 DAY FROM numbers(300000) SETTINGS max_block_size=300000, max_insert_threads=1")
        node.query("INSERT INTO review_ttl_budget SELECT number+300000, now()-INTERVAL 1 DAY FROM numbers(900000) SETTINGS max_block_size=900000, max_insert_threads=1")
        assert node.query("SELECT count() FROM system.parts WHERE active AND table='review_ttl_budget'").strip() == '2'
        node.query('SYSTEM START TTL MERGES review_ttl_budget')
        # Starting TTL alone must not bypass `STOP MERGES`.
        time.sleep(1.5)
        assert node.query('SELECT count() FROM review_ttl_budget').strip() == '1200000'
    finally:
        node.query('SYSTEM START MERGES review_ttl_budget')
        node.query('SYSTEM START TTL MERGES review_ttl_budget')
    wait_count('review_ttl_budget', 300000)
    node.query('DROP TABLE review_ttl_budget SYNC')


def test_oversized_source_ttl_rewrite():
    node.query('DROP TABLE IF EXISTS review_ttl_large SYNC')
    node.query('''CREATE TABLE review_ttl_large (id UInt64, s String, expires DateTime,
        INDEX large_minmax s TYPE minmax GRANULARITY 1)
        ENGINE=UpsertMergeTree ORDER BY id UNIQUE KEY id TTL expires
        SETTINGS index_granularity=1, index_granularity_bytes=0''')
    node.query('SYSTEM STOP MERGES review_ttl_large')
    node.query('SYSTEM STOP TTL MERGES review_ttl_large')
    try:
        # The user columns fit one unit, but per-row index payload makes the part larger.
        node.query("INSERT INTO review_ttl_large SELECT number, materialize(repeat('x', 1000000)), now()-INTERVAL 1 DAY FROM numbers(24)")
        assert int(node.query("SELECT max(data_uncompressed_bytes + secondary_indices_uncompressed_bytes) FROM system.parts WHERE active AND table='review_ttl_large'")) > 32 * 1024 * 1024
    finally:
        node.query('SYSTEM START TTL MERGES review_ttl_large')
        node.query('SYSTEM START MERGES review_ttl_large')
    wait_count('review_ttl_large', 0)
    node.query('DROP TABLE review_ttl_large SYNC')


def test_fenced_table_metadata_is_readable():
    node.query('DROP TABLE IF EXISTS review_fenced SYNC')
    node.query('CREATE TABLE review_fenced (id UInt64, INDEX i id TYPE minmax GRANULARITY 1) ENGINE=UpsertMergeTree ORDER BY id UNIQUE KEY id')
    node.query('SYSTEM ENABLE FAILPOINT upsert_maintenance_corrupt_candidate')
    try:
        node.query('INSERT INTO review_fenced VALUES (1)')
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            try:
                node.query('SELECT count() FROM review_fenced')
            except Exception as error:
                assert 'fenced' in str(error)
                break
            time.sleep(0.1)
        else:
            raise AssertionError('table was not fenced')
        assert node.query("SELECT name FROM system.columns WHERE table='review_fenced'").strip() == 'id'
        assert node.query("SELECT name FROM system.data_skipping_indices WHERE table='review_fenced'").strip() == 'i'
        assert 'fenced' in node.query_and_get_error('INSERT INTO review_fenced VALUES (2)')
    finally:
        node.query('SYSTEM DISABLE FAILPOINT upsert_maintenance_corrupt_candidate')
        node.restart_clickhouse()
    assert node.query('SELECT id FROM review_fenced').strip() == '1'
    node.query('DROP TABLE review_fenced SYNC')


def test_recovery_cleans_delete_temporary_directory():
    node.query('DROP TABLE IF EXISTS review_cleanup SYNC')
    node.query('CREATE TABLE review_cleanup (id UInt64) ENGINE=UpsertMergeTree ORDER BY id UNIQUE KEY id')
    node.query('INSERT INTO review_cleanup VALUES (1)')
    path = node.query("SELECT arrayElement(data_paths,1) FROM system.tables WHERE name='review_cleanup'").strip()
    node.query('DETACH TABLE review_cleanup SYNC')
    leftover = path + 'delete_tmp_all_100_100_0'
    node.exec_in_container(['mkdir', leftover])
    node.exec_in_container(['touch', leftover + '/partial'])
    node.query('ATTACH TABLE review_cleanup')
    node.exec_in_container(['test', '!', '-e', leftover])
    assert node.query('SELECT id FROM review_cleanup').strip() == '1'
    node.query('DROP TABLE review_cleanup SYNC')


def test_two_views_share_upsert_target():
    for enabled in (0, 1):
        for t in ('review_mv_a', 'review_mv_b', 'review_mv_source', 'review_mv_target'):
            node.query(f'DROP TABLE IF EXISTS {t} SYNC')
        node.query('CREATE TABLE review_mv_source (id UInt64, v UInt64) ENGINE=MergeTree ORDER BY id')
        node.query('CREATE TABLE review_mv_target (id UInt64, v UInt64) ENGINE=UpsertMergeTree(v) ORDER BY id UNIQUE KEY id')
        node.query('CREATE MATERIALIZED VIEW review_mv_a TO review_mv_target AS SELECT id, v FROM review_mv_source')
        node.query('CREATE MATERIALIZED VIEW review_mv_b TO review_mv_target AS SELECT id+100 AS id, v FROM review_mv_source')
        for _ in range(2):
            node.query(f"INSERT INTO review_mv_source SETTINGS insert_deduplication_token='views', deduplicate_blocks_in_dependent_materialized_views={enabled} VALUES (1,1)")
        assert node.query('SELECT id, v FROM review_mv_target ORDER BY id').strip() == '1\t1\n101\t1'
        for t in ('review_mv_a', 'review_mv_b', 'review_mv_source', 'review_mv_target'):
            node.query(f'DROP TABLE {t} SYNC')


def test_chained_upsert_retry_keeps_source_block_identity():
    for t in ('review_chain_mv', 'review_chain_source', 'review_chain_target'):
        node.query(f'DROP TABLE IF EXISTS {t} SYNC')
    for t in ('review_chain_source', 'review_chain_target'):
        node.query(f'CREATE TABLE {t} (id UInt64, v UInt64) ENGINE=UpsertMergeTree(v) ORDER BY id UNIQUE KEY id')
    node.query('CREATE MATERIALIZED VIEW review_chain_mv TO review_chain_target AS SELECT id, v FROM review_chain_source')
    # Replayed blocks are filtered at the source. Later blocks must retain their
    # source identity rather than reuse a downstream counter from the first attempt.
    query = "INSERT INTO review_chain_source SELECT number, 1 FROM numbers(12) SETTINGS max_block_size=4, max_threads=1, max_insert_threads=1, min_insert_block_size_rows=0, min_insert_block_size_bytes=0, insert_deduplication_token='chain', deduplicate_blocks_in_dependent_materialized_views=1"
    failing_query = query.replace('number, 1 FROM', 'number, 1 + throwIf(number >= 8) FROM')
    assert 'throwIf' in node.query_and_get_error(failing_query)
    committed = int(node.query('SELECT count() FROM review_chain_source'))
    assert 0 < committed < 12, committed
    before = {int(x) for x in node.query('SELECT id FROM review_chain_target').split()}
    node.query(query)
    node.query(query)
    # Preserve the documented source/view non-atomicity while verifying that the later
    # source blocks do not collide with a token from an earlier forwarded block.
    assert node.query('SELECT count(), sum(id), sum(v) FROM review_chain_source').strip() == '12\t66\t12'
    after = {int(x) for x in node.query('SELECT id FROM review_chain_target').split()}
    assert after == before | set(range(committed, 12)), (before, committed, after)
    for t in ('review_chain_mv', 'review_chain_source', 'review_chain_target'):
        node.query(f'DROP TABLE {t} SYNC')


def test_drop_closes_state_before_removing_directory():
    for _ in range(3):
        node.query('DROP TABLE IF EXISTS review_drop SYNC')
        node.query('''CREATE TABLE review_drop (id UInt64, v UInt64) ENGINE=UpsertMergeTree(v)
            ORDER BY id UNIQUE KEY id SETTINGS upsert_write_buffer_bytes=1048576, upsert_wal_bytes=1048576''')
        node.query('INSERT INTO review_drop SELECT number, 1 FROM numbers(20000)')
        node.query('INSERT INTO review_drop SELECT number, 2 FROM numbers(20000)')
        path = node.query("SELECT arrayElement(data_paths,1) FROM system.tables WHERE name='review_drop'").strip()
        node.query('DROP TABLE review_drop SYNC')
        node.exec_in_container(['test', '!', '-e', path])


def test_parallel_replica_snapshot_is_rejected():
    node.query('DROP TABLE IF EXISTS review_parallel SYNC')
    node.query('CREATE TABLE review_parallel (id UInt64) ENGINE=UpsertMergeTree ORDER BY id UNIQUE KEY id')
    node.query('INSERT INTO review_parallel VALUES (1)')
    for analyzer in (0, 1):
        error = node.query_and_get_error('SELECT sum(id) FROM review_parallel', settings={
            'allow_experimental_analyzer': analyzer,
            'allow_experimental_parallel_reading_from_replicas': 2,
            'parallel_replicas_only_with_analyzer': 0,
            'max_parallel_replicas': 2,
            'parallel_replicas_for_non_replicated_merge_tree': 1,
            'cluster_for_parallel_replicas': 'review_cluster',
        })
        assert 'UpsertMergeTree does not support parallel replica snapshots' in error
    assert node.query('SELECT sum(id) FROM review_parallel').strip() == '1'
    node.query('DROP TABLE review_parallel SYNC')


def test_failed_create_resource_validation_can_retry():
    node.query('DROP DATABASE IF EXISTS review_ordinary SYNC')
    node.query('CREATE DATABASE review_ordinary ENGINE=Ordinary', settings={'allow_deprecated_database_ordinary': 1})
    definition = 'CREATE TABLE review_ordinary.retry (id UInt64) ENGINE=UpsertMergeTree ORDER BY id UNIQUE KEY id'
    assert 'upsert_recovery_threads' in node.query_and_get_error(definition + ' SETTINGS upsert_recovery_threads=0')
    node.query(definition)
    node.query('INSERT INTO review_ordinary.retry VALUES (1)')
    assert node.query('SELECT id FROM review_ordinary.retry').strip() == '1'
    node.query('DROP DATABASE review_ordinary SYNC')


def test_attach_committed_rows_survive_deferred_cleanup():
    node.query('DROP TABLE IF EXISTS review_attach SYNC')
    node.query('CREATE TABLE review_attach (id UInt64, v UInt64) ENGINE=UpsertMergeTree(v) ORDER BY id UNIQUE KEY id')
    node.query('INSERT INTO review_attach VALUES (1,1),(2,1)')
    node.query('INSERT INTO review_attach VALUES (1,2)')
    node.query('DELETE FROM review_attach WHERE id=2')
    node.query("ALTER TABLE review_attach DETACH PARTITION ID 'all'")
    node.query("ALTER TABLE review_attach ATTACH PARTITION ID 'all'")
    assert node.query('SELECT id,v FROM review_attach').strip() == '1\t2'
    node.restart_clickhouse()
    assert node.query('SELECT id,v FROM review_attach').strip() == '1\t2'
    node.query('DROP TABLE review_attach SYNC')


def test_sparse_insert_select_key_and_version():
    for t in ('review_sparse_source', 'review_sparse_target'):
        node.query(f'DROP TABLE IF EXISTS {t} SYNC')
    node.query('''CREATE TABLE review_sparse_source (id UInt64, v UInt64) ENGINE=MergeTree ORDER BY id
        SETTINGS ratio_of_defaults_for_sparse_serialization=0.1''')
    node.query('CREATE TABLE review_sparse_target (id UInt64, v UInt64) ENGINE=UpsertMergeTree(v) ORDER BY id UNIQUE KEY id')
    node.query('INSERT INTO review_sparse_source SELECT number, if(number%100=0, 2, 0) FROM numbers(10000)')
    node.query('INSERT INTO review_sparse_target SELECT * FROM review_sparse_source')
    assert node.query('SELECT count(),sum(v) FROM review_sparse_target').strip() == '10000\t200'
    for t in ('review_sparse_source', 'review_sparse_target'):
        node.query(f'DROP TABLE {t} SYNC')
