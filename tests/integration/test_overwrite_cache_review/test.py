from concurrent.futures import ThreadPoolExecutor
import uuid
import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance('node', main_configs=['configs/backups.xml'], stay_alive=True)
backup_root = '/var/lib/clickhouse/backups/'

@pytest.fixture(scope='module', autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()

def create(name):
    node.query(f'DROP TABLE IF EXISTS {name} SYNC')
    node.query(f"CREATE TABLE {name} (id UInt64, tag String, v UInt64, value String) ENGINE=OverwriteCache(v) KEYS(id,tag) INDEX(tag) SETTINGS persist_mode='sync'")

def test_atomic_delete_preserves_new_rows_and_dropped_index_reader():
    create('ow_delete')
    node.query("INSERT INTO ow_delete SELECT number,'risk',1,'old' FROM numbers(70000)")
    node.query('SYSTEM ENABLE FAILPOINT overwrite_cache_pause_before_delete_commit')
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            deletion = pool.submit(node.query, "DELETE FROM ow_delete WHERE tag='risk' AND value='old' SETTINGS max_block_size=1024")
            try:
                node.query('SYSTEM WAIT FAILPOINT overwrite_cache_pause_before_delete_commit PAUSE')
                assert node.query("SELECT count() FROM ow_delete WHERE tag='risk'") == '70000\n'
                node.query("INSERT INTO ow_delete VALUES (0,'risk',2,'new')")
                # The mutation still holds its snapshot; dropping its index must not drain it.
                node.query('ALTER TABLE ow_delete DROP INDEX(tag)', timeout=10)
            finally:
                node.query('SYSTEM DISABLE FAILPOINT overwrite_cache_pause_before_delete_commit')
            deletion.result(timeout=30)
        assert node.query("SELECT value FROM ow_delete WHERE id=0 AND tag='risk'") == 'new\n'
        node.restart_clickhouse()
        assert node.query("SELECT value FROM ow_delete WHERE id=0 AND tag='risk'") == 'new\n'
        assert node.query("SELECT total_rows FROM system.tables WHERE name='ow_delete'") == '1\n'
    finally:
        node.query('SYSTEM DISABLE FAILPOINT overwrite_cache_pause_before_delete_commit')
        node.query('DROP TABLE ow_delete SYNC')

def test_row_policy_rejects_lookup_function():
    create('ow_policy')
    node.query("INSERT INTO ow_policy VALUES (1,'risk',1,'one'),(2,'risk',1,'two')")
    node.query('CREATE ROW POLICY ow_policy_filter ON ow_policy USING id=1 TO ALL')
    try:
        assert node.query("SELECT id FROM ow_policy WHERE tag='risk'") == '1\n'
        error = node.query_and_get_error("SELECT overwriteCacheGet('ow_policy','value',toUInt64(2),'risk')")
        assert 'does not support row policies' in error
    finally:
        node.query('DROP ROW POLICY ow_policy_filter ON ow_policy')
        node.query('DROP TABLE ow_policy SYNC')

def test_restore_blocks_insert_and_rejects_nonempty():
    create('ow_restore')
    node.query("INSERT INTO ow_restore VALUES (1,'risk',1,'one')")
    backup = backup_root + str(uuid.uuid4())
    node.query(f"BACKUP TABLE ow_restore TO File('{backup}')")
    error = node.query_and_get_error(f"RESTORE TABLE ow_restore FROM File('{backup}') SETTINGS allow_non_empty_tables=1")
    assert 'requires an empty table' in error
    assert node.query("SELECT value FROM ow_restore WHERE id=1 AND tag='risk'") == 'one\n'
    node.query('TRUNCATE TABLE ow_restore')
    node.query('SYSTEM ENABLE FAILPOINT overwrite_cache_pause_before_restore_install')
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            restore = pool.submit(node.query, f"RESTORE TABLE ow_restore FROM File('{backup}')")
            try:
                node.query('SYSTEM WAIT FAILPOINT overwrite_cache_pause_before_restore_install PAUSE')
                assert 'restore is in progress' in node.query_and_get_error("INSERT INTO ow_restore VALUES (2,'risk',2,'two')")
            finally:
                node.query('SYSTEM DISABLE FAILPOINT overwrite_cache_pause_before_restore_install')
            restore.result(timeout=30)
        node.restart_clickhouse()
        assert node.query("SELECT value FROM ow_restore WHERE id=1 AND tag='risk'") == 'one\n'
        assert node.query("SELECT total_rows FROM system.tables WHERE name='ow_restore'") == '1\n'
    finally:
        node.query('SYSTEM DISABLE FAILPOINT overwrite_cache_pause_before_restore_install')
        node.query('DROP TABLE ow_restore SYNC')

def test_memory_setting_survives_reattach():
    create('ow_memory')
    node.query("INSERT INTO ow_memory VALUES (1,'risk',1,'one')")
    assert 'MEMORY_LIMIT_EXCEEDED' in node.query_and_get_error('ALTER TABLE ow_memory MODIFY SETTING max_memory_bytes=1')
    node.query('ALTER TABLE ow_memory MODIFY SETTING max_memory_bytes=515396075520')
    node.restart_clickhouse()
    assert 'max_memory_bytes = 515396075520' in node.query('SHOW CREATE TABLE ow_memory')
    assert node.query("SELECT value FROM ow_memory WHERE id=1 AND tag='risk'") == 'one\n'
    node.query('DROP TABLE ow_memory SYNC')


def test_restore_invalid_fingerprint_preserves_empty_target():
    create('ow_backup_source')
    node.query("INSERT INTO ow_backup_source VALUES (1,'risk',1,'one')")
    backup = backup_root + str(uuid.uuid4())
    node.query(f"BACKUP TABLE ow_backup_source TO File('{backup}')")
    node.query("CREATE TABLE ow_backup_target (id UInt64, tag String, v UInt32, value String) ENGINE=OverwriteCache(v) KEYS(id,tag) INDEX(tag) SETTINGS persist_mode='sync'")
    try:
        error = node.query_and_get_error(f"RESTORE TABLE ow_backup_source AS ow_backup_target FROM File('{backup}') SETTINGS allow_different_table_def=1")
        assert 'different table definition' in error
        node.query("INSERT INTO ow_backup_target VALUES (2,'risk',2,'still writable')")
        node.restart_clickhouse()
        assert node.query("SELECT value FROM ow_backup_target WHERE id=2 AND tag='risk'") == 'still writable\n'
    finally:
        node.query('DROP TABLE ow_backup_target SYNC')
        node.query('DROP TABLE ow_backup_source SYNC')
