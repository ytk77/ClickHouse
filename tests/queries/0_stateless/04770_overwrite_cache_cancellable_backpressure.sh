#!/usr/bin/env bash

set -euo pipefail
CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

python3 - "$CLICKHOUSE_CLIENT" "$CLICKHOUSE_DATABASE" "$CLICKHOUSE_TMP" <<'PY'
import pathlib
import shlex
import subprocess
import sys
import tempfile
import time
import uuid

client = shlex.split(sys.argv[1]) + ['--async_insert=0']
table = sys.argv[2] + '.overwrite_cache_cancellable_backpressure'
failpoint = 'overwrite_cache_pause_before_persist'
run_id = 'overwrite-cache-cancel-' + str(uuid.uuid4())
processes = []


def query(sql, timeout=30):
    return subprocess.check_output(client + ['--query', sql], text=True, timeout=timeout).strip()


def launch(name, sql):
    query_id = run_id + '-' + name
    output = open(work / name, 'w+')
    process = subprocess.Popen(client + ['--query_id', query_id, '--query', sql], stdout=output, stderr=output)
    processes.append((process, output))
    return query_id, process, output


def wait_for_event(item, event):
    query_id, process, output = item
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        assert process.poll() is None, f'{query_id} exited before {event}'
        count = query(f"SELECT count() FROM system.processes WHERE query_id = '{query_id}' AND ProfileEvents['{event}'] > 0")
        if count == '1':
            return
        time.sleep(0.02)
    detail = query(f"SELECT query_id, mapFilter((k,v) -> startsWith(k, 'OverwriteCache'), ProfileEvents) FROM system.processes WHERE startsWith(query_id, '{run_id}')")
    raise AssertionError(f'{query_id} did not reach {event}: {detail}')


def finish(item, cancelled=False):
    query_id, process, output = item
    code = process.wait(timeout=10)
    output.seek(0)
    text = output.read()
    if cancelled:
        assert code == 0 or 'QUERY_WAS_CANCELLED' in text, text
    else:
        assert code == 0, text
    assert query(f"SELECT count() FROM system.processes WHERE query_id = '{query_id}'") == '0'


def cancel(item):
    query(f"KILL QUERY WHERE query_id = '{item[0]}' SYNC", timeout=10)
    finish(item, cancelled=True)


def create(mode):
    query(f"""CREATE TABLE {table} (key UInt64, bucket UInt8, version UInt64, payload String)
        ENGINE = OverwriteCache(version) KEYS (key, bucket) INDEX (bucket)
        SETTINGS persist_mode = '{mode}', max_memory_bytes = 2147483648,
        max_pending_insert_bytes = 1073741824, max_concurrent_insert_preparations = 2,
        background_compaction_min_interval_ms = 3600000""")


with tempfile.TemporaryDirectory(prefix='overwrite-cache-cancel-', dir=sys.argv[3]) as directory:
    work = pathlib.Path(directory)
    try:
        create('async')
        query(f'SYSTEM ENABLE FAILPOINT {failpoint}')
        # One oversized block occupies the entire persistence budget, including the active write.
        query(f"INSERT INTO {table} SELECT 0, 1, 1, materialize(repeat(repeat('x', 1024), 270 * 1024))")
        query(f'SYSTEM WAIT FAILPOINT {failpoint} PAUSE')

        head = launch('queue', f"INSERT INTO {table} SELECT number + 1, 1, 2, 'cancelled' FROM numbers(2)")
        wait_for_event(head, 'OverwriteCachePersistenceQueueWaits')
        assert query(f'SELECT count() FROM {table} WHERE bucket = 1') == '1'
        middle = launch('publication', f"INSERT INTO {table} VALUES (3, 1, 1, 'cancelled')")
        wait_for_event(middle, 'OverwriteCachePublicationWaits')
        admission = launch('admission', f"INSERT INTO {table} VALUES (4, 1, 1, 'cancelled')")
        wait_for_event(admission, 'OverwriteCacheInsertAdmissionWaits')

        # Cancel a later ticket while its predecessor is still blocked. No writer progress is needed.
        cancel(admission)
        cancel(middle)
        cancel(head)
        print('admission, publication and queue cancellation completed while persistence was paused')
        assert query(f'SELECT count() FROM {table} WHERE bucket = 1') == '1'

        deletion = launch('delete', f'DELETE FROM {table} WHERE key = 0 AND bucket = 1')
        wait_for_event(deletion, 'OverwriteCachePersistenceQueueWaits')
        cancel(deletion)
        assert query(f'SELECT count() FROM {table} WHERE bucket = 1') == '1'
        print('cancelled deletion rolled back')

        survivor = launch('survivor', f"INSERT INTO {table} VALUES (5, 1, 1, 'survived')")
        wait_for_event(survivor, 'OverwriteCachePersistenceQueueWaits')
        query(f'SYSTEM NOTIFY FAILPOINT {failpoint}')
        finish(survivor)
        query(f'DETACH TABLE {table}')
        query(f'ATTACH TABLE {table}')
        assert query(f'SELECT count(), sum(key) FROM {table} WHERE bucket = 1') == '2\t5'
        print('later publication and replay preserved exactly the accepted rows')
        query(f'DROP TABLE {table} SYNC')

        create('sync')
        query(f'SYSTEM ENABLE FAILPOINT {failpoint}')
        durable = launch('durable', f"INSERT INTO {table} VALUES (7, 1, 1, 'accepted')")
        query(f'SYSTEM WAIT FAILPOINT {failpoint} PAUSE')
        wait_for_event(durable, 'OverwriteCachePersistenceDurableWaits')
        cancel(durable)
        assert query(f'SELECT count() FROM {table} WHERE key = 7 AND bucket = 1') == '1'
        query(f'SYSTEM NOTIFY FAILPOINT {failpoint}')
        query(f'DETACH TABLE {table}')
        query(f'ATTACH TABLE {table}')
        assert query(f'SELECT payload FROM {table} WHERE key = 7 AND bucket = 1') == 'accepted'
        print('durable wait cancellation preserved the accepted commit')
    finally:
        query(f'SYSTEM DISABLE FAILPOINT {failpoint}')
        query(f"KILL QUERY WHERE startsWith(query_id, '{run_id}') ASYNC")
        for process, output in processes:
            try:
                process.wait(timeout=30)
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait()
                output.close()
        query(f'DROP TABLE IF EXISTS {table} SYNC')
PY
