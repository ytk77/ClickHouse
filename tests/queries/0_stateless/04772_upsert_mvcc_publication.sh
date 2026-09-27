#!/usr/bin/env bash
# Tags: no-parallel
# The process-wide failpoints pause commits for every `UpsertMergeTree` table.
set -euo pipefail
CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

table="${CLICKHOUSE_DATABASE}.upsert_mvcc_publication"
writer_pid=""
reader_pid=""
reader_output=$(mktemp "$CLICKHOUSE_TMP/upsert-mvcc-reader-XXXXXX")
cleanup()
{
    for point in upsert_pause_before_state_write upsert_pause_after_state_sync upsert_pause_after_read_snapshot; do
        $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $point" >/dev/null 2>&1 ||:
    done
    if [[ -n "$writer_pid" ]]; then
        wait "$writer_pid" >/dev/null 2>&1 ||:
    fi
    if [[ -n "$reader_pid" ]]; then
        wait "$reader_pid" >/dev/null 2>&1 ||:
    fi
    rm -f "$reader_output"
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS $table SYNC" >/dev/null 2>&1 ||:
}
trap cleanup EXIT

for point in upsert_pause_before_state_write upsert_pause_after_state_sync; do
    $CLICKHOUSE_CLIENT --allow_experimental_unique_key=1 -q "CREATE TABLE $table
        (p UInt8, id UInt64, ver UInt64, value UInt64)
        ENGINE=UpsertMergeTree(ver) PARTITION BY p ORDER BY id UNIQUE KEY id"
    $CLICKHOUSE_CLIENT --async_insert=0 -q "INSERT INTO $table VALUES (1,1,1,10),(2,2,1,20)"

    # Keep a reader pinned across the next commit, independently of new readers during the stalled write.
    $CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT upsert_pause_after_read_snapshot"
    $CLICKHOUSE_CLIENT -q "SELECT 'pinned', p, sum(value) FROM $table GROUP BY p ORDER BY p" > "$reader_output" &
    reader_pid=$!
    $CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT upsert_pause_after_read_snapshot PAUSE"

    $CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT $point"
    $CLICKHOUSE_CLIENT --async_insert=0 -q "INSERT INTO $table VALUES (1,1,2,11)" &
    writer_pid=$!
    $CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT $point PAUSE"

    # The client deadline also bounds the old uninterruptible parts-lock regression.
    timeout 15s $CLICKHOUSE_CLIENT --receive_timeout=10 -q "SELECT 'during', p, sum(value)
        FROM $table GROUP BY p ORDER BY p SETTINGS apply_patch_parts=1"
    timeout 15s $CLICKHOUSE_CLIENT --receive_timeout=10 -q "SELECT 'other partition', sum(value)
        FROM $table WHERE p=2 SETTINGS apply_patch_parts=1"
    timeout 15s $CLICKHOUSE_CLIENT --receive_timeout=10 -q "SELECT 'count during', count() FROM $table"

    $CLICKHOUSE_CLIENT -q "SYSTEM NOTIFY FAILPOINT $point"
    wait "$writer_pid"
    writer_pid=""
    $CLICKHOUSE_CLIENT -q "SELECT 'after', p, sum(value) FROM $table GROUP BY p ORDER BY p"
    $CLICKHOUSE_CLIENT -q "SYSTEM NOTIFY FAILPOINT upsert_pause_after_read_snapshot"
    wait "$reader_pid"
    reader_pid=""
    cat "$reader_output"
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $point"
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT upsert_pause_after_read_snapshot"
    $CLICKHOUSE_CLIENT -q "DROP TABLE $table SYNC"
done
