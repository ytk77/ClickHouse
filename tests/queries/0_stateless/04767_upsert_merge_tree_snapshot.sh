#!/usr/bin/env bash
# Tags: no-parallel
# The process-wide failpoint pauses snapshots for every `UpsertMergeTree` table.
set -euo pipefail
CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

table="${CLICKHOUSE_DATABASE}.upsert_snapshot"
reader_pid=""
reader_output=$(mktemp "$CLICKHOUSE_TMP/upsert-snapshot-XXXXXX")
cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM NOTIFY FAILPOINT upsert_pause_after_read_snapshot" >/dev/null 2>&1 ||:
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT upsert_pause_after_read_snapshot" >/dev/null 2>&1 ||:
    if [[ -n "$reader_pid" ]]; then
        wait "$reader_pid" >/dev/null 2>&1 ||:
    fi
    rm -f "$reader_output"
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS $table SYNC" >/dev/null 2>&1 ||:
}
trap cleanup EXIT
$CLICKHOUSE_CLIENT --allow_experimental_unique_key=1 -q "CREATE TABLE $table (id UInt64, value UInt64) ENGINE=UpsertMergeTree ORDER BY value UNIQUE KEY id"
$CLICKHOUSE_CLIENT --async_insert=0 -q "INSERT INTO $table VALUES (1,10),(2,20)"
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT upsert_pause_after_read_snapshot"
$CLICKHOUSE_CLIENT -q "SELECT 'old', id, value FROM $table ORDER BY id" > "$reader_output" &
reader_pid=$!
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT upsert_pause_after_read_snapshot PAUSE"
$CLICKHOUSE_CLIENT --async_insert=0 -q "INSERT INTO $table VALUES (1,30),(3,40)"
$CLICKHOUSE_CLIENT -q "SYSTEM NOTIFY FAILPOINT upsert_pause_after_read_snapshot"
wait "$reader_pid"
reader_pid=""
cat "$reader_output"
$CLICKHOUSE_CLIENT -q "SELECT 'new', id, value FROM $table ORDER BY id"
