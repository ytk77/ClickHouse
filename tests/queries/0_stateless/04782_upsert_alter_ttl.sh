#!/usr/bin/env bash
set -euo pipefail
CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

table="${CLICKHOUSE_DATABASE}.upsert_alter_ttl"
cleanup()
{
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS $table SYNC" >/dev/null 2>&1 ||:
}
trap cleanup EXIT
wait_count()
{
    local expected=$1
    local deadline=$((SECONDS + 60))
    local actual
    while :; do
        actual=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM $table")
        if [[ "$actual" == "$expected" ]]; then
            return
        fi
        if (( SECONDS >= deadline )); then
            echo "Expected $expected rows after TTL alteration, got $actual" >&2
            exit 1
        fi
        sleep 0.1
    done
}

for wide_threshold in 0 10000000; do
    $CLICKHOUSE_CLIENT --allow_experimental_unique_key=1 -q "CREATE TABLE $table
        (id UInt64, ver UInt64, expires DateTime, category UInt8)
        ENGINE=UpsertMergeTree(ver) ORDER BY category UNIQUE KEY id
        SETTINGS min_bytes_for_wide_part=$wide_threshold"
    $CLICKHOUSE_CLIENT --async_insert=0 -q "INSERT INTO $table VALUES
        (1,10,'2000-01-01',1),(2,10,'2000-01-01',2),(3,10,'2100-01-01',1)"
    $CLICKHOUSE_CLIENT -q "ALTER TABLE $table MODIFY TTL expires DELETE WHERE category=1"
    wait_count 2
    $CLICKHOUSE_CLIENT -q "SELECT id FROM $table ORDER BY id"

    # The timestamp expression is identical; a changed `WHERE` must still invalidate cached policy information.
    $CLICKHOUSE_CLIENT -q "ALTER TABLE $table MODIFY TTL expires DELETE WHERE category=2"
    wait_count 1
    $CLICKHOUSE_CLIENT -q "SELECT id FROM $table ORDER BY id"

    $CLICKHOUSE_CLIENT -q "ALTER TABLE $table REMOVE TTL"
    $CLICKHOUSE_CLIENT --async_insert=0 -q "INSERT INTO $table VALUES (4,1,'2000-01-01',2)"
    $CLICKHOUSE_CLIENT -q "DETACH TABLE $table SYNC"
    $CLICKHOUSE_CLIENT -q "ATTACH TABLE $table"
    $CLICKHOUSE_CLIENT -q "SELECT id FROM $table ORDER BY id"

    $CLICKHOUSE_CLIENT -q "SYSTEM STOP TTL MERGES $table"
    $CLICKHOUSE_CLIENT -q "ALTER TABLE $table MODIFY TTL expires"
    $CLICKHOUSE_CLIENT -q "ALTER TABLE $table MODIFY TTL expires + INTERVAL 200 YEAR"
    $CLICKHOUSE_CLIENT -q "DETACH TABLE $table SYNC"
    $CLICKHOUSE_CLIENT -q "ATTACH TABLE $table"
    $CLICKHOUSE_CLIENT -q "SELECT id FROM $table ORDER BY id"
    $CLICKHOUSE_CLIENT -q "ALTER TABLE $table MODIFY TTL expires"
    wait_count 1
    $CLICKHOUSE_CLIENT -q "SELECT id FROM $table ORDER BY id"
    cleanup
done
