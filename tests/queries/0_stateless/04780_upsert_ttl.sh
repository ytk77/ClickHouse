#!/usr/bin/env bash
set -euo pipefail
CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

table="${CLICKHOUSE_DATABASE}.upsert_ttl"
cleanup()
{
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS $table SYNC" >/dev/null 2>&1 ||:
}
trap cleanup EXIT

wait_rows()
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
            echo "TTL did not reach $expected rows: $actual" >&2
            exit 1
        fi
        sleep 0.1
    done
}

# Exercise both part formats, sorting that differs from the unique key, and partition-local expiration.
for wide_threshold in 0 10000000; do
    $CLICKHOUSE_CLIENT --allow_experimental_unique_key=1 -q "CREATE TABLE $table
        (p UInt8, id UInt64, ver UInt64, expires DateTime, value String)
        ENGINE=UpsertMergeTree(ver) PARTITION BY p ORDER BY value UNIQUE KEY id
        TTL expires DELETE SETTINGS min_bytes_for_wide_part=$wide_threshold"
    $CLICKHOUSE_CLIENT -q "SYSTEM STOP TTL MERGES $table"
    $CLICKHOUSE_CLIENT --async_insert=0 -q "INSERT INTO $table VALUES
        (1,1,10,'2000-01-01','expired'), (1,2,10,'2100-01-01','retained'),
        (2,1,10,'2100-01-01','other partition')"
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM $table"
    $CLICKHOUSE_CLIENT -q "SYSTEM START TTL MERGES $table"
    wait_rows 2
    $CLICKHOUSE_CLIENT -q "SELECT p,id,ver,value FROM $table ORDER BY p,id"
    $CLICKHOUSE_CLIENT -q "DETACH TABLE $table SYNC"
    $CLICKHOUSE_CLIENT -q "ATTACH TABLE $table"
    $CLICKHOUSE_CLIENT -q "SELECT p,id,ver,value FROM $table FINAL ORDER BY p,id"
    # Expiration forgets the winner; a later insert may start this key again at a lower version.
    $CLICKHOUSE_CLIENT --async_insert=0 -q "INSERT INTO $table VALUES (1,1,1,'2100-01-01','reinserted')"
    $CLICKHOUSE_CLIENT -q "SELECT p,id,ver,value FROM $table ORDER BY p,id"
    cleanup
done

# Multiple conditional rules and zero timestamps use the standard `TTL` semantics.
$CLICKHOUSE_CLIENT --allow_experimental_unique_key=1 -q "CREATE TABLE $table
    (id UInt64, expires DateTime64(3), category UInt8)
    ENGINE=UpsertMergeTree ORDER BY id UNIQUE KEY id
    TTL expires DELETE WHERE category=1, expires + INTERVAL 1 DAY DELETE WHERE category=2"
$CLICKHOUSE_CLIENT --async_insert=0 -q "INSERT INTO $table (id,expires,category) VALUES
    (1,'2000-01-01',1), (2,'2000-01-01',2), (3,'2000-01-01',3),
    (4,'2100-01-01',1), (5,'1970-01-01',1)"
wait_rows 3
$CLICKHOUSE_CLIENT -q "SELECT id,category FROM $table ORDER BY id"
cleanup

# A single fully expired part is eligible without another insert or a dead-row threshold.
$CLICKHOUSE_CLIENT --allow_experimental_unique_key=1 -q "CREATE TABLE $table
    (id UInt64, expires Date) ENGINE=UpsertMergeTree ORDER BY id UNIQUE KEY id
    TTL expires + INTERVAL 1 DAY"
$CLICKHOUSE_CLIENT --async_insert=0 -q "INSERT INTO $table VALUES (1,'2000-01-01')"
wait_rows 0
$CLICKHOUSE_CLIENT -q "DETACH TABLE $table SYNC"
$CLICKHOUSE_CLIENT -q "ATTACH TABLE $table"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM $table"
