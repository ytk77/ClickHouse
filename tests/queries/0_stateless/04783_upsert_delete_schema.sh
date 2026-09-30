#!/usr/bin/env bash
set -euo pipefail
CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

table="${CLICKHOUSE_DATABASE}.upsert_delete_schema"
cleanup()
{
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS $table SYNC" >/dev/null 2>&1 ||:
}
trap cleanup EXIT
for wide_threshold in 0 10000000; do
    $CLICKHOUSE_CLIENT --allow_experimental_unique_key=1 -q "CREATE TABLE $table
        (id UInt64, ver UInt64, bucket UInt8, value Nullable(Int64), label String)
        ENGINE=UpsertMergeTree(ver) PARTITION BY bucket ORDER BY id UNIQUE KEY id
        SETTINGS min_bytes_for_wide_part=$wide_threshold"
    $CLICKHOUSE_CLIENT --async_insert=0 -q "INSERT INTO $table VALUES
        (1,10,1,11,'old'),(2,10,1,NULL,'two'),(3,10,2,33,'three'),(4,10,2,44,'four')"
    $CLICKHOUSE_CLIENT --async_insert=0 -q "INSERT INTO $table VALUES (1,11,1,12,'new')"
    $CLICKHOUSE_CLIENT -q "ALTER TABLE $table ADD COLUMN extra Int64 DEFAULT ifNull(value, 0) + 100 AFTER value"
    $CLICKHOUSE_CLIENT -q "ALTER TABLE $table ADD COLUMN doubled Int64 MATERIALIZED extra * 2, ADD COLUMN alias_value Int64 ALIAS extra + 1"
    $CLICKHOUSE_CLIENT -q "SELECT id,extra,doubled,alias_value FROM $table ORDER BY id"
    $CLICKHOUSE_CLIENT -q "ALTER TABLE $table ADD COLUMN IF NOT EXISTS extra UInt8, DROP COLUMN IF EXISTS absent"
    $CLICKHOUSE_CLIENT -q "ALTER TABLE $table ADD INDEX extra_idx extra TYPE minmax GRANULARITY 1"
    $CLICKHOUSE_CLIENT -q "ALTER TABLE $table MATERIALIZE INDEX extra_idx"
    $CLICKHOUSE_CLIENT -q "SELECT id FROM $table WHERE extra > 130 ORDER BY id SETTINGS force_data_skipping_indices='extra_idx'"
    $CLICKHOUSE_CLIENT -q "DELETE FROM $table IN PARTITION 1 WHERE isNull(value) OR alias_value = 113"
    $CLICKHOUSE_CLIENT -q "SELECT id FROM $table ORDER BY id"
    $CLICKHOUSE_CLIENT -q "ALTER TABLE $table DELETE WHERE id IN (SELECT toUInt64(3))"
    $CLICKHOUSE_CLIENT -q "DELETE FROM $table WHERE value < 0"
    $CLICKHOUSE_CLIENT -q "ALTER TABLE $table DROP INDEX extra_idx, DROP COLUMN doubled, DROP COLUMN alias_value, DROP COLUMN extra, DROP COLUMN label"
    $CLICKHOUSE_CLIENT -q "ALTER TABLE $table DROP INDEX IF EXISTS extra_idx"
    $CLICKHOUSE_CLIENT -q "ALTER TABLE $table ADD COLUMN label String DEFAULT 'fresh', ADD COLUMN extra String DEFAULT 'reset'"
    $CLICKHOUSE_CLIENT -q "SELECT id,label,extra FROM $table ORDER BY id"
    $CLICKHOUSE_CLIENT -q "ALTER TABLE $table ADD INDEX extra_idx label TYPE bloom_filter GRANULARITY 1"
    $CLICKHOUSE_CLIENT -q "ALTER TABLE $table MATERIALIZE INDEX extra_idx IN PARTITION 2"
    $CLICKHOUSE_CLIENT -q "SELECT id FROM $table WHERE label = 'fresh' SETTINGS force_data_skipping_indices='extra_idx'"
    $CLICKHOUSE_CLIENT -q "DETACH TABLE $table SYNC"
    $CLICKHOUSE_CLIENT -q "ATTACH TABLE $table"
    $CLICKHOUSE_CLIENT -q "SELECT id,label,extra FROM $table ORDER BY id"
    $CLICKHOUSE_CLIENT --async_insert=0 -q "INSERT INTO $table (id,ver,bucket,value) VALUES (1,1,1,1),(4,1,2,99)"
    $CLICKHOUSE_CLIENT -q "SELECT id,ver,value,label,extra FROM $table ORDER BY id"
    $CLICKHOUSE_CLIENT -q "DELETE FROM $table WHERE 1"
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM $table"
    $CLICKHOUSE_CLIENT -q "DETACH TABLE $table SYNC"
    $CLICKHOUSE_CLIENT -q "ATTACH TABLE $table"
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM $table"
    $CLICKHOUSE_CLIENT -q "DROP TABLE $table SYNC"
done
