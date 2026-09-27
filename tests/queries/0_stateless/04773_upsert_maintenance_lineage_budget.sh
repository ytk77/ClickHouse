#!/usr/bin/env bash
set -euo pipefail
CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

table="${CLICKHOUSE_DATABASE}.upsert_maintenance_lineage_budget"
cleanup()
{
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS $table SYNC" >/dev/null 2>&1 ||:
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --allow_experimental_unique_key=1 -q "CREATE TABLE $table
    (id UInt64, ver UInt64) ENGINE=UpsertMergeTree(ver) ORDER BY id UNIQUE KEY id
    SETTINGS min_bytes_for_wide_part=0"

# Both inputs fit; their combined encoded lineage exceeds 64 MiB despite only 16 MiB of column data.
for start in 0 524288; do
    $CLICKHOUSE_CLIENT --async_insert=0 -q "INSERT INTO $table SELECT number, toUInt64(1)
        FROM numbers($start, 524288) SETTINGS max_threads=1, max_insert_threads=1,
        max_block_size=524288, min_insert_block_size_rows=524288, min_insert_block_size_bytes=0"
done

# Later small parts must still consolidate instead of waiting behind an impossible candidate.
for id in {1048576..1048583}; do
    $CLICKHOUSE_CLIENT --async_insert=0 -q "INSERT INTO $table VALUES ($id,1)"
done

deadline=$((SECONDS + 60))
while :; do
    parts=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.parts
        WHERE database='$CLICKHOUSE_DATABASE' AND table='upsert_maintenance_lineage_budget' AND active")
    if [[ "$parts" == 3 ]]; then
        break
    fi
    if (( SECONDS >= deadline )); then
        echo "Maintenance did not consolidate the small parts: $parts active parts" >&2
        exit 1
    fi
    sleep 0.2
done

$CLICKHOUSE_CLIENT -q "SELECT count(), min(id), max(id), sum(ver) FROM $table"
echo "small parts consolidated"

# A target-sized part with dead rows must remain eligible for reclamation.
$CLICKHOUSE_CLIENT --async_insert=0 -q "INSERT INTO $table SELECT number, toUInt64(2)
    FROM numbers(262144) SETTINGS max_threads=1, max_insert_threads=1,
    max_block_size=262144, min_insert_block_size_rows=262144, min_insert_block_size_bytes=0"

deadline=$((SECONDS + 60))
while :; do
    rows=$($CLICKHOUSE_CLIENT -q "SELECT sum(rows) FROM system.parts
        WHERE database='$CLICKHOUSE_DATABASE' AND table='upsert_maintenance_lineage_budget' AND active")
    if [[ "$rows" == 1048584 ]]; then
        break
    fi
    if (( SECONDS >= deadline )); then
        echo "Maintenance did not reclaim superseded rows: $rows physical rows" >&2
        exit 1
    fi
    sleep 0.2
done
$CLICKHOUSE_CLIENT -q "SELECT count(), min(id), max(id), sum(ver) FROM $table"
echo "superseded rows reclaimed"
