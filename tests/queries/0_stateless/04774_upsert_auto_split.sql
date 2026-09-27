SET allow_experimental_unique_key = 1;
SET async_insert = 0;
SET max_threads = 4;
SET max_insert_threads = 4;
SET max_threads_min_free_memory_per_thread = 0;
SET max_insert_threads_min_free_memory_per_thread = 0;
SET max_block_size = 2097152;
SET min_insert_block_size_rows = 2097152;
SET min_insert_block_size_bytes = 0;
SET use_strict_insert_block_limits = 0;

CREATE TABLE upsert_split (p UInt8, id UInt64, ver UInt64, value UInt64, payload String)
ENGINE = UpsertMergeTree(ver) PARTITION BY p ORDER BY value UNIQUE KEY id;
-- A single input exceeds 64 MiB; duplicate keys cross the internal slice boundaries.
INSERT INTO upsert_split SETTINGS insert_deduplication_token = 'split-bytes'
SELECT number % 2, number % 1024, 1, number, repeat(toString(number % 10), 1024) FROM numbers(65536);
SELECT 'bytes', count() = 1024, uniqExact(id) = 1024, countIf(value != id + 64512) = 0,
    countIf(payload != repeat(toString(value % 10), 1024)) = 0 FROM upsert_split;
INSERT INTO upsert_split SELECT number % 2, number, 1, 999999, 'newer' FROM numbers(1024);
INSERT INTO upsert_split SETTINGS insert_deduplication_token = 'split-bytes', max_insert_threads = 1
SELECT number % 2, number % 1024, 1, number, repeat(toString(number % 10), 1024) FROM numbers(65536);
SELECT 'retry', count() = 1024, countIf(value != 999999 OR payload != 'newer') = 0 FROM upsert_split;
DETACH TABLE upsert_split SYNC;
ATTACH TABLE upsert_split;
INSERT INTO upsert_split SETTINGS insert_deduplication_token = 'split-bytes'
SELECT number % 2, number % 1024, 1, number, repeat(toString(number % 10), 1024) FROM numbers(65536);
SELECT 'recovery', count() = 1024, countIf(value != 999999 OR payload != 'newer') = 0 FROM upsert_split;
INSERT INTO upsert_split SETTINGS insert_deduplication_token = 'split-bytes'
SELECT number % 2, number % 1024, 2, number, repeat(toString(number % 10), 1024) FROM numbers(65536); -- { serverError BAD_ARGUMENTS }
SELECT 'conflict', count() = 1024, countIf(value != 999999 OR payload != 'newer') = 0 FROM upsert_split;
DROP TABLE upsert_split SYNC;

CREATE TABLE upsert_split_rows (id UInt64, value UInt64)
ENGINE = UpsertMergeTree ORDER BY value UNIQUE KEY id;
INSERT INTO upsert_split_rows SELECT number % 1024, number FROM numbers(1048579);
SELECT 'rows', count() = 1024, uniqExact(id) = 1024,
    countIf(value != id + intDiv(1048578 - id, 1024) * 1024) = 0 FROM upsert_split_rows;
DROP TABLE upsert_split_rows SYNC;

CREATE TABLE upsert_split_single (id UInt64, payload String)
ENGINE = UpsertMergeTree ORDER BY id UNIQUE KEY id;
INSERT INTO upsert_split_single SELECT 1, repeat(repeat('x', 1024), 32768); -- { serverError LIMIT_EXCEEDED }
INSERT INTO upsert_split_single VALUES (2, 'ok');
SELECT 'single-row', count() = 1, min(id) = 2, min(payload) = 'ok' FROM upsert_split_single;
DROP TABLE upsert_split_single SYNC;

-- Columns fit below 32 MiB, but a versioned million-row sidecar does not fit within 64 MiB.
CREATE TABLE upsert_split_lineage (id UInt64, ver UInt64)
ENGINE = UpsertMergeTree(ver) ORDER BY id UNIQUE KEY id;
INSERT INTO upsert_split_lineage SELECT number, toUInt64(1) FROM numbers(1048576);
SELECT 'lineage', count() = 1048576, uniqExact(id) = 1048576, sum(ver) = 1048576 FROM upsert_split_lineage;
DROP TABLE upsert_split_lineage SYNC;
