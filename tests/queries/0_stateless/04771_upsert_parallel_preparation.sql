SET allow_experimental_unique_key = 1;
SET async_insert = 0;
SET max_threads = 4;
SET max_insert_threads = 4;
SET max_block_size = 1024;
SET max_insert_block_size = 1024;
SET min_insert_block_size_rows = 1024;
SET min_insert_block_size_bytes = 0;

CREATE TABLE upsert_parallel (p UInt8, id UInt64, ver UInt64, value UInt64)
ENGINE = UpsertMergeTree(ver) PARTITION BY p ORDER BY value UNIQUE KEY id;
INSERT INTO upsert_parallel SETTINGS insert_deduplication_token = 'parallel-units'
SELECT number % 2, number % 1024, 1, number FROM numbers(16384);
SELECT 'ordered', count() = 1024, uniqExact(id) = 1024, countIf(value != id + 15360) = 0 FROM upsert_parallel;
INSERT INTO upsert_parallel SELECT number % 2, number, 2, 99999 FROM numbers(1024);
INSERT INTO upsert_parallel SETTINGS insert_deduplication_token = 'parallel-units', max_insert_threads = 1
SELECT number % 2, number % 1024, 1, number FROM numbers(16384);
SELECT 'replayed', count() = 1024, min(ver) = 2, min(value) = 99999 FROM upsert_parallel;
DETACH TABLE upsert_parallel SYNC;
ATTACH TABLE upsert_parallel;
INSERT INTO upsert_parallel SETTINGS insert_deduplication_token = 'parallel-units'
SELECT number % 2, number % 1024, 1, number FROM numbers(16384);
SELECT 'recovered', count() = 1024, min(ver) = 2, min(value) = 99999 FROM upsert_parallel;
INSERT INTO upsert_parallel SETTINGS insert_deduplication_token = 'parallel-units'
SELECT number % 2, number % 1024, 3, number FROM numbers(16384); -- { serverError BAD_ARGUMENTS }
SELECT 'conflict', count() = 1024, min(ver) = 2, min(value) = 99999 FROM upsert_parallel;
DROP TABLE upsert_parallel SYNC;

CREATE TABLE upsert_parallel_unversioned (id UInt64, value UInt64)
ENGINE = UpsertMergeTree ORDER BY value UNIQUE KEY id;
INSERT INTO upsert_parallel_unversioned SELECT number % 1024, number FROM numbers(16384);
SELECT 'unversioned', count() = 1024, uniqExact(id) = 1024, countIf(value != id + 15360) = 0 FROM upsert_parallel_unversioned;
DROP TABLE upsert_parallel_unversioned SYNC;
