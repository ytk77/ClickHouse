-- Tags: long
SET allow_experimental_unique_key = 1;
SET async_insert = 0;
SET max_threads = 1;
SET max_insert_threads = 1;
SET max_block_size = 1048576;
SET max_insert_block_size = 1048576;
SET min_insert_block_size_rows = 0;
SET min_insert_block_size_bytes = 0;

CREATE TABLE upsert_batch_split (a UInt64, b UInt32, p String, value UInt8)
ENGINE = UpsertMergeTree PARTITION BY p ORDER BY (a,b) UNIQUE KEY (a,b);
INSERT INTO upsert_batch_split SELECT number, 1, 'a', 1 FROM numbers(1048576) SETTINGS insert_deduplication_token='first';
INSERT INTO upsert_batch_split SELECT number, 1, 'a', 2 FROM numbers(1048576) SETTINGS insert_deduplication_token='overwrite';
INSERT INTO upsert_batch_split SELECT number, 1, 'a', 1 FROM numbers(1048576) SETTINGS insert_deduplication_token='first';
SELECT count(), uniqExact((a,b)), min(value), max(value) FROM upsert_batch_split;
DETACH TABLE upsert_batch_split SYNC;
ATTACH TABLE upsert_batch_split;
SELECT count(), sum(a), min(value), max(value) FROM upsert_batch_split;
DROP TABLE upsert_batch_split SYNC;

CREATE TABLE upsert_versioned_batch_split (a UInt64, b UInt32, p String, ver UInt64)
ENGINE = UpsertMergeTree(ver) PARTITION BY p ORDER BY (a,b) UNIQUE KEY (a,b);
INSERT INTO upsert_versioned_batch_split SELECT number, 1, 'a', 1 FROM numbers(950000);
INSERT INTO upsert_versioned_batch_split SELECT number, 1, 'a', 2 FROM numbers(950000);
SELECT count(), uniqExact((a,b)), min(ver), max(ver) FROM upsert_versioned_batch_split;
DROP TABLE upsert_versioned_batch_split SYNC;

-- Seed exactly the first partition of an old partially committed unit. Its original ordinal is zero.
CREATE TABLE upsert_partial_batch (a UInt64, b UInt32, p String, value UInt8)
ENGINE = UpsertMergeTree PARTITION BY p ORDER BY (a,b) UNIQUE KEY (a,b);
INSERT INTO upsert_partial_batch SELECT toUInt64(0), toUInt32(1), 'a', toUInt8(1) SETTINGS insert_deduplication_token='partial';
INSERT INTO upsert_partial_batch VALUES (0,1,'a',2);
INSERT INTO upsert_partial_batch SELECT number, 1, if(number=0, 'a', 'b'), 1 FROM numbers(1048576) SETTINGS insert_deduplication_token='partial';
INSERT INTO upsert_partial_batch VALUES (1,1,'b',2);
INSERT INTO upsert_partial_batch SELECT number, 1, if(number=0, 'a', 'b'), 1 FROM numbers(1048576) SETTINGS insert_deduplication_token='partial';
SELECT count(), uniqExact((a,b)), countIf(value=2) FROM upsert_partial_batch;
DROP TABLE upsert_partial_batch SYNC;

CREATE TABLE upsert_unchanged_batch (a UInt64, p UInt32)
ENGINE = UpsertMergeTree PARTITION BY p ORDER BY a UNIQUE KEY a;
INSERT INTO upsert_unchanged_batch SELECT number, toUInt32(202609) FROM numbers(1048576);
SELECT count(), sum(rows), min(min_block_number), max(max_block_number) FROM system.parts
WHERE database=currentDatabase() AND table='upsert_unchanged_batch' AND active;
DROP TABLE upsert_unchanged_batch SYNC;
