SET allow_experimental_unique_key = 1;
SET async_insert = 0;
SET unique_key_max_encoded_size = 512;
SET max_threads = 1;
SET max_block_size = 32768;
SET max_insert_block_size = 32768;
SET min_insert_block_size_rows = 0;
SET min_insert_block_size_bytes = 0;

CREATE TABLE upsert_disk_capacity (id String, ver UInt64, value UInt64)
ENGINE = UpsertMergeTree(ver) ORDER BY id UNIQUE KEY id;
INSERT INTO upsert_disk_capacity
SELECT concat(repeat('x', 256), toString(number)), 10, number FROM numbers(300000);
SELECT count(), uniqExact(id), sum(value) FROM upsert_disk_capacity;
INSERT INTO upsert_disk_capacity VALUES (concat(repeat('x',256),'0'),11,999), (concat(repeat('x',256),'299999'),9,0);
SELECT count(), sum(value) FROM upsert_disk_capacity;
DETACH TABLE upsert_disk_capacity SYNC;
ATTACH TABLE upsert_disk_capacity;
SELECT count(), uniqExact(id), sum(value) FROM upsert_disk_capacity;
SELECT ver,value FROM upsert_disk_capacity WHERE id=concat(repeat('x',256),'0');
SELECT ver,value FROM upsert_disk_capacity WHERE id=concat(repeat('x',256),'299999');

CREATE TABLE upsert_disk_partitions (p UInt64, id UInt64, ver UInt64)
ENGINE = UpsertMergeTree(ver) PARTITION BY p ORDER BY id UNIQUE KEY id;
INSERT INTO upsert_disk_partitions SELECT number, 1, 10 FROM numbers(600)
SETTINGS max_partitions_per_insert_block=600;
INSERT INTO upsert_disk_partitions VALUES (599,1,9),(0,1,11);
SELECT count(), sum(ver) FROM upsert_disk_partitions;
DETACH TABLE upsert_disk_partitions SYNC;
ATTACH TABLE upsert_disk_partitions;
SELECT count(), sum(ver) FROM upsert_disk_partitions;
DROP TABLE upsert_disk_capacity SYNC;
DROP TABLE upsert_disk_partitions SYNC;
