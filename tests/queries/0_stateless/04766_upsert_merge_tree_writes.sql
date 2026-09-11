SET allow_experimental_unique_key = 1;
SET async_insert = 0;

CREATE TABLE upsert_writes (id UInt64, value String, items Array(UInt64))
ENGINE = UpsertMergeTree ORDER BY (value, id) UNIQUE KEY id
SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO upsert_writes VALUES (1,'first',[1]),(1,'last',[2]),(2,'untouched',[3]);
INSERT INTO upsert_writes VALUES (1,'replacement',[4,5]),(3,'new',[6]);
SELECT id,value,items FROM upsert_writes ORDER BY id;
SELECT count(),uniqExact(id) FROM upsert_writes;
SELECT id,value FROM upsert_writes FINAL ORDER BY id;
SELECT count() FROM upsert_writes PREWHERE value = 'last';
SELECT count() FROM upsert_writes PREWHERE value = 'replacement';
SELECT id FROM upsert_writes PREWHERE 1 WHERE has(items,5);
DETACH TABLE upsert_writes SYNC;
ATTACH TABLE upsert_writes;
SELECT id,value,items FROM upsert_writes ORDER BY id;

CREATE TABLE upsert_versions (id UInt64, ver UInt64, value String)
ENGINE = UpsertMergeTree(ver) ORDER BY value UNIQUE KEY id
SETTINGS min_rows_for_wide_part = 1000000, min_bytes_for_wide_part = 1000000000;
INSERT INTO upsert_versions VALUES (1,10,'initial'),(2,1,'untouched');
INSERT INTO upsert_versions VALUES (1,11,'higher');
INSERT INTO upsert_versions VALUES (1,9,'stale');
INSERT INTO upsert_versions VALUES (1,11,'tie'),(1,11,'tie_later'),(1,8,'stale_in_block');
SELECT id,ver,value FROM upsert_versions ORDER BY id;
SELECT count() FROM upsert_versions WHERE value LIKE 'stale%';
SELECT count() FROM upsert_versions FINAL;
DETACH TABLE upsert_versions SYNC;
ATTACH TABLE upsert_versions;
SELECT id,ver,value FROM upsert_versions ORDER BY id;

CREATE TABLE upsert_tokens (id String, value UInt64) ENGINE=UpsertMergeTree ORDER BY value UNIQUE KEY id;
INSERT INTO upsert_tokens SETTINGS insert_deduplication_token='stable-unit' VALUES ('key',10);
INSERT INTO upsert_tokens VALUES ('key',20);
INSERT INTO upsert_tokens SETTINGS insert_deduplication_token='stable-unit' VALUES ('key',10);
SELECT * FROM upsert_tokens;
INSERT INTO upsert_tokens SETTINGS insert_deduplication_token='stable-unit' VALUES ('key',30); -- { serverError BAD_ARGUMENTS }
SELECT * FROM upsert_tokens;
DETACH TABLE upsert_tokens SYNC;
ATTACH TABLE upsert_tokens;
INSERT INTO upsert_tokens SETTINGS insert_deduplication_token='stable-unit' VALUES ('key',10);
SELECT * FROM upsert_tokens;

CREATE TABLE upsert_partitions (tenant UInt8, id UInt64, value UInt64)
ENGINE=UpsertMergeTree PARTITION BY tenant ORDER BY value UNIQUE KEY id;
INSERT INTO upsert_partitions VALUES (1,1,10),(2,1,20),(1,1,11);
INSERT INTO upsert_partitions VALUES (2,1,21);
SELECT * FROM upsert_partitions ORDER BY tenant;

SYSTEM RELOAD ASYNCHRONOUS METRICS;

DROP TABLE upsert_writes SYNC;
DROP TABLE upsert_versions SYNC;
DROP TABLE upsert_tokens SYNC;
DROP TABLE upsert_partitions SYNC;
