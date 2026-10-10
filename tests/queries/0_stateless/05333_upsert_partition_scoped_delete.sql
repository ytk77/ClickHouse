-- Tags: use-rocksdb
SET allow_experimental_unique_key = 1;
SET async_insert = 0;
DROP TABLE IF EXISTS upsert_partition_scoped_delete;
DROP TABLE IF EXISTS direct_delete_partition_guard;
CREATE TABLE upsert_partition_scoped_delete
(p UInt8, id UInt64, v UInt64)
ENGINE=UpsertMergeTree(v) PARTITION BY p ORDER BY id UNIQUE KEY id;
INSERT INTO upsert_partition_scoped_delete VALUES (1,1,10),(1,2,10),(2,1,20),(2,2,20);
DELETE FROM upsert_partition_scoped_delete IN PARTITION 1 WHERE id=1;
SELECT p,id,v FROM upsert_partition_scoped_delete ORDER BY p,id;
DELETE FROM upsert_partition_scoped_delete IN PARTITION 2 WHERE id=2;
SELECT p,id,v FROM upsert_partition_scoped_delete ORDER BY p,id;
DELETE FROM upsert_partition_scoped_delete IN PARTITION 1, 2 WHERE id=2; -- { serverError NOT_IMPLEMENTED }
DELETE FROM upsert_partition_scoped_delete WHERE id=1;
SELECT p,id,v FROM upsert_partition_scoped_delete ORDER BY p,id;
CREATE TABLE direct_delete_partition_guard (id UInt64, v UInt64)
ENGINE=EmbeddedRocksDB PRIMARY KEY id;
INSERT INTO direct_delete_partition_guard VALUES (1,10),(2,20);
DELETE FROM direct_delete_partition_guard IN PARTITION 1 WHERE id=1; -- { serverError NOT_IMPLEMENTED }
SELECT id,v FROM direct_delete_partition_guard ORDER BY id;
DROP TABLE direct_delete_partition_guard;
DROP TABLE upsert_partition_scoped_delete;
