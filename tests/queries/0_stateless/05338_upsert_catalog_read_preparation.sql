-- Tags: use-rocksdb
SET allow_experimental_unique_key=1;
SET async_insert=0;
CREATE TABLE upsert_catalog_read (p UInt64,id UInt64,v UInt64,value UInt64)
ENGINE=UpsertMergeTree(v) PARTITION BY p ORDER BY (p,id) UNIQUE KEY (p,id);
SYSTEM STOP MERGES upsert_catalog_read;
INSERT INTO upsert_catalog_read SELECT number%16,number,1,number FROM numbers(64);
INSERT INTO upsert_catalog_read SELECT number%16,number,2,number+100 FROM numbers(32);
SELECT 'partition',count(),sum(value) FROM upsert_catalog_read WHERE p=3;
SELECT 'range',count(),sum(value) FROM upsert_catalog_read WHERE p BETWEEN 2 AND 4;
SELECT 'unrelated',count(),sum(value) FROM upsert_catalog_read WHERE id>=60;
SELECT 'empty',count(),sum(value) FROM upsert_catalog_read WHERE p=99;
SELECT 'prewhere',count(),sum(value) FROM upsert_catalog_read PREWHERE p=3;
SELECT 'final',count(),sum(value) FROM upsert_catalog_read FINAL WHERE p=3;
SELECT 'unoptimized',count(),sum(value) FROM upsert_catalog_read WHERE p=3 SETTINGS query_plan_enable_optimizations=0;
SELECT 'additional',count(),sum(value) FROM upsert_catalog_read
SETTINGS additional_table_filters={'upsert_catalog_read':'p = 3'};
SELECT 'disjunction',count(),sum(value) FROM upsert_catalog_read WHERE p=3 OR id=60;
SELECT 'union',sum(c),sum(s) FROM
(SELECT count() c,sum(value) s FROM upsert_catalog_read WHERE p=3
 UNION ALL SELECT count() c,sum(value) s FROM upsert_catalog_read WHERE p=4);
SELECT 'join',count(),sum(t.value) FROM upsert_catalog_read t
INNER JOIN (SELECT toUInt64(3) p) r USING p;
CREATE VIEW upsert_catalog_view AS SELECT * FROM upsert_catalog_read WHERE p BETWEEN 2 AND 4;
SELECT 'view',count(),sum(value) FROM upsert_catalog_view WHERE p=3;
DROP VIEW upsert_catalog_view;
SELECT 'all',count(),sum(value) FROM upsert_catalog_read;
INSERT INTO upsert_catalog_read VALUES (3,3,3,1000),(99,99,1,99);
SELECT 'new snapshot',count(),sum(value) FROM upsert_catalog_read WHERE p=3;
SELECT 'new partition',count(),sum(value) FROM upsert_catalog_read WHERE p=99;
ALTER TABLE upsert_catalog_read DROP PARTITION 99;
SELECT 'removed partition',count() FROM upsert_catalog_read WHERE p=99;
DETACH TABLE upsert_catalog_read;
ATTACH TABLE upsert_catalog_read;
SELECT 'recovered',count(),sum(value) FROM upsert_catalog_read WHERE p=3;
DROP TABLE upsert_catalog_read;

-- Partition substitution still applies to expressions and virtual partition inputs.
CREATE TABLE native_catalog_conditions (p UInt64,id UInt64)
ENGINE=MergeTree PARTITION BY p%4 ORDER BY id;
INSERT INTO native_catalog_conditions SELECT number,number FROM numbers(32);
SELECT 'expression',count(),sum(id) FROM native_catalog_conditions WHERE p%4=2;
SELECT 'virtual',count(),sum(id) FROM native_catalog_conditions WHERE _partition_id='2';
SELECT 'unrelated native',count(),sum(id) FROM native_catalog_conditions WHERE id>=28;
DROP TABLE native_catalog_conditions;
