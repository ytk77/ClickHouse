SET allow_experimental_unique_key = 1;
SET allow_experimental_alias_table_engine = 1;
SET async_insert = 0;

CREATE TABLE upsert_async (id UInt64, v UInt64)
ENGINE = UpsertMergeTree ORDER BY id UNIQUE KEY id;
INSERT INTO upsert_async VALUES (1,30),(2,40);
SELECT * FROM upsert_async ORDER BY id;
DETACH TABLE upsert_async SYNC;
ATTACH TABLE upsert_async;
SELECT * FROM upsert_async ORDER BY id;
DROP TABLE upsert_async SYNC;

CREATE TABLE upsert_mv_target (id UInt64, v UInt64) ENGINE = UpsertMergeTree ORDER BY id UNIQUE KEY id;
CREATE TABLE upsert_mv_source (id UInt64, v UInt64) ENGINE = Null;
CREATE TABLE upsert_mv_middle (id UInt64, v UInt64) ENGINE = Null;
CREATE MATERIALIZED VIEW upsert_mv_first TO upsert_mv_middle AS SELECT * FROM upsert_mv_source;
CREATE MATERIALIZED VIEW upsert_mv_last TO upsert_mv_target AS SELECT * FROM upsert_mv_middle;
INSERT INTO upsert_mv_source VALUES (1,30);
SELECT * FROM upsert_mv_target ORDER BY id;
DROP TABLE upsert_mv_first SYNC;
DROP TABLE upsert_mv_last SYNC;
DROP TABLE upsert_mv_source SYNC;
DROP TABLE upsert_mv_middle SYNC;
DROP TABLE upsert_mv_target SYNC;

CREATE TABLE merge_tree_async (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO merge_tree_async SETTINGS async_insert = 1, wait_for_async_insert = 1 VALUES (7);
SELECT * FROM merge_tree_async;
DROP TABLE merge_tree_async SYNC;

CREATE TABLE async_missing_target (id UInt64) ENGINE = Memory;
CREATE TABLE async_missing_source (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE MATERIALIZED VIEW async_missing_mv TO async_missing_target AS SELECT * FROM async_missing_source;
DROP TABLE async_missing_target SYNC;
INSERT INTO async_missing_source SETTINGS async_insert=1, wait_for_async_insert=1, materialized_views_ignore_errors=1 VALUES (8);
SELECT * FROM async_missing_source;
DROP TABLE async_missing_mv SYNC;
DROP TABLE async_missing_source SYNC;
