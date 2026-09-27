SET allow_experimental_unique_key = 1;
SET allow_experimental_alias_table_engine = 1;
SET async_insert = 0;

CREATE TABLE upsert_async (id UInt64, v UInt64)
ENGINE = UpsertMergeTree ORDER BY id UNIQUE KEY id;
INSERT INTO upsert_async SETTINGS async_insert = 1, wait_for_async_insert = 0 VALUES (1,10); -- { serverError SUPPORT_IS_DISABLED }
INSERT INTO upsert_async SETTINGS async_insert = 1, wait_for_async_insert = 1 VALUES (2,20); -- { serverError SUPPORT_IS_DISABLED }
CREATE TABLE upsert_async_alias ENGINE = Alias('upsert_async');
INSERT INTO upsert_async_alias SETTINGS async_insert = 1, wait_for_async_insert = 0 VALUES (3,50); -- { serverError SUPPORT_IS_DISABLED }
DROP TABLE upsert_async_alias SYNC;
INSERT INTO upsert_async VALUES (1,30),(2,40);
SELECT * FROM upsert_async ORDER BY id;
DETACH TABLE upsert_async SYNC;
ATTACH TABLE upsert_async;
SELECT * FROM upsert_async ORDER BY id;
DROP TABLE upsert_async SYNC;

CREATE TABLE upsert_async_setting (id UInt64)
ENGINE = UpsertMergeTree ORDER BY id UNIQUE KEY id SETTINGS async_insert = 1; -- { serverError SUPPORT_IS_DISABLED }
DROP TABLE IF EXISTS upsert_async_setting SYNC;

CREATE TABLE upsert_mv_target (id UInt64, v UInt64) ENGINE = UpsertMergeTree ORDER BY id UNIQUE KEY id;
CREATE TABLE upsert_mv_source (id UInt64, v UInt64) ENGINE = Null;
CREATE TABLE upsert_mv_middle (id UInt64, v UInt64) ENGINE = Null;
CREATE MATERIALIZED VIEW upsert_mv_first TO upsert_mv_middle AS SELECT * FROM upsert_mv_source;
CREATE MATERIALIZED VIEW upsert_mv_last TO upsert_mv_target AS SELECT * FROM upsert_mv_middle;
CREATE TABLE upsert_mv_alias ENGINE = Alias('upsert_mv_source');
INSERT INTO upsert_mv_source SETTINGS async_insert = 1, wait_for_async_insert = 0 VALUES (1,10); -- { serverError SUPPORT_IS_DISABLED }
INSERT INTO upsert_mv_alias SETTINGS async_insert = 1, wait_for_async_insert = 0 VALUES (1,15); -- { serverError SUPPORT_IS_DISABLED }
INSERT INTO upsert_mv_last SETTINGS async_insert = 1, wait_for_async_insert = 0 VALUES (1,20); -- { serverError SUPPORT_IS_DISABLED }
INSERT INTO upsert_mv_source VALUES (1,30);
SELECT * FROM upsert_mv_target ORDER BY id;
CREATE TABLE upsert_async_source (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id SETTINGS async_insert = 1;
CREATE MATERIALIZED VIEW upsert_async_source_mv TO upsert_mv_target AS SELECT * FROM upsert_async_source;
INSERT INTO upsert_async_source SETTINGS async_insert = 0, wait_for_async_insert = 0 VALUES (1,40); -- { serverError SUPPORT_IS_DISABLED }
DROP TABLE upsert_async_source_mv SYNC;
DROP TABLE upsert_async_source SYNC;
DROP TABLE upsert_mv_alias SYNC;
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
