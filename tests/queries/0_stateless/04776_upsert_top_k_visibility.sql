-- Tags: no-parallel
-- The process-wide failpoint keeps superseded rows in their original parts.
SET allow_experimental_unique_key = 1;
SET async_insert = 0;
SET use_skip_indexes_for_top_k = 1;
SYSTEM ENABLE FAILPOINT upsert_pause_before_merge_commit;

CREATE TABLE upsert_top_k (k UInt64, v UInt64, INDEX iv v TYPE minmax GRANULARITY 1)
ENGINE = UpsertMergeTree ORDER BY k UNIQUE KEY k SETTINGS index_granularity = 1;
INSERT INTO upsert_top_k VALUES (1,1),(2,50),(3,60),(4,200);
INSERT INTO upsert_top_k VALUES (1,100),(4,40);
SELECT 'asc1', * FROM upsert_top_k ORDER BY v LIMIT 1;
SELECT 'asc3', * FROM upsert_top_k ORDER BY v LIMIT 3;
SELECT 'desc1', * FROM upsert_top_k ORDER BY v DESC LIMIT 1;
SELECT 'desc3', * FROM upsert_top_k ORDER BY v DESC LIMIT 3;
SELECT 'final', * FROM upsert_top_k FINAL ORDER BY v LIMIT 3;
SELECT 'filtered', * FROM upsert_top_k WHERE v >= 50 ORDER BY v LIMIT 2;
SELECT 'control', * FROM upsert_top_k ORDER BY v LIMIT 3 SETTINGS use_skip_indexes_for_top_k = 0;

SYSTEM DISABLE FAILPOINT upsert_pause_before_merge_commit;
DROP TABLE upsert_top_k SYNC;
