-- Tags: use-rocksdb
SET allow_experimental_unique_key = 1;
SET async_insert = 0;
SET optimize_use_projections = 1;
SET optimize_use_implicit_projections = 1;
SET optimize_trivial_count_query = 1;

CREATE TABLE upsert_snapshot_visible_count
(p UInt64, id UInt64, v UInt64, n Nullable(UInt64))
ENGINE = UpsertMergeTree(v) PARTITION BY p ORDER BY (p, id) UNIQUE KEY (p, id);
SYSTEM STOP MERGES upsert_snapshot_visible_count;
INSERT INTO upsert_snapshot_visible_count VALUES
(1,1,1,10),(1,2,1,NULL),(1,3,1,30),(2,1,1,10),(2,2,1,NULL),(2,3,1,30);
SELECT 'unmasked', count() FROM upsert_snapshot_visible_count;
SELECT 'unmasked scan', count() FROM upsert_snapshot_visible_count SETTINGS optimize_use_projections=0;
SELECT 'unmasked plan', countIf(position(explain, '_upsert_visible_count') > 0) > 0 FROM
(EXPLAIN SELECT count() FROM upsert_snapshot_visible_count);

INSERT INTO upsert_snapshot_visible_count VALUES (1,1,2,NULL),(2,1,3,40),(1,2,0,99);
SELECT 'masked', count() FROM upsert_snapshot_visible_count;
SELECT 'masked scan', count() FROM upsert_snapshot_visible_count SETTINGS optimize_use_projections=0;
SELECT 'masked plan', countIf(position(explain, '_upsert_visible_count') > 0) > 0 FROM
(EXPLAIN SELECT count() FROM upsert_snapshot_visible_count);
SELECT 'nullable', count(n) FROM upsert_snapshot_visible_count;
SELECT 'nullable plan', countIf(position(explain, '_upsert_visible_count') > 0) > 0 FROM
(EXPLAIN SELECT count(n) FROM upsert_snapshot_visible_count);

SELECT 'where', count() FROM upsert_snapshot_visible_count WHERE id != 1;
SELECT 'where plan', countIf(position(explain, '_upsert_visible_count') > 0) > 0 FROM
(EXPLAIN SELECT count() FROM upsert_snapshot_visible_count WHERE id != 1);
SELECT 'prewhere', count() FROM upsert_snapshot_visible_count PREWHERE id = 1;
SELECT 'prewhere plan', countIf(position(explain, '_upsert_visible_count') > 0) > 0 FROM
(EXPLAIN SELECT count() FROM upsert_snapshot_visible_count PREWHERE id = 1);
SELECT 'additional filter', count() FROM upsert_snapshot_visible_count
SETTINGS additional_table_filters={'upsert_snapshot_visible_count':'id != 1'};
SELECT 'additional filter plan', countIf(position(explain, '_upsert_visible_count') > 0) > 0 FROM
(EXPLAIN SELECT count() FROM upsert_snapshot_visible_count
 SETTINGS additional_table_filters={'upsert_snapshot_visible_count':'id != 1'});
SELECT 'final', count() FROM upsert_snapshot_visible_count FINAL;
SELECT 'final plan', countIf(position(explain, '_upsert_visible_count') > 0) > 0 FROM
(EXPLAIN SELECT count() FROM upsert_snapshot_visible_count FINAL);

CREATE ROW POLICY upsert_snapshot_visible_count_policy ON upsert_snapshot_visible_count USING id != 1 TO ALL;
SELECT 'row policy', count() FROM upsert_snapshot_visible_count;
SELECT 'row policy plan', countIf(position(explain, '_upsert_visible_count') > 0) > 0 FROM
(EXPLAIN SELECT count() FROM upsert_snapshot_visible_count);
DROP ROW POLICY upsert_snapshot_visible_count_policy ON upsert_snapshot_visible_count;

SELECT 'trivial disabled', count() FROM upsert_snapshot_visible_count SETTINGS optimize_trivial_count_query=0;
SELECT 'trivial disabled plan', countIf(position(explain, '_upsert_visible_count') > 0) > 0 FROM
(EXPLAIN SELECT count() FROM upsert_snapshot_visible_count SETTINGS optimize_trivial_count_query=0);
SELECT 'projections disabled', count() FROM upsert_snapshot_visible_count SETTINGS optimize_use_projections=0;
SELECT 'projections disabled plan', countIf(position(explain, '_upsert_visible_count') > 0) > 0 FROM
(EXPLAIN SELECT count() FROM upsert_snapshot_visible_count SETTINGS optimize_use_projections=0);
SELECT 'implicit disabled', count() FROM upsert_snapshot_visible_count SETTINGS optimize_use_implicit_projections=0;
SELECT 'implicit disabled plan', countIf(position(explain, '_upsert_visible_count') > 0) > 0 FROM
(EXPLAIN SELECT count() FROM upsert_snapshot_visible_count SETTINGS optimize_use_implicit_projections=0);
SELECT 'forced projection', count() FROM upsert_snapshot_visible_count SETTINGS force_optimize_projection=1;
SELECT count() FROM upsert_snapshot_visible_count SETTINGS max_partitions_to_read=1; -- { serverError TOO_MANY_PARTITIONS }
SELECT count() FROM upsert_snapshot_visible_count SETTINGS max_partitions_to_read=1, optimize_use_projections=0; -- { serverError TOO_MANY_PARTITIONS }
SELECT count() FROM upsert_snapshot_visible_count SETTINGS force_primary_key=1; -- { serverError INDEX_NOT_USED }
SELECT count() FROM upsert_snapshot_visible_count SETTINGS force_primary_key=1, optimize_use_projections=0; -- { serverError INDEX_NOT_USED }

SELECT 'logged', count() FROM upsert_snapshot_visible_count
SETTINGS log_queries=1, log_comment='05336_visible_count_log';
SYSTEM FLUSH LOGS;
SELECT 'query log', arrayExists(x -> position(x, '_upsert_visible_count') > 0, projections)
FROM system.query_log WHERE type='QueryFinish' AND current_database=currentDatabase()
AND log_comment='05336_visible_count_log' ORDER BY event_time_microseconds DESC LIMIT 1;

SET enable_analyzer=0;
SELECT 'old analyzer', count() FROM upsert_snapshot_visible_count;
SELECT 'old analyzer plan', countIf(position(explain, '_upsert_visible_count') > 0) > 0 FROM
(EXPLAIN SELECT count() FROM upsert_snapshot_visible_count);
SET enable_analyzer=1;

DELETE FROM upsert_snapshot_visible_count WHERE p=2 AND id=2;
SELECT 'delete', count() FROM upsert_snapshot_visible_count;
SELECT 'delete scan', count() FROM upsert_snapshot_visible_count SETTINGS optimize_use_projections=0;
SELECT 'delete plan', countIf(position(explain, '_upsert_visible_count') > 0) > 0 FROM
(EXPLAIN SELECT count() FROM upsert_snapshot_visible_count);
SELECT 'subqueries', a.c + b.c FROM
(SELECT count() c FROM upsert_snapshot_visible_count) a CROSS JOIN
(SELECT count() c FROM upsert_snapshot_visible_count) b;

CREATE TABLE upsert_snapshot_hidden_part (id UInt64,v UInt64)
ENGINE=UpsertMergeTree(v) ORDER BY id UNIQUE KEY id;
SYSTEM STOP MERGES upsert_snapshot_hidden_part;
INSERT INTO upsert_snapshot_hidden_part SELECT number,1 FROM numbers(3);
INSERT INTO upsert_snapshot_hidden_part SELECT number,2 FROM numbers(3);
-- The original part is completely masked while the replacement remains visible.
SELECT 'hidden part', count() FROM upsert_snapshot_hidden_part;
SELECT 'hidden part scan', count() FROM upsert_snapshot_hidden_part SETTINGS optimize_use_projections=0;
SELECT 'hidden part plan', countIf(position(explain, '_upsert_visible_count') > 0) > 0 FROM
(EXPLAIN SELECT count() FROM upsert_snapshot_hidden_part);
DELETE FROM upsert_snapshot_hidden_part WHERE 1;
SELECT 'empty', count() FROM upsert_snapshot_hidden_part;
SELECT 'empty scan', count() FROM upsert_snapshot_hidden_part SETTINGS optimize_use_projections=0;
SELECT 'empty plan', countIf(position(explain, '_upsert_visible_count') > 0) > 0 FROM
(EXPLAIN SELECT count() FROM upsert_snapshot_hidden_part);
SELECT 'empty result plan', countIf(position(explain, '_upsert_visible_count') > 0) > 0 FROM
(EXPLAIN SELECT count() FROM upsert_snapshot_hidden_part SETTINGS empty_result_for_aggregation_by_empty_set=1);
SELECT 'empty result rows', count() FROM
(SELECT count() FROM upsert_snapshot_hidden_part SETTINGS empty_result_for_aggregation_by_empty_set=1);
SELECT 'empty result scan rows', count() FROM
(SELECT count() FROM upsert_snapshot_hidden_part SETTINGS empty_result_for_aggregation_by_empty_set=1,optimize_use_projections=0);
SELECT 'constant empty rows', count() FROM
(SELECT count() FROM upsert_snapshot_hidden_part GROUP BY 'k' SETTINGS empty_result_for_aggregation_by_constant_keys_on_empty_set=1);
SELECT 'constant empty scan rows', count() FROM
(SELECT count() FROM upsert_snapshot_hidden_part GROUP BY 'k' SETTINGS empty_result_for_aggregation_by_constant_keys_on_empty_set=1,optimize_use_projections=0);
TRUNCATE TABLE upsert_snapshot_visible_count;
SELECT 'truncate', count() FROM upsert_snapshot_visible_count;
SELECT 'truncate scan', count() FROM upsert_snapshot_visible_count SETTINGS optimize_use_projections=0;
-- Zero physical parts use the normal empty-storage plan.
SELECT 'truncate plan', countIf(position(explain, '_upsert_visible_count') > 0) > 0 FROM
(EXPLAIN SELECT count() FROM upsert_snapshot_visible_count);
DROP TABLE upsert_snapshot_visible_count SYNC;
DROP TABLE upsert_snapshot_hidden_part SYNC;
