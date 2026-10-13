-- Tags: use-rocksdb
SET allow_experimental_unique_key=1;
SET async_insert=0;
SET max_threads=4;
SET optimize_use_projections=0;

CREATE TABLE upsert_filtered_aggregation
(id UInt64, v UInt64, amount Int64, n Nullable(Int64), INDEX iv v TYPE minmax GRANULARITY 1)
ENGINE=UpsertMergeTree(v) PARTITION BY id % 2 ORDER BY id UNIQUE KEY id
SETTINGS index_granularity=4, index_granularity_bytes=0;
SYSTEM STOP MERGES upsert_filtered_aggregation;
INSERT INTO upsert_filtered_aggregation SELECT number,1,number,if(number%3=0,NULL,toInt64(number)) FROM numbers(32);
INSERT INTO upsert_filtered_aggregation SELECT number,2,number*10,if(number%3=0,NULL,toInt64(number*10)) FROM numbers(32);
INSERT INTO upsert_filtered_aggregation SELECT number,3,number*100,if(number%3=0,NULL,toInt64(number*100)) FROM numbers(4);

-- Two original parts are fully hidden; two replacements mix hidden and visible granules.
SELECT 'filtered sum',sum(amount) FROM upsert_filtered_aggregation WHERE amount>=0
SETTINGS log_queries=1,log_comment='05337_filtered_sum';
SYSTEM FLUSH LOGS;
SELECT 'pruned',ProfileEvents['UpsertHiddenPartsSkipped']=2,length(partitions)=2,
ProfileEvents['UpsertMaskReadBytes']=0
FROM system.query_log WHERE type='QueryFinish' AND current_database=currentDatabase()
AND log_comment='05337_filtered_sum' ORDER BY event_time_microseconds DESC LIMIT 1;
SELECT 'grouped',id%2 AS g,sum(amount),sum(n) FROM upsert_filtered_aggregation WHERE amount>=0 GROUP BY g ORDER BY g;
SELECT 'prewhere',sum(amount),sum(n) FROM upsert_filtered_aggregation PREWHERE id>=16 SETTINGS max_block_size=3;
SELECT 'mixed granules',sum(amount),sum(n) FROM upsert_filtered_aggregation WHERE id<12 SETTINGS max_block_size=3;
SELECT 'ordered',groupArray(id) FROM (SELECT id FROM upsert_filtered_aggregation ORDER BY id LIMIT 5);
SELECT 'lazy',count(),uniqExact(id) FROM (SELECT id FROM upsert_filtered_aggregation LIMIT 3 SETTINGS max_threads=1)
SETTINGS log_queries=1,log_comment='05337_lazy';
SYSTEM FLUSH LOGS;
-- Object-storage runs use the eager remote pool; only local parts enable the lazy route.
WITH (SELECT min(is_remote)=0 FROM system.disks WHERE name IN
    (SELECT disk_name FROM system.parts WHERE database=currentDatabase() AND table='upsert_filtered_aggregation' AND active)) AS has_local
SELECT 'lazy prepared',(ProfileEvents['UpsertHiddenPartsSkipped']>0)=has_local,
(ProfileEvents['UpsertLazyReadPartsPrepared']=1)=has_local
FROM system.query_log WHERE type='QueryFinish' AND current_database=currentDatabase()
AND log_comment='05337_lazy' ORDER BY event_time_microseconds DESC LIMIT 1;
SELECT 'final',sum(amount) FROM upsert_filtered_aggregation FINAL WHERE amount>=0
SETTINGS log_queries=1,log_comment='05337_final';
SYSTEM FLUSH LOGS;
SELECT 'final gate',ProfileEvents['UpsertHiddenPartsSkipped']=0
FROM system.query_log WHERE type='QueryFinish' AND current_database=currentDatabase()
AND log_comment='05337_final' ORDER BY event_time_microseconds DESC LIMIT 1;
SELECT 'physical limit',sum(amount) FROM upsert_filtered_aggregation WHERE id>=0 SETTINGS max_rows_to_read=68;
SELECT sum(amount) FROM upsert_filtered_aggregation WHERE id>=0 SETTINGS max_rows_to_read=67; -- { serverError TOO_MANY_ROWS }
SELECT sum(amount) FROM upsert_filtered_aggregation WHERE amount>=0 SETTINGS max_partitions_to_read=1; -- { serverError TOO_MANY_PARTITIONS }
SELECT sum(amount) FROM upsert_filtered_aggregation WHERE amount>=0 SETTINGS force_primary_key=1; -- { serverError INDEX_NOT_USED }

CREATE ROW POLICY upsert_filtered_aggregation_policy ON upsert_filtered_aggregation USING id>=16 TO ALL;
SELECT 'policy',sum(amount) FROM upsert_filtered_aggregation WHERE amount>=0;
DROP ROW POLICY upsert_filtered_aggregation_policy ON upsert_filtered_aggregation;
SELECT 'additional filter',sum(amount) FROM upsert_filtered_aggregation WHERE amount>=0
SETTINGS additional_table_filters={'upsert_filtered_aggregation':'id>=16'};
SET enable_analyzer=0;
SELECT 'old analyzer',sum(amount) FROM upsert_filtered_aggregation WHERE amount>=0;
SET enable_analyzer=1;

-- Eager minmax analysis selects only the old fully hidden parts. Pipeline pruning removes all of them.
SELECT 'pruned empty',sum(amount),sum(n),count() FROM upsert_filtered_aggregation WHERE v=1
SETTINGS use_skip_indexes=1,use_skip_indexes_on_data_read=0,log_queries=1,log_comment='05337_pruned_empty';
SYSTEM FLUSH LOGS;
SELECT 'all selected pruned',ProfileEvents['UpsertHiddenPartsSkipped']=2
FROM system.query_log WHERE type='QueryFinish' AND current_database=currentDatabase()
AND log_comment='05337_pruned_empty' ORDER BY event_time_microseconds DESC LIMIT 1;
SELECT 'pruned empty groups',count() FROM
(SELECT id%2,sum(amount) FROM upsert_filtered_aggregation WHERE v=1 GROUP BY id%2
SETTINGS use_skip_indexes=1,use_skip_indexes_on_data_read=0);
SELECT 'pruned empty result',count() FROM
(SELECT sum(amount) FROM upsert_filtered_aggregation WHERE v=1
SETTINGS use_skip_indexes=1,use_skip_indexes_on_data_read=0,empty_result_for_aggregation_by_empty_set=1);
SELECT 'pruned null empty',sum(amount) FROM upsert_filtered_aggregation WHERE v=1
SETTINGS use_skip_indexes=1,use_skip_indexes_on_data_read=0,aggregate_functions_null_for_empty=1;

DELETE FROM upsert_filtered_aggregation WHERE id<4;
SELECT 'deleted selection',sum(amount),sum(n),count() FROM upsert_filtered_aggregation WHERE id<4;
SELECT 'hidden groups',count() FROM (SELECT id%2,sum(amount) FROM upsert_filtered_aggregation WHERE id<4 GROUP BY id%2);
DELETE FROM upsert_filtered_aggregation WHERE 1;
SELECT 'deleted all',sum(amount),sum(n),count() FROM upsert_filtered_aggregation WHERE amount>=0;
SELECT 'empty result',count() FROM (SELECT sum(amount) FROM upsert_filtered_aggregation WHERE amount>=0 SETTINGS empty_result_for_aggregation_by_empty_set=1);
SELECT 'null empty',sum(amount) FROM upsert_filtered_aggregation WHERE amount>=0 SETTINGS aggregate_functions_null_for_empty=1;
SELECT 'deleted groups',count() FROM (SELECT id%2,sum(amount) FROM upsert_filtered_aggregation WHERE amount>=0 GROUP BY id%2);
DROP TABLE upsert_filtered_aggregation;

-- The shared statistics pass-through change also preserves ordinary MergeTree reads.
CREATE TABLE native_filtered_aggregation (id UInt64, amount Int64) ENGINE=MergeTree ORDER BY id;
INSERT INTO native_filtered_aggregation SELECT number,toInt64(number*10) FROM numbers(32);
SELECT 'native',sum(amount) FROM native_filtered_aggregation WHERE id>=16;
DROP TABLE native_filtered_aggregation;

CREATE TABLE upsert_hidden_ranges (id UInt64,v UInt64,amount Int64)
ENGINE=UpsertMergeTree(v) ORDER BY id UNIQUE KEY id SETTINGS index_granularity=4,index_granularity_bytes=0;
SYSTEM STOP MERGES upsert_hidden_ranges;
INSERT INTO upsert_hidden_ranges SELECT number,1,toInt64(number) FROM numbers(32);
INSERT INTO upsert_hidden_ranges SELECT number,2,toInt64(number*10) FROM numbers(12);
-- The part is not fully hidden, but every mark selected by this predicate is hidden.
SELECT 'hidden ranges',sum(amount) FROM upsert_hidden_ranges WHERE id<4 AND v=1
SETTINGS log_queries=1,log_comment='05337_hidden_ranges';
SYSTEM FLUSH LOGS;
SELECT 'range pruned',ProfileEvents['UpsertHiddenPartsSkipped']=1
FROM system.query_log WHERE type='QueryFinish' AND current_database=currentDatabase()
AND log_comment='05337_hidden_ranges' ORDER BY event_time_microseconds DESC LIMIT 1;
SELECT 'surviving ranges',sum(amount) FROM upsert_hidden_ranges WHERE id>=16;
DROP TABLE upsert_hidden_ranges;
