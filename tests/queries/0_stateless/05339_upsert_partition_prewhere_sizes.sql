-- Tags: use-rocksdb
SET allow_experimental_unique_key=1;
SET async_insert=0;
CREATE TABLE upsert_partition_prewhere_sizes
(p UInt64,id UInt64,v UInt64,x String CODEC(NONE),y String CODEC(NONE))
ENGINE=UpsertMergeTree(v) PARTITION BY p ORDER BY (p,id) UNIQUE KEY (p,id)
SETTINGS min_bytes_for_wide_part=0,min_rows_for_wide_part=0;
SYSTEM STOP MERGES upsert_partition_prewhere_sizes;
-- The selected partition has a small `x` and large `y`; the other partition reverses their total sizes.
INSERT INTO upsert_partition_prewhere_sizes SELECT 1,number,1,'x',repeat('y',1000) FROM numbers(512);
INSERT INTO upsert_partition_prewhere_sizes SELECT 2,number,1,repeat('x',10000),'y' FROM numbers(512);
SELECT count() FROM upsert_partition_prewhere_sizes WHERE p=1 AND x='missing' AND y='missing'
SETTINGS optimize_move_to_prewhere=1,move_primary_key_columns_to_end_of_prewhere=1,
allow_reorder_prewhere_conditions=1,enable_multiple_prewhere_read_steps=1,move_all_conditions_to_prewhere=1,
query_plan_enable_optimizations=1,use_partition_pruning=1,use_query_cache=0,use_query_condition_cache=0,
use_statistics=0,log_queries=1,log_comment='05339_partition_sizes';
SYSTEM FLUSH LOGS;
-- Reject rows using the selected partition's small column before reading its large column.
SELECT read_bytes<100000 FROM system.query_log
WHERE type='QueryFinish' AND current_database=currentDatabase() AND log_comment='05339_partition_sizes'
ORDER BY event_time_microseconds DESC LIMIT 1;
DROP TABLE upsert_partition_prewhere_sizes;
