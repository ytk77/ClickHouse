SET allow_experimental_unique_key = 1;
SET async_insert = 0;
SET max_threads = 4;
SET max_insert_threads = 4;

CREATE TABLE upsert_barrier_wide (id UInt64, ver UInt64, value Nullable(String))
ENGINE = UpsertMergeTree(ver) ORDER BY id UNIQUE KEY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO upsert_barrier_wide VALUES (1, 1, 'old');
SYSTEM ENABLE FAILPOINT upsert_throw_before_payload_sync;
INSERT INTO upsert_barrier_wide SELECT number, 2, toString(number) FROM numbers(32); -- { serverError FAULT_INJECTED }
SYSTEM DISABLE FAILPOINT upsert_throw_before_payload_sync;
SELECT 'wide-failed', count(), min(ver), min(value) FROM upsert_barrier_wide;
INSERT INTO upsert_barrier_wide SELECT number, 2, toString(number) FROM numbers(32);
DETACH TABLE upsert_barrier_wide SYNC;
ATTACH TABLE upsert_barrier_wide;
SELECT 'wide-recovered', count(), uniqExact(id), min(ver), countIf(value != toString(id)) FROM upsert_barrier_wide;
DROP TABLE upsert_barrier_wide SYNC;

CREATE TABLE upsert_barrier_compact (id UInt64, ver UInt64, value Nullable(String))
ENGINE = UpsertMergeTree(ver) ORDER BY id UNIQUE KEY id SETTINGS min_bytes_for_wide_part = 33554432;
INSERT INTO upsert_barrier_compact VALUES (1, 1, 'old');
SYSTEM ENABLE FAILPOINT upsert_throw_before_payload_sync;
INSERT INTO upsert_barrier_compact SELECT number, 2, toString(number) FROM numbers(32); -- { serverError FAULT_INJECTED }
SYSTEM DISABLE FAILPOINT upsert_throw_before_payload_sync;
SELECT 'compact-failed', count(), min(ver), min(value) FROM upsert_barrier_compact;
INSERT INTO upsert_barrier_compact SELECT number, 2, toString(number) FROM numbers(32);
DETACH TABLE upsert_barrier_compact SYNC;
ATTACH TABLE upsert_barrier_compact;
SELECT 'compact-recovered', count(), uniqExact(id), min(ver), countIf(value != toString(id)) FROM upsert_barrier_compact;
DROP TABLE upsert_barrier_compact SYNC;
