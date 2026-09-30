SET allow_experimental_unique_key = 1;

CREATE TABLE upsert_contract
(
    tenant UInt64,
    id UInt64,
    ver UInt64,
    value String DEFAULT 'default'
)
ENGINE = UpsertMergeTree(ver)
PARTITION BY tenant
ORDER BY (tenant, value)
UNIQUE KEY (tenant, id);

SELECT engine, startsWith(engine_full, 'UpsertMergeTree(ver)'), partition_key, sorting_key, unique_key
FROM system.tables WHERE database = currentDatabase() AND name = 'upsert_contract';
SELECT count() FROM upsert_contract;
SELECT count() FROM upsert_contract FINAL;
SELECT _part FROM upsert_contract; -- { serverError SUPPORT_IS_DISABLED }
DETACH TABLE upsert_contract;
ATTACH TABLE upsert_contract;
SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = 'upsert_contract';

CREATE TABLE upsert_lww (id String, v UInt64) ENGINE = UpsertMergeTree ORDER BY v UNIQUE KEY id;
SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = 'upsert_lww';

CREATE TABLE upsert_bad (id UInt64) ENGINE = UpsertMergeTree ORDER BY id; -- { serverError BAD_ARGUMENTS }
CREATE TABLE upsert_bad (id UInt64) ENGINE = UpsertMergeTree ORDER BY id UNIQUE KEY (); -- { serverError BAD_ARGUMENTS }
CREATE TABLE upsert_bad (id UInt64, ver UInt32) ENGINE = UpsertMergeTree(ver) ORDER BY id UNIQUE KEY id; -- { serverError BAD_ARGUMENTS }
CREATE TABLE upsert_bad (id UInt64, ver Nullable(UInt64)) ENGINE = UpsertMergeTree(ver) ORDER BY id UNIQUE KEY id; -- { serverError BAD_ARGUMENTS }
CREATE TABLE upsert_bad (id UInt64) ENGINE = UpsertMergeTree(1) ORDER BY id UNIQUE KEY id; -- { serverError BAD_ARGUMENTS }
CREATE TABLE upsert_bad (id UInt64) ENGINE = UpsertMergeTree(id, id) ORDER BY id UNIQUE KEY id; -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
CREATE TABLE upsert_bad (id UInt64, v UInt64 ALIAS id) ENGINE = UpsertMergeTree(v) ORDER BY id UNIQUE KEY id; -- { serverError BAD_ARGUMENTS }
CREATE TABLE upsert_bad (id Float64) ENGINE = UpsertMergeTree ORDER BY id UNIQUE KEY id; -- { serverError BAD_ARGUMENTS }
CREATE TABLE upsert_bad (id UInt64, k Nullable(String)) ENGINE = UpsertMergeTree ORDER BY id UNIQUE KEY k; -- { serverError BAD_ARGUMENTS }
CREATE TABLE upsert_bad (id UInt64, _upsert_delete UInt8) ENGINE = UpsertMergeTree ORDER BY id UNIQUE KEY id; -- { serverError BAD_ARGUMENTS }

ALTER TABLE upsert_contract MODIFY COLUMN ver UInt32; -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE upsert_contract RENAME COLUMN ver TO version; -- { serverError SUPPORT_IS_DISABLED }

CREATE TABLE upsert_tuple (id Tuple(UInt64, String), v UInt8) ENGINE = UpsertMergeTree ORDER BY v UNIQUE KEY id;
SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = 'upsert_tuple';
DROP TABLE upsert_tuple;
DROP TABLE upsert_lww;
DROP TABLE upsert_contract;
