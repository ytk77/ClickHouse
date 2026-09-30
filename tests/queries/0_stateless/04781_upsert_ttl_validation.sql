SET allow_experimental_unique_key = 1;

SELECT supports_ttl FROM system.table_engines WHERE name = 'UpsertMergeTree';

CREATE TABLE upsert_ttl_bad (id UInt64, expires DateTime, value String TTL expires)
ENGINE = UpsertMergeTree ORDER BY id UNIQUE KEY id; -- { serverError SUPPORT_IS_DISABLED }
CREATE TABLE upsert_ttl_bad (id UInt64, expires DateTime)
ENGINE = UpsertMergeTree ORDER BY id UNIQUE KEY id TTL expires RECOMPRESS CODEC(LZ4); -- { serverError SUPPORT_IS_DISABLED }
CREATE TABLE upsert_ttl_bad (id UInt64, expires DateTime)
ENGINE = UpsertMergeTree ORDER BY id UNIQUE KEY id TTL expires TO DISK 'default'; -- { serverError SUPPORT_IS_DISABLED }
CREATE TABLE upsert_ttl_bad (id UInt64, expires DateTime)
ENGINE = UpsertMergeTree ORDER BY id UNIQUE KEY id TTL expires GROUP BY id; -- { serverError SUPPORT_IS_DISABLED }
CREATE TABLE upsert_ttl_bad (id UInt64, expires DateTime)
ENGINE = UpsertMergeTree ORDER BY id UNIQUE KEY id TTL expires SETTINGS ttl_only_drop_parts=1; -- { serverError SUPPORT_IS_DISABLED }

CREATE TABLE upsert_ttl_date32 (id UInt64, expires Date32)
ENGINE = UpsertMergeTree ORDER BY id UNIQUE KEY id TTL expires;
DETACH TABLE upsert_ttl_date32 SYNC;
ATTACH TABLE upsert_ttl_date32;
DROP TABLE upsert_ttl_date32 SYNC;
