-- Tags: use-rocksdb
SET allow_experimental_unique_key = 1;
SET async_insert = 0;

CREATE TABLE compact_bad (id UInt64, v UInt64) ENGINE = UpsertMergeTree(v) ORDER BY id UNIQUE KEY id SETTINGS upsert_lineage_compact = 1, upsert_lineage_compression = 0; -- { serverError BAD_ARGUMENTS }
CREATE TABLE compact_settings (id UInt64, v UInt64) ENGINE = UpsertMergeTree(v) ORDER BY id UNIQUE KEY id SETTINGS upsert_lineage_compact = 0, upsert_lineage_compression = 0;
SYSTEM STOP MERGES compact_settings;
INSERT INTO compact_settings VALUES (1,1);
ALTER TABLE compact_settings MODIFY SETTING upsert_lineage_compact = 1; -- { serverError BAD_ARGUMENTS }
ALTER TABLE compact_settings MODIFY SETTING upsert_lineage_compression = 1;
INSERT INTO compact_settings VALUES (2,2);
ALTER TABLE compact_settings MODIFY SETTING upsert_lineage_compact = 1;
INSERT INTO compact_settings VALUES (1,3),(3,3);
ALTER TABLE compact_settings MODIFY SETTING upsert_lineage_compression = 0; -- { serverError BAD_ARGUMENTS }
SELECT id,v FROM compact_settings ORDER BY id;
ALTER TABLE compact_settings MODIFY SETTING upsert_lineage_compact = 0, upsert_lineage_compression = 0;
INSERT INTO compact_settings VALUES (4,4);
SELECT count(),sum(v) FROM compact_settings;
DROP TABLE compact_settings SYNC;
