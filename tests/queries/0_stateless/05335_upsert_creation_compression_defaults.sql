-- Tags: use-rocksdb
SET allow_experimental_unique_key = 1;
SET async_insert = 0;
SET compatibility = '';

CREATE TABLE creation_defaults_plain (id UInt64, v UInt64) ENGINE = UpsertMergeTree(v) ORDER BY id UNIQUE KEY id;
CREATE TABLE creation_defaults_disabled (id UInt64, v UInt64) ENGINE = UpsertMergeTree(v) ORDER BY id UNIQUE KEY id SETTINGS upsert_lineage_compression = 0, upsert_state_compression = 0;
CREATE TABLE creation_defaults_state_disabled (id UInt64, v UInt64) ENGINE = UpsertMergeTree(v) ORDER BY id UNIQUE KEY id SETTINGS upsert_state_compression = 0;
CREATE TABLE creation_defaults_ctas ENGINE = UpsertMergeTree(v) ORDER BY id UNIQUE KEY id AS SELECT toUInt64(1) AS id, toUInt64(2) AS v;
CREATE OR REPLACE TABLE creation_defaults_replace (id UInt64, v UInt64) ENGINE = UpsertMergeTree(v) ORDER BY id UNIQUE KEY id;
CREATE TABLE creation_defaults_native (id UInt64) ENGINE = MergeTree ORDER BY id;

SET compatibility = '26.8';
SET allow_experimental_unique_key = 1;
CREATE TABLE creation_defaults_old (id UInt64, v UInt64) ENGINE = UpsertMergeTree(v) ORDER BY id UNIQUE KEY id;
SET compatibility = '26.10';
SET allow_experimental_unique_key = 1;
CREATE TABLE creation_defaults_old_major (id UInt64, v UInt64) ENGINE = UpsertMergeTree(v) ORDER BY id UNIQUE KEY id;
CREATE TABLE creation_defaults_optin (id UInt64, v UInt64) ENGINE = UpsertMergeTree(v) ORDER BY id UNIQUE KEY id SETTINGS upsert_lineage_compression = 1, upsert_lineage_compact = 1, upsert_state_compression = 1;
CREATE TABLE creation_defaults_legacy (id UInt64, v UInt64) ENGINE = UpsertMergeTree(v) ORDER BY id UNIQUE KEY id;
DETACH TABLE creation_defaults_legacy SYNC;
SET compatibility = '26.10.1.1';
SET allow_experimental_unique_key = 1;
SET async_insert = 0;
CREATE TABLE creation_defaults_from_old AS creation_defaults_old;
ATTACH TABLE creation_defaults_legacy;
ALTER TABLE creation_defaults_legacy ADD COLUMN value String DEFAULT '';
DETACH TABLE creation_defaults_legacy SYNC;
ATTACH TABLE creation_defaults_legacy;

CREATE TABLE creation_defaults_source (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
CREATE MATERIALIZED VIEW creation_defaults_mv ENGINE = UpsertMergeTree(v) ORDER BY id UNIQUE KEY id AS SELECT id, v FROM creation_defaults_source;
INSERT INTO creation_defaults_source VALUES (1,1),(1,2);
SELECT count(),sum(v) FROM creation_defaults_mv;
SELECT count(),sum(v) FROM creation_defaults_ctas;
SELECT name,
    extract(create_table_query, 'upsert_lineage_compression = (true|false|[01])') IN ('1', 'true'),
    extract(create_table_query, 'upsert_lineage_compact = (true|false|[01])') IN ('1', 'true'),
    extract(create_table_query, 'upsert_state_compression = (true|false|[01])') IN ('1', 'true')
FROM system.tables WHERE database = currentDatabase() AND startsWith(name, 'creation_defaults_') ORDER BY name;
SELECT name, position(create_table_query, 'upsert_lineage_compression') = 0,
    position(create_table_query, 'upsert_lineage_compact') = 0,
    position(create_table_query, 'upsert_state_compression') = 0
FROM system.tables WHERE database = currentDatabase() AND name IN ('creation_defaults_legacy','creation_defaults_old','creation_defaults_old_major') ORDER BY name;

DROP TABLE creation_defaults_mv SYNC;
DROP TABLE creation_defaults_source SYNC;
DROP TABLE creation_defaults_plain SYNC;
DROP TABLE creation_defaults_disabled SYNC;
DROP TABLE creation_defaults_state_disabled SYNC;
DROP TABLE creation_defaults_ctas SYNC;
DROP TABLE creation_defaults_replace SYNC;
DROP TABLE creation_defaults_native SYNC;
DROP TABLE creation_defaults_old SYNC;
DROP TABLE creation_defaults_old_major SYNC;
DROP TABLE creation_defaults_optin SYNC;
DROP TABLE creation_defaults_from_old SYNC;
DROP TABLE creation_defaults_legacy SYNC;
