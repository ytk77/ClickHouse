SET allow_experimental_unique_key = 1;
SET async_insert = 0;

CREATE TABLE upsert_materialized_identity
(
    id String,
    tx_time DateTime('UTC'),
    source_version Int64,
    value String,
    key_time UInt32 MATERIALIZED toUnixTimestamp(tx_time),
    write_version UInt64 MATERIALIZED bitXor(reinterpretAsUInt64(source_version), toUInt64(9223372036854775808))
)
ENGINE = UpsertMergeTree(write_version)
PARTITION BY cityHash64(id, key_time) % 512
ORDER BY (id, tx_time)
UNIQUE KEY (id, key_time);

INSERT INTO upsert_materialized_identity (id,tx_time,source_version,value) VALUES
    ('a','2026-01-01 00:00:00',-1,'negative'),
    ('a','2026-01-02 00:00:00',1,'different_time');
INSERT INTO upsert_materialized_identity (id,tx_time,source_version,value) VALUES
    ('a','2026-01-01 00:00:00',2,'positive');
INSERT INTO upsert_materialized_identity (id,tx_time,source_version,value) VALUES
    ('a','2026-01-01 00:00:00',-2,'older_negative');
SELECT id,source_version,value FROM upsert_materialized_identity ORDER BY tx_time;
SELECT write_version FROM upsert_materialized_identity WHERE value='positive';
DETACH TABLE upsert_materialized_identity SYNC;
ATTACH TABLE upsert_materialized_identity;
SELECT id,source_version,value FROM upsert_materialized_identity ORDER BY tx_time;
DROP TABLE upsert_materialized_identity SYNC;
