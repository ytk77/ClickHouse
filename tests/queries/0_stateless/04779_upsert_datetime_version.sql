SET allow_experimental_unique_key = 1;
SET async_insert = 0;

CREATE TABLE upsert_datetime (id UInt64, updatetime DateTime('Asia/Taipei'), value String)
ENGINE = UpsertMergeTree(updatetime) ORDER BY value UNIQUE KEY id;
INSERT INTO upsert_datetime VALUES
    (1,'2026-09-01 12:00:01','new'), (1,'2026-09-01 12:00:00','stale'),
    (2,'2026-09-01 12:00:00','old');
INSERT INTO upsert_datetime VALUES
    (1,'2026-09-01 12:00:00','late_stale'), (2,'2026-09-01 12:00:01','updated');
INSERT INTO upsert_datetime SELECT 3, toDateTime('2026-09-01 04:00:02','UTC'), 'same_instant';
INSERT INTO upsert_datetime SELECT 3, toDateTime('2026-09-01 04:00:01','UTC'), 'older_instant';
INSERT INTO upsert_datetime VALUES
    (2,'2026-09-01 12:00:01','tie_first'), (2,'2026-09-01 12:00:01','tie_last');
SELECT * FROM upsert_datetime ORDER BY id;
DETACH TABLE upsert_datetime SYNC;
ATTACH TABLE upsert_datetime;
SELECT * FROM upsert_datetime FINAL ORDER BY id;
DROP TABLE upsert_datetime SYNC;

CREATE TABLE upsert_datetime64 (id UInt64, updatetime DateTime64(9, 'Asia/Taipei'), value String)
ENGINE = UpsertMergeTree(updatetime) ORDER BY value UNIQUE KEY id;
INSERT INTO upsert_datetime64 SELECT id, fromUnixTimestamp64Nano(ticks), value
FROM values('id UInt64,ticks Int64,value String',
    (1,-1,'negative'), (1,-2,'older_negative'), (2,-10,'old'),
    (3,0,'epoch'), (4,1,'positive'), (5,-2208988800000000000,'year_1900'),
    (6,9223372036854775807,'maximum'));
INSERT INTO upsert_datetime64 SELECT id, fromUnixTimestamp64Nano(ticks), value
FROM values('id UInt64,ticks Int64,value String',
    (1,0,'zero'), (1,-1,'late_negative'), (2,-9,'new_negative'),
    (3,-1,'older_epoch'), (4,0,'older_positive'), (6,1,'older_maximum'));
INSERT INTO upsert_datetime64 SELECT id, fromUnixTimestamp64Nano(ticks), value
FROM values('id UInt64,ticks Int64,value String', (2,-9,'tie_first'), (2,-9,'tie_last'));
SELECT id, toUnixTimestamp64Nano(updatetime), value FROM upsert_datetime64 ORDER BY id;
DETACH TABLE upsert_datetime64 SYNC;
ATTACH TABLE upsert_datetime64;
SELECT id, toUnixTimestamp64Nano(updatetime), value FROM upsert_datetime64 FINAL ORDER BY id;
DROP TABLE upsert_datetime64 SYNC;

-- Preserve the full unsigned range of existing `UInt64` versions.
CREATE TABLE upsert_unsigned_version (id UInt64, ver UInt64, value String)
ENGINE = UpsertMergeTree(ver) ORDER BY id UNIQUE KEY id;
INSERT INTO upsert_unsigned_version VALUES (1,0,'zero'), (1,9223372036854775808,'high_bit');
INSERT INTO upsert_unsigned_version VALUES (1,18446744073709551615,'maximum');
INSERT INTO upsert_unsigned_version VALUES (1,0,'stale_zero');
SELECT * FROM upsert_unsigned_version;
DROP TABLE upsert_unsigned_version SYNC;

CREATE TABLE upsert_bad_time (id UInt64, ver Nullable(DateTime))
ENGINE = UpsertMergeTree(ver) ORDER BY id UNIQUE KEY id; -- { serverError BAD_ARGUMENTS }
CREATE TABLE upsert_bad_time (id UInt64, ver Nullable(DateTime64(3)))
ENGINE = UpsertMergeTree(ver) ORDER BY id UNIQUE KEY id; -- { serverError BAD_ARGUMENTS }
CREATE TABLE upsert_bad_time (id UInt64, ts DateTime64(3), ver DateTime64(3) ALIAS ts)
ENGINE = UpsertMergeTree(ver) ORDER BY id UNIQUE KEY id; -- { serverError BAD_ARGUMENTS }
