DROP TABLE IF EXISTS overwrite_review;
CREATE TABLE overwrite_review (tenant UInt64, id UInt64, tag String, version UInt64, payload String)
ENGINE=OverwriteCache(version) KEYS (tenant, id, tag) INDEX (tenant), (tag);
INSERT INTO overwrite_review VALUES (1,0,'risk',1,'alpha'),(1,1,'risk',1,'beta');
SELECT overwriteCacheGet('overwrite_review', 'payload', toUInt64(1), number, 'risk') FROM numbers(2);
SELECT overwriteCacheGetOrNull('overwrite_review', 'payload', toUInt64(1), number, 'risk') FROM numbers(3);
SELECT id FROM overwrite_review WHERE tenant=1 AND id=1 AND tag='risk' SETTINGS query_plan_optimize_primary_key=0;
ALTER TABLE overwrite_review MODIFY SETTING max_memory_bytes=515396075520;
SELECT payload FROM overwrite_review WHERE tenant=1 AND id=0 AND tag='risk';
ALTER TABLE overwrite_review DROP INDEX (tag);
SELECT count() FROM overwrite_review WHERE tenant=1;
ALTER TABLE overwrite_review ADD INDEX (tag);
INSERT INTO overwrite_review SELECT 1, number+2, 'risk', 1, 'old' FROM numbers(70000);
DELETE FROM overwrite_review WHERE tenant=1 AND payload='old' SETTINGS max_block_size=1024;
SELECT count() FROM overwrite_review WHERE tenant=1;
DROP TABLE overwrite_review;

CREATE TABLE overwrite_review_intersection (a UInt64, b UInt64, id UInt64, v UInt64, payload String)
ENGINE=OverwriteCache(v) KEYS(a,b,id) INDEX(a),(b);
INSERT INTO overwrite_review_intersection SELECT 1, number, number, 1, repeat('x',100) FROM numbers(10000);
SELECT count() FROM overwrite_review_intersection WHERE a=1 AND b IN (SELECT number FROM numbers(10000));
DROP TABLE overwrite_review_intersection;
