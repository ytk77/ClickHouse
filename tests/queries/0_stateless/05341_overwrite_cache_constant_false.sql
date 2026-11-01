SET async_insert=0;
CREATE TABLE cache_false (tenant UInt8,id UInt64,v UInt64,payload String)
ENGINE=OverwriteCache(v) KEYS (tenant,id) INDEX (tenant) SETTINGS persist_mode='none';
INSERT INTO cache_false VALUES (1,1,1,'a'),(1,2,1,'b'),(2,3,1,'c');
SELECT 'empty primary',count() FROM cache_false WHERE (tenant,id) IN CAST([], 'Array(Tuple(UInt8,UInt64))');
SELECT 'empty conjunction',count() FROM cache_false WHERE tenant=1 AND id IN tuple() AND payload='a';
SELECT 'empty lookup',count() FROM cache_false WHERE tenant IN tuple();
SELECT 'empty non-key',count() FROM cache_false WHERE payload IN tuple();
SELECT 'false',count() FROM cache_false WHERE 0;
SELECT 'null',count() FROM cache_false WHERE CAST(NULL AS Nullable(UInt8));
WITH toUInt8(0) AS never SELECT 'alias',count() FROM cache_false WHERE never;
SELECT 'no groups',count() FROM (SELECT tenant,count() FROM cache_false WHERE id IN tuple() GROUP BY tenant);
SELECT id FROM cache_false WHERE id IN tuple() LIMIT 1;
SELECT 'or survives',id FROM cache_false WHERE (tenant=1 AND id IN tuple()) OR (tenant=2 AND id=3);
SELECT 'not empty',count() FROM cache_false WHERE tenant=1 AND id NOT IN tuple();
SELECT 'not false',count() FROM cache_false WHERE tenant=1 AND NOT (id IN tuple());
DELETE FROM cache_false WHERE id IN tuple();
SELECT 'delete no-op',count() FROM cache_false WHERE tenant IN (1,2);
-- A constant true predicate still requires an indexed lookup.
SELECT payload FROM cache_false WHERE 1; -- { serverError BAD_ARGUMENTS }
SELECT payload FROM cache_false WHERE 1 OR tenant=1; -- { serverError BAD_ARGUMENTS }

CREATE TABLE memcache_false (tenant UInt8,id UInt64,v UInt64,payload String)
ENGINE=MemCache(v) KEYS (tenant,id) INDEX (tenant) SETTINGS persist_mode='none';
INSERT INTO memcache_false SELECT * FROM cache_false WHERE tenant IN (1,2);
SELECT 'mem empty conjunction',count() FROM memcache_false WHERE tenant=1 AND id IN tuple();
SELECT 'mem false',count() FROM memcache_false WHERE 0;
SELECT 'mem null',count() FROM memcache_false WHERE CAST(NULL AS Nullable(UInt8));
SELECT 'mem survives',id,payload FROM memcache_false WHERE tenant=2 AND id=3;
DELETE FROM memcache_false WHERE id IN tuple();
SELECT 'mem delete no-op',count() FROM memcache_false WHERE tenant IN (1,2);
SELECT payload FROM memcache_false WHERE 1; -- { serverError BAD_ARGUMENTS }
DROP TABLE memcache_false;
DROP TABLE cache_false;
