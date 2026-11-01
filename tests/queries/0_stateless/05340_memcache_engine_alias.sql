SET async_insert=0;
SELECT name,length(description)>9 FROM system.table_engines
WHERE name IN ('MemCache','OverwriteCache') ORDER BY name;
CREATE TABLE memcache_alias (tenant UInt8,id UInt64,v UInt64,payload String)
ENGINE=MemCache(v) KEYS (tenant,id) INDEX (tenant)
SETTINGS persist_mode='sync',max_memory_bytes=10000000;
SELECT engine,position(create_table_query,'ENGINE = MemCache(v)')>0
FROM system.tables WHERE database=currentDatabase() AND name='memcache_alias';
INSERT INTO memcache_alias VALUES (1,1,1,'old'),(1,2,1,'second'),(2,1,1,'other');
INSERT INTO memcache_alias VALUES (1,1,2,'new'),(1,1,0,'stale');
SELECT id,v,payload FROM memcache_alias WHERE tenant=1 ORDER BY id;
SELECT overwriteCacheGet('memcache_alias','payload',toUInt8(1),toUInt64(1));
ALTER TABLE memcache_alias ADD INDEX (id);
SELECT tenant,payload FROM memcache_alias WHERE id=1 ORDER BY tenant;
ALTER TABLE memcache_alias DROP INDEX (id);
DELETE FROM memcache_alias WHERE tenant=1 AND id=2;
DETACH TABLE memcache_alias;
ATTACH TABLE memcache_alias;
SELECT id,v,payload FROM memcache_alias WHERE tenant=1 ORDER BY id;
SELECT position(create_table_query,'ENGINE = MemCache(v)')>0
FROM system.tables WHERE database=currentDatabase() AND name='memcache_alias';
CREATE TABLE memcache_clone AS memcache_alias;
INSERT INTO memcache_clone VALUES (1,9,1,'clone');
SELECT payload FROM memcache_clone WHERE tenant=1 AND id=9;
SELECT position(create_table_query,'ENGINE = MemCache(v)')>0
FROM system.tables WHERE database=currentDatabase() AND name='memcache_clone';
DROP TABLE memcache_clone;
DROP TABLE memcache_alias;

CREATE TABLE overwrite_cache_alias_control (id UInt64,v UInt64,payload String)
ENGINE=OverwriteCache(v) KEYS (id) SETTINGS persist_mode='none';
INSERT INTO overwrite_cache_alias_control VALUES (1,1,'old'),(1,2,'new');
SELECT payload FROM overwrite_cache_alias_control WHERE id=1;
DROP TABLE overwrite_cache_alias_control;
CREATE TABLE memcache_bad (id UInt64,v UInt64) ENGINE=MemCache KEYS (id); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
CREATE TABLE memcache_bad (id UInt64,v UInt64) ENGINE=MemCache(v); -- { serverError BAD_ARGUMENTS }
CREATE TABLE memcache_bad (id UInt64,v UInt64) ENGINE=MemCache(v) KEYS (id) SETTINGS bogus_setting=1; -- { serverError UNKNOWN_SETTING }
