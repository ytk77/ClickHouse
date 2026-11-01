-- Tags: use-rocksdb
SET async_insert=0;
CREATE TABLE kv_false_single (id UInt64,payload String) ENGINE=EmbeddedRocksDB PRIMARY KEY id;
INSERT INTO kv_false_single VALUES (1,'a'),(2,'b');
SELECT 'single false',count() FROM kv_false_single WHERE 0;
SELECT 'single null',count() FROM kv_false_single WHERE CAST(NULL AS Nullable(UInt8));
SELECT 'single empty',count() FROM kv_false_single WHERE id IN tuple();
SELECT 'single or',id,payload FROM kv_false_single WHERE (id IN tuple()) OR id=2;
SELECT 'single true',count() FROM kv_false_single WHERE 1;
SELECT 'single not empty',count() FROM kv_false_single WHERE id NOT IN tuple();
DROP TABLE kv_false_single;

CREATE TABLE kv_false_composite (tenant UInt8,id UInt64,payload String)
ENGINE=EmbeddedRocksDB PRIMARY KEY (tenant,id);
INSERT INTO kv_false_composite VALUES (1,1,'a'),(2,3,'c');
SELECT 'composite false',count() FROM kv_false_composite WHERE tenant=1 AND id IN tuple();
SELECT 'composite empty',count() FROM kv_false_composite WHERE (tenant,id) IN CAST([], 'Array(Tuple(UInt8,UInt64))');
SELECT 'composite or',id,payload FROM kv_false_composite WHERE (tenant=1 AND id IN tuple()) OR (tenant=2 AND id=3);
SELECT 'composite true',count() FROM kv_false_composite WHERE 1;
DROP TABLE kv_false_composite;
