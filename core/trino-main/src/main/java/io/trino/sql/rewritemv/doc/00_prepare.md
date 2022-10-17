```sql
show catalogs;

show schemas from iceberg;

drop schema iceberg.kernel_db01;

CREATE SCHEMA iceberg.kernel_db01
WITH (
   location='s3a://fastdata-tsdb/warehouse/kernel_db01.db'
);


-- 测试
DROP TABLE if exists iceberg.kernel_db01.part01_1;

CREATE TABLE iceberg.kernel_db01.part01_1
as
select * from tpch.tiny.part
where mfgr='Manufacturer#5'
  and brand = 'Brand#51'
;
```
