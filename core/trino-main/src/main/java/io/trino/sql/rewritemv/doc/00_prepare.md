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


# hdfs版本
```sql
create schema linkhouse.kernel_db01
with(
    location = 'hdfs://10.0.10.163:8020/dlink/linkhouse_4/kernel_db01.db' 
);


CREATE TABLE linkhouse.kernel_db01.part01_1
as
select * from tpch.tiny.part
where mfgr='Manufacturer#5' and brand = 'Brand#51';

select count(*) from linkhouse.kernel_db01.part01_1;

drop table linkhouse.kernel_db01.part01_1;
```


# 注
- 如果使用 hdfs 而不是oss, catalog name修改一下即可 iceberg ==> linkhouse
- 注意先使用 00_prepare 准备好 schema = linkhouse.kernel_db01
