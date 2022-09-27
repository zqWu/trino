# base_table
```sql
DROP TABLE if exists iceberg.kernel_db01.part03_3;

CREATE TABLE iceberg.kernel_db01.part03_3
as 
    select partkey, mfgr, brand, type, size
    from tpch.tiny.part;

-- mv定义
create or replace materialized view iceberg.kernel_db01.mv_part03_3 as
SELECT mfgr mfgr2, brand, type type2, size
from iceberg.kernel_db01.part03_3
where 1=1
  and mfgr='Manufacturer#1'
  and size>20
  and brand like 'Brand#1%'
GROUP BY mfgr, brand, type, size;

refresh MATERIALIZED VIEW iceberg.kernel_db01.mv_part03_3;

-- select * from iceberg.kernel_db01.mv_part03_3;
-- drop materialized view iceberg.kernel_db01.mv_part03_3 ;
-- drop table iceberg.kernel_db01.part03_3;
```

## 测试sql, 预期能够替换
```sql
-- sql1
SELECT mfgr mfgr2, brand, type type2, size
from iceberg.kernel_db01.part03_3
where 1=1
  and mfgr='Manufacturer#1'
  and size>=30 and size<40
  and brand like 'Brand#2%'
GROUP BY mfgr, brand, type, size;

```

## 测试点
- where
  - equivalent class (size, size_cp)
