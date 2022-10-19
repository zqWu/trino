# base_table
```sql
USE iceberg.kernel_db01;

DROP TABLE if exists part03_3;

CREATE TABLE part03_3
as 
select partkey, mfgr, brand, type, size
from tpch.tiny.part;

-- mv定义
create or replace materialized view mv_part03_3 as
SELECT mfgr mfgr2, brand, type type2, size
from part03_3
where 1=1
  and mfgr='Manufacturer#1'
  and size>20
  and brand like 'Brand#1%'
GROUP BY mfgr, brand, type, size;

refresh MATERIALIZED VIEW mv_part03_3;

-- select * from mv_part03_3;
-- drop materialized view mv_part03_3 ;
-- drop table part03_3;
```

## 测试sql
```sql
set session query_rewrite_with_materialized_view_status = 2;

-- sql1 预期能够替换
SELECT mfgr mfgr2, brand, type type2, size
from part03_3
where 1=1
  and mfgr='Manufacturer#1'
  and size>=30 and size<40
  and brand like 'Brand#1%'
GROUP BY mfgr, brand, type, size;


-- sql1 预期能够替换
SELECT mfgr mfgr2, brand, type type2, size
from part03_3
where 1=1
  and mfgr='Manufacturer#1'
  and size>=30 and size<40
  and brand like 'Brand#2%'
GROUP BY mfgr, brand, type, size;
```

## 测试点
- where
  - equivalent class (size, size_cp)
