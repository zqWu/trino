# base_table
```sql
USE iceberg.kernel_db01;

DROP TABLE if exists part03_2;

CREATE TABLE part03_2
as 
select partkey, mfgr, brand, type, size
from tpch.tiny.part;

-- 
create or replace materialized view mv_part03_2 as
SELECT mfgr mfgr2, brand, type type2, size
from part03_2
where 1=1
  and mfgr='Manufacturer#1'
  and size>20
GROUP BY mfgr, brand, type, size;

refresh MATERIALIZED VIEW mv_part03_2;

-- select * from mv_part03_2;
-- drop materialized view mv_part03_2 ;
-- drop table part03_2;
```

## 测试sql, 预期能够替换
```sql
set session query_rewrite_with_materialized_view_status = 2;

-- sql1
SELECT mfgr mfgr2, brand, type type2, size
from part03_2
where 1=1
  and mfgr='Manufacturer#1'
  and size>=30 and size<40
GROUP BY mfgr, brand, type, size;

```

## 测试点
- where
  - equivalent class (size, size_cp)
