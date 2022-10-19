# 解决 groupBy having
- mv 有groupBy, query有 groupBy
- 都没有 having

# base_table & mv
```sql
USE iceberg.kernel_db01;

DROP TABLE if exists part04_2;

CREATE TABLE part04_2
as
select partkey, mfgr, brand, type, size s0, size s1, size s2, size s3, size s4, size s5, size s6, size s7
from tpch.tiny.part;

-- mv
create or replace materialized view mv_part04_2 as
SELECT mfgr mfgr2, brand, s0, s1, s2
from part04_2
where 1=1
  and mfgr='Manufacturer#4'
GROUP BY mfgr, brand, s0, s1, s2
;

refresh MATERIALIZED VIEW mv_part04_2;

-- select * from mv_part04_2;
-- drop materialized view mv_part04_2 ;
-- drop table part04_2;
```

## 测试sql, 预期能够替换
```sql
set session query_rewrite_with_materialized_view_status = 2;

-- mv有groupBy, query 无groupBy, 预期 not fit
SELECT mfgr, brand, s0
from part04_2
where 1=1
  and s0=s1
  and s0=s2
  and mfgr='Manufacturer#4'
;

-- 相同groupBy
SELECT mfgr, brand, s0, s1
from part04_2
where mfgr='Manufacturer#4'
  and s0=s1 
  and s0=s2
GROUP BY mfgr, brand, s0, s1, s2
;

-- query groupBy更少
SELECT mfgr, brand, s0, s1
from part04_2
where mfgr='Manufacturer#4'
  and s0=s1
  and s0=s2
GROUP BY mfgr, brand, s0, s1
;

-- 字段不包含, 预期 not fit
SELECT mfgr, brand, s0, s3
from part04_2
where mfgr='Manufacturer#4'
  and s0=s1
  and s0=s2
GROUP BY mfgr, brand, s0, s3
;

```
