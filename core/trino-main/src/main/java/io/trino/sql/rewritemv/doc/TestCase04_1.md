# 解决 groupBy having
- mv 没groupBy, query有 groupBy

# base_table
```sql
USE iceberg.kernel_db01;

DROP TABLE if exists part04_1;

CREATE TABLE part04_1
as
select partkey, mfgr, brand, type, size s0, size s1, size s2, size s3, size s4, size s5, size s6, size s7
from tpch.tiny.part;


-- mv定义
create or replace materialized view mv_part04_1 as
SELECT mfgr mfgr2, brand, s1
from part04_1
where 1=1
  and s0=s1 
  and s0=s2
;

refresh MATERIALIZED VIEW mv_part04_1;

-- select * from mv_part04_1;
-- drop materialized view mv_part04_1 ;
-- drop table part04_1;
```

## 测试sql, 预期能够替换
```sql
set session query_rewrite_with_materialized_view_status = 2;

-- sql1, 原mv select
SELECT mfgr, brand, s0
from part04_1
where mfgr='Manufacturer#4'
  and s0=s1 
  and s0=s2
;

-- 原mv的基础上 多了 groupBy, 且字段包含
SELECT mfgr, brand, s0
from part04_1
where mfgr='Manufacturer#4'
  and s0=s1 
  and s0=s2
GROUP BY mfgr, brand, s0
;


-- 原mv的基础上 多了 groupBy, 且字段不包含, 预期不能命中
SELECT mfgr, brand, s0, s3
from part04_1
where mfgr='Manufacturer#4'
  and s0=s1
  and s0=s2
GROUP BY mfgr, brand, s0, s3
;
```
