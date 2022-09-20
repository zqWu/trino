# 解决 groupBy having
- mv 没groupBy, query有 groupBy

# base_table
```sql
DROP TABLE if exists iceberg.kernel_db01.part04_1;

CREATE TABLE iceberg.kernel_db01.part04_1
as
    select partkey, mfgr, brand, type, size s0, size s1, size s2, size s3, size s4, size s5, size s6, size s7
    from tpch.tiny.part;
```

## mv定义
```sql
create or replace materialized view iceberg.kernel_db01.mv_part_04_1 as
SELECT mfgr mfgr2, brand, s1
from iceberg.kernel_db01.part04_1
where 1=1
  and s0=s1 
  and s0=s2
;

refresh MATERIALIZED VIEW iceberg.kernel_db01.mv_part_04_1;

select * from iceberg.kernel_db01.mv_part_04_1;
```

## 测试sql, 预期能够替换
```sql
-- sql1, 原mv select
SELECT mfgr, brand, s0, count(*) as _cnt
from iceberg.kernel_db01.part04_1
where mfgr='Manufacturer#4'
  and s0=s1 
  and s0=s2

-- sql1, 原mv的基础上 多了 groupBy
SELECT mfgr, brand, s0, count(*) as _cnt
from iceberg.kernel_db01.part04_1
where mfgr='Manufacturer#4'
  and s0=s1 
  and s0=s2
GROUP BY mfgr, brand, s0
```
