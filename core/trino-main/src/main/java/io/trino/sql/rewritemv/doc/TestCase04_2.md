# 解决 groupBy having
- mv 没groupBy, query有 groupBy

# base_table & mv
```sql
DROP TABLE if exists iceberg.kernel_db01.part04_2;

CREATE TABLE iceberg.kernel_db01.part04_2
as
select partkey, mfgr, brand, type, size s0, size s1, size s2, size s3, size s4, size s5, size s6, size s7
from tpch.tiny.part;

-- mv
create or replace materialized view iceberg.kernel_db01.mv_part_04_2 as
SELECT mfgr mfgr2, brand, s0, s1, s2
from iceberg.kernel_db01.part04_2
where 1=1
  and mfgr='Manufacturer#4'
GROUP BY mfgr, brand, s0, s1, s2
;

refresh MATERIALIZED VIEW iceberg.kernel_db01.mv_part_04_2;

select * from iceberg.kernel_db01.mv_part_04_2;
```

## 测试sql, 预期能够替换
```sql
-- mv有groupBy, query 无groupBy
SELECT mfgr, brand, s0
from iceberg.kernel_db01.part04_2
where 1=1
  and s0=s1
  and s0=s2
  and mfgr='Manufacturer#4'
;

-- 相同groupBy
SELECT mfgr, brand, s0, s1
from iceberg.kernel_db01.part04_2
where mfgr='Manufacturer#4'
  and s0=s1 
  and s0=s2
GROUP BY mfgr, brand, s0, s1, s2

-- query groupBy更少
SELECT mfgr, brand, s0, s1
from iceberg.kernel_db01.part04_2
where mfgr='Manufacturer#4'
  and s0=s1
  and s0=s2
GROUP BY mfgr, brand, s0, s1

-- 字段不包含
SELECT mfgr, brand, s0, s3
from iceberg.kernel_db01.part04_2
where mfgr='Manufacturer#4'
  and s0=s1
  and s0=s2
GROUP BY mfgr, brand, s0, s3
```
