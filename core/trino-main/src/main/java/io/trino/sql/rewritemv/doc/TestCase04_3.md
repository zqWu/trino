# 解决 groupBy having
- mv 没groupBy, query有 groupBy

# base_table & mv
```sql
DROP TABLE if exists iceberg.kernel_db01.part04_3;

CREATE TABLE iceberg.kernel_db01.part04_3
as
select partkey, mfgr, brand, type, size s0, size s1, size s2, size s3, size s4, size s5, size s6, size s7
from tpch.tiny.part;

-- mv
create or replace materialized view iceberg.kernel_db01.mv_part_04_3 as
SELECT mfgr mfgr2, brand, s0, s1, s2, count(*) as _cnt
from iceberg.kernel_db01.part04_3
where 1=1
  and mfgr='Manufacturer#4'
GROUP BY mfgr, brand, s0, s1, s2
;

refresh MATERIALIZED VIEW iceberg.kernel_db01.mv_part_04_3;

select * from iceberg.kernel_db01.mv_part_04_3;
```

## 测试sql, 预期能够替换
```sql
-- 相同groupBy, 多having, 且 having字段直接存在
SELECT mfgr, brand, s0, s1, count(*) as _cnt
from iceberg.kernel_db01.part04_3
    where mfgr='Manufacturer#4'
  and s0=s1 
  and s0=s2
GROUP BY mfgr, brand, s0, s1, s2
having s0 > 3


```
