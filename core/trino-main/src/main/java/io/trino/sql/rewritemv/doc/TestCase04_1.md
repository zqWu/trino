# 解决 groupBy having


# base_table
```sql
DROP TABLE if exists iceberg.kernel_db01.part04;

CREATE TABLE iceberg.kernel_db01.part04
as
    select partkey, mfgr, brand, type, size s0, size s1, size s2, size s3, size s4, size s5, size s6, size s7
    from tpch.tiny.part;
```

## mv定义
```sql
create or replace materialized view iceberg.kernel_db01.mv_part_04 as
SELECT mfgr mfgr2, brand, s0, count(*) as _cnt
from iceberg.kernel_db01.part04
where 1=1
  and s0=s1 
  and s0=s2
GROUP BY mfgr, brand, s0
;

refresh MATERIALIZED VIEW iceberg.kernel_db01.mv_part_04;

select * from iceberg.kernel_db01.mv_part_04;
```

## 测试sql, 预期能够替换
```sql
-- sql1, 原mv的基础上 多了 having语句
SELECT mfgr mfgr2, brand, s0, count(*) as _cnt
from iceberg.kernel_db01.part04
    where 1=1
  and s0=s1 
  and s0=s2
GROUP BY mfgr, brand, s0
having count(*) > 4
;

```
