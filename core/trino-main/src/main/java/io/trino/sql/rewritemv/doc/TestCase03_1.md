# 解决 where 中等价问题2

## 测试点
- where
    - equivalent class (size, size_cp)
    - range predicate
    - other predicate


# base_table
```sql
DROP TABLE if exists iceberg.kernel_db01.part03_1;

CREATE TABLE iceberg.kernel_db01.part03_1
as 
    select partkey, mfgr, brand, type, size s0, size s1, size s2, size s3, size s4, size s5, size s6, size s7
    from tpch.tiny.part;


-- mv定义
create or replace materialized view iceberg.kernel_db01.mv_part03_1 as
SELECT mfgr mfgr2, brand, type type2, s0, s1, s3, s5
from iceberg.kernel_db01.part03_1
where 1=1
  and mfgr='Manufacturer#1'
  and s1=s2 and s1=s7
  and s3=s4
  and s5=s6
GROUP BY mfgr, brand, type, s0, s1, s3, s5;

refresh MATERIALIZED VIEW iceberg.kernel_db01.mv_part03_1;

-- select * from iceberg.kernel_db01.mv_part03_1;
-- drop materialized view iceberg.kernel_db01.mv_part03_1 ;
-- drop table iceberg.kernel_db01.part03_1;
```

## 测试sql, 预期能够替换
```sql
-- sql1
SELECT iceberg.kernel_db01.part03_1.mfgr, brand, type, s2
from iceberg.kernel_db01.part03_1
where 2=2
  and mfgr=mfgr
  and 'Manufacturer#1'=mfgr
  and brand='Brand#14'
  and s1=s2 and s1=s3 and s1=s4 and s1=s5 and s1=s6 and s1=s7 and s1=s0
GROUP BY mfgr, brand, type, s2
-- where条件的改写
-- s2 ===> s1等等

-- sql2, 在sql1的基础上增加了 and s0>10 and s0<40
SELECT iceberg.kernel_db01.part03_1.mfgr, brand, type, s2
from iceberg.kernel_db01.part03_1
where 2=2
  and mfgr=mfgr
  and 'Manufacturer#1'=mfgr
  and brand='Brand#14'
  and s1=s2 and s1=s3 and s1=s4 and s1=s5 and s1=s6 and s1=s7 and s1=s0
  and s0>=30 and s0<40
GROUP BY mfgr, brand, type, s2

-- sql3, and s0>=10 and s0<40 ===> and s0 between 10 and 40
SELECT iceberg.kernel_db01.part03_1.mfgr, brand, type, s2
from iceberg.kernel_db01.part03_1
where 2=2
  and mfgr=mfgr
  and 'Manufacturer#1'=mfgr
  and brand='Brand#14'
  and s1=s2 and s1=s3 and s1=s4 and s1=s5 and s1=s6 and s1=s7 and s1=s0
  and s0 between 40 and 50
GROUP BY mfgr, brand, type, s2

-- sql4, 在 sql3 的基础上增加了 
-- and type like 'LARGE%'
-- and brand like 'Brand%'
-- and brand not in ('haha')
SELECT iceberg.kernel_db01.part03_1.mfgr, brand, type, s2
from iceberg.kernel_db01.part03_1
where 2=2
  and mfgr=mfgr
  and 'Manufacturer#1'=mfgr
  and brand='Brand#14'
  and s1=s2 and s1=s3 and s1=s4 and s1=s5 and s1=s6 and s1=s7 and s1=s0
  and s0 between 10 and 40
  and type like 'LARGE%'
  and brand like 'Brand%'
  and brand not in ('haha')
GROUP BY mfgr, brand, type, s2


-- sql5, 在 sql4 的基础上  and (brand='Brand#14' and (s2=18 and 1+1=2))
SELECT iceberg.kernel_db01.part03_1.mfgr, brand, type, s2
from iceberg.kernel_db01.part03_1
where 2=2
  and mfgr=mfgr
  and 'Manufacturer#1'=mfgr
  and (brand='Brand#14' and (s2=18 and 1+1=2) and s0+1>=10+1)
  and s1=s2 and s1=s3 and s1=s4 and s1=s5 and s1=s6 and s1=s7 and s1=s0
  and s0 between 10 and 40
  and type like 'LARGE%'
  and brand like 'Brand%'
  and brand not in ('haha')
  and s0+1 in (1,2,19)
GROUP BY mfgr, brand, type, s2


-- s2=18 ===> s2+1=19
SELECT iceberg.kernel_db01.part03_1.mfgr, brand, type, s2
from iceberg.kernel_db01.part03_1
where 2=2
  and mfgr=mfgr
  and 'Manufacturer#1'=mfgr
  and (brand='Brand#14' and (s2+1=19 and 1+1=2) and s0+1>=10+1)
  and s1=s2 and s1=s3 and s1=s4 and s1=s5 and s1=s6 and s1=s7 and s1=s0
  and s0 between 10 and 40
  and type like 'LARGE%'
  and brand like 'Brand%'
  and brand not in ('haha')
  and s0+1 in (1,2,19)
GROUP BY mfgr, brand, type, s2
```

