# base_table
```sql
DROP TABLE if exists iceberg.kernel_db01.part02;

CREATE TABLE iceberg.kernel_db01.part02
as 
    select partkey, mfgr, brand, type, size s0, size s1, size s2, size s3, size s4, size s5, size s6, size s7
    from tpch.tiny.part;
```


# 解决 where 中等价问题2


## mv定义
```sql
create or replace materialized view iceberg.kernel_db01.mv_part_03 as
SELECT mfgr mfgr2, brand, type type2, s0, s1, s3, s5
from iceberg.kernel_db01.part02
where 1=1
  and mfgr='Manufacturer#1'
  and s1=s2 and s1=s7
  and s3=s4
  and s5=s6
GROUP BY mfgr, brand, type, s0, s1, s3, s5;

refresh MATERIALIZED VIEW iceberg.kernel_db01.mv_part_03;

select * from iceberg.kernel_db01.mv_part_03;
```

## 测试sql, 预期能够替换
```sql
-- sql1
SELECT iceberg.kernel_db01.part02.mfgr, brand, type, s2
from iceberg.kernel_db01.part02
where 2=2
  and mfgr=mfgr
  and 'Manufacturer#1'=mfgr
  and brand='Brand#14'
  and s1=s2 and s1=s3 and s1=s4 and s1=s5 and s1=s6 and s1=s7 and s1=s0
GROUP BY mfgr, brand, type, s2


-- sql1 手动用mv 改写
SELECT mfgr2 mfgr, brand, type2 type, s1 as s2
from iceberg.kernel_db01.mv_part_03
where 
    'Manufacturer#1'=mfgr2
  and brand='Brand#14'
  and s1=s3 and s1=s5 and s1=s0
GROUP BY mfgr2, brand, type2, s1;



-- sql2
SELECT iceberg.kernel_db01.part02.mfgr, brand, type, size
from iceberg.kernel_db01.part02
where 2=2
  and mfgr=mfgr
  and 'Manufacturer#1'=mfgr
  and (brand='Brand#14' and (size=7 and 1+1=2))
  and and s1=s2 and s1=s3 and s1=s4 and s1=s5 and s1=s6 and s1=s7
GROUP BY mfgr, brand, type,size
```

## 测试点
- where
  - equivalent class (size, size_cp)
