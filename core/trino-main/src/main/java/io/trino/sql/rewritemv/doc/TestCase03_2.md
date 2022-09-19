# base_table
```sql
DROP TABLE if exists iceberg.kernel_db01.part03_2;

CREATE TABLE iceberg.kernel_db01.part03_2
as 
    select partkey, mfgr, brand, type, size
    from tpch.tiny.part;
```


# 解决 where 中等价问题2


## mv定义
```sql
create or replace materialized view iceberg.kernel_db01.mv_part_03_2 as
SELECT mfgr mfgr2, brand, type type2, size
from iceberg.kernel_db01.part03_2
where 1=1
  and mfgr='Manufacturer#1'
  and size>20
GROUP BY mfgr, brand, type, size;

refresh MATERIALIZED VIEW iceberg.kernel_db01.mv_part_03_2;

select * from iceberg.kernel_db01.mv_part_03_2;
```

## 测试sql, 预期能够替换
```sql
-- sql1
SELECT mfgr mfgr2, brand, type type2, size
from iceberg.kernel_db01.part03_2
where 1=1
  and mfgr='Manufacturer#1'
  and size>=30 and size<40
GROUP BY mfgr, brand, type, size;

```

## 测试点
- where
  - equivalent class (size, size_cp)
