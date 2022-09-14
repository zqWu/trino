# base_table
```sql
DROP TABLE if exists iceberg.kernel_db01.part;

CREATE TABLE iceberg.kernel_db01.part
as 
    select partkey, mfgr, brand, type, size, size as size_cp
    from tpch.tiny.part;
```


# 解决 where 中等价问题
- where a=b and a=1
- where a=b and b=1

## mv定义
```sql
create or replace materialized view iceberg.kernel_db01.mv_part_02 as
SELECT mfgr mfgr2, brand, type type2, size size2
from iceberg.kernel_db01.part
where  1=1 
        and 2=2 
        and mfgr='Manufacturer#1' 
        and size=7 
        and size=size_cp
GROUP BY mfgr, brand, type, size;

refresh MATERIALIZED VIEW iceberg.kernel_db01.mv_part_02;
```

## 测试sql, 预期能够替换
```sql
SELECT iceberg.kernel_db01.part.mfgr, brand, type, count(1) as _cnt
from iceberg.kernel_db01.part
where 1=1
     and mfgr=mfgr
     and 'Manufacturer#1'=mfgr
     and (brand='Brand#14' and size_cp=7)
     and size_cp=size
GROUP BY mfgr, brand, type
```
