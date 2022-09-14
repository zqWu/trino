# base_table
```sql
DROP TABLE if exists iceberg.kernel_db01.part;

CREATE TABLE iceberg.kernel_db01.part
as select * from tpch.tiny.part;
-- select count(*) from iceberg.kernel_db01.part; -- 2000
```


# 单表简单处理
- mv / original 都是单表
- from 处理
- where 条件的处理(目前处理等值条件)
- select 处理
- groupBy 处理 
  - 目前支持 groupBy 字段1, 字段2, ...

## mv定义
```sql
create or replace materialized view iceberg.kernel_db01.mv_part_01 as
SELECT mfgr mfgr2, brand, type type2, size size2
from iceberg.kernel_db01.part
where 1=1 and 2=2 and mfgr='Manufacturer#1'
GROUP BY mfgr, brand, type, size;

refresh MATERIALIZED VIEW iceberg.kernel_db01.mv_part_01;

select * from iceberg.kernel_db01.mv_part_01;
```

## 测试sql, 预期能够替换
```sql
-- sql1
SELECT mfgr, brand, type, count(1) as _cnt
from iceberg.kernel_db01.part
    where 1=1
    and mfgr=mfgr
    and 'Manufacturer#1'=mfgr
    and (brand='Brand#14' and size = 7)
GROUP BY mfgr, brand, type
;

-- sql2
SELECT mfgr, brand, type
from iceberg.kernel_db01.part
    where 1=1
    and mfgr=mfgr
    and 'Manufacturer#1'=mfgr
    and (brand='Brand#14' and size = 7)
GROUP BY mfgr, brand, type,size
;
```
## 测试点
- where
    - 1=1, 2=2能够去掉
    - mfgr='Manufacturer#1' 和 'Manufacturer#1'=mfgr 能够处理, 仅是=左右两边换位置
    - and(A and B) 能够处理成 and A and B
    - 能够处理 逻辑相同的不同字段(size, size_2)

- groupBy 能够处理
  - 相同 groupBy
  - 更少的 groupBy
