# 单表简单处理
- mv / original 都是单表
- from 处理
- where 条件的处理(目前处理等值条件)
- select 处理
- groupBy 处理
    - 目前支持 groupBy 字段1, 字段2, ...

## 测试点
- where
    - 1=1, 2=2能够去掉
    - mfgr='Manufacturer#1' 和 'Manufacturer#1'=mfgr 能够处理, 仅是=左右两边换位置
    - and(A and B) 能够处理成 and A and B
    - 能够处理 逻辑相同的不同字段(size, size_2)

- groupBy 能够处理
    - 相同 groupBy
    - 更少的 groupBy


# base_table & mv
```sql
USE iceberg.kernel_db01;

DROP TABLE if exists part01_1;

CREATE TABLE part01_1
as select * from tpch.tiny.part;
-- select count(*) from part01_1; -- 2000

-- mv定义
create or replace materialized view mv_part01_1 as
SELECT mfgr mfgr2, brand, type type2, size size2
from part01_1
where 1=1 and 2=2 and mfgr='Manufacturer#1'
GROUP BY mfgr, brand, type, size;

refresh MATERIALIZED VIEW mv_part01_1;

-- select * from mv_part01_1;
-- drop materialized view mv_part01_1 ;
-- drop table part01_1;
```

## 测试sql, 预期能够替换
```sql
set session query_rewrite_with_materialized_view_status = 2;

-- sql1
SELECT mfgr, brand, type
from part01_1
where 1=1
  and mfgr=mfgr
  and 'Manufacturer#1'=mfgr
  and (brand='Brand#14' and size = 7)
GROUP BY mfgr, brand, type
;

-- sql2
SELECT mfgr, brand, type
from part01_1
    where 1=1
    and mfgr=mfgr
    and 'Manufacturer#1'=mfgr
    and (brand='Brand#14' and size = 7)
GROUP BY mfgr, brand, type,size
;
```

