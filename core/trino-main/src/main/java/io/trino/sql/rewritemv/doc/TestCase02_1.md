# 解决 where 中等价问题
- 各种名字 normalize
- where a=b and a=1
- where a=b and b=1

## 测试点
- where
    - equivalent class (size, size_cp)

# base_table
```sql
DROP TABLE if exists iceberg.kernel_db01.part02_1;

CREATE TABLE iceberg.kernel_db01.part02_1
as 
    select partkey, mfgr, brand, type, size, size as size_cp
    from tpch.tiny.part;

-- mv定义
create or replace materialized view iceberg.kernel_db01.mv_part02_1 as
SELECT mfgr mfgr2, brand, type type2, size size2
from iceberg.kernel_db01.part02_1
where  1=1 
        and 2=2 
        and mfgr='Manufacturer#1' 
        and size=7 
        and size=size_cp
GROUP BY mfgr, brand, type, size;

refresh MATERIALIZED VIEW iceberg.kernel_db01.mv_part02_1;

-- select * from iceberg.kernel_db01.mv_part02_1;
-- drop materialized view iceberg.kernel_db01.mv_part02_1 ;
-- drop table iceberg.kernel_db01.part02_1;
```

## 测试sql, 预期能够替换
```sql
set session query_rewrite_with_materialized_view_status = 2;

SELECT iceberg.kernel_db01.part02_1.mfgr, brand, type
from iceberg.kernel_db01.part02_1
where 1=1
     and mfgr=mfgr
     and 'Manufacturer#1'=mfgr
     and (brand='Brand#14' and size_cp=7)
     and size_cp=size
GROUP BY mfgr, brand, type
;
```


