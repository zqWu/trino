# 解决 groupBy having
- select 字段

# base_table & mv
```sql
DROP TABLE if exists iceberg.kernel_db01.part05_1;

CREATE TABLE iceberg.kernel_db01.part05_1
as
select partkey, mfgr, brand, type, size, retailprice, container
from tpch.tiny.part;


-- mv
create or replace materialized view iceberg.kernel_db01.mv_part05_1 as
SELECT 	mfgr mfgr2, 
        iceberg.kernel_db01.part05_1.brand, 
        type, 
        size, 
        retailprice, 
        container
from iceberg.kernel_db01.part05_1
;

refresh MATERIALIZED VIEW iceberg.kernel_db01.mv_part05_1;

select count(1) from iceberg.kernel_db01.mv_part05_1;
select * from iceberg.kernel_db01.mv_part05_1;
-- drop materialized view iceberg.kernel_db01.mv_part05_1 ;
-- drop table iceberg.kernel_db01.part05_1;
```

## 测试sql
```sql
-- max/min/avg
set session query_rewrite_with_materialized_view_status = 1;

SELECT mfgr, brand, size+1 as , (size+1)*2, avg(retailprice) as avg_price
from iceberg.kernel_db01.part05_1

where iceberg.kernel_db01.part05_1.size>=30 and -size<= -32
-- and size in (select distinct size from iceberg.kernel_db01.part05_1)
  and size in (31,32,33)

GROUP BY mfgr, brand,size
having max(retailprice) < 1890
   and min(retailprice) > 910
   and avg(retailprice) < 1390
order by avg_price desc
;
```
