# 解决 groupBy having
- select 字段

# base_table & mv
```sql
USE iceberg.kernel_db01;

DROP TABLE if exists part05_1;

CREATE TABLE part05_1
as
select partkey, mfgr, brand, type, size, retailprice, container
from tpch.tiny.part;


-- mv
create or replace materialized view mv_part05_1 as
SELECT 	mfgr mfgr2, 
        part05_1.brand, 
        type, 
        size, 
        retailprice, 
        container
from part05_1;

refresh MATERIALIZED VIEW mv_part05_1;

select count(1) from mv_part05_1;
select * from mv_part05_1;
-- drop materialized view mv_part05_1 ;
-- drop table part05_1;
```

## 测试sql
```sql
set session query_rewrite_with_materialized_view_status = 2;

-- max/min/avg
SELECT mfgr, brand, size+1 as size_1 , (size+1)*2 size_2, avg(retailprice) as avg_price
from part05_1
where part05_1.size>=30 
    and -size<= -32
    and size in (31,32,33)
GROUP BY mfgr, brand,size
having max(retailprice) < 1890
   and min(retailprice) > 910
   and avg(retailprice) < 1390
order by avg_price desc
;
```
