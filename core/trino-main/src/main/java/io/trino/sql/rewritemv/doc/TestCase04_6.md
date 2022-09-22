# 解决 groupBy having
- 前提: mv没有 groupBy, having => having, 函数支持 max/min/sum/avg/count

# base_table & mv
```sql
DROP TABLE if exists iceberg.kernel_db01.part04_6;

CREATE TABLE iceberg.kernel_db01.part04_6
as
select partkey, mfgr, brand, type, size, retailprice, container
from tpch.tiny.part;


-- mv
create or replace materialized view iceberg.kernel_db01.mv_part04_6 as
SELECT 	mfgr mfgr2, 
        iceberg.kernel_db01.part04_6.brand, 
        type, 
        size, 
        retailprice, 
        container
from iceberg.kernel_db01.part04_6
;

refresh MATERIALIZED VIEW iceberg.kernel_db01.mv_part04_6;

select count(1) from iceberg.kernel_db01.mv_part04_6;
select * from iceberg.kernel_db01.mv_part04_6;
-- drop materialized view iceberg.kernel_db01.mv_part04_6 ;
-- drop table iceberg.kernel_db01.part04_6;
```

## 测试sql
```sql
-- max/min/avg
SELECT mfgr, brand, size -- , avg(retailprice) as avg_price
from iceberg.kernel_db01.part04_6
where size between 30 and 32
GROUP BY mfgr, brand,size
having max(retailprice) < 1890
   and min(retailprice) > 910
   and avg(retailprice) < 1390

-- 手动改写, mv无groupBy
SELECT mfgr2, brand, size -- , sum(sum_price)/sum(cnt_star) as avg_price
from iceberg.kernel_db01.mv_part04_6
where size>=30 and size <=32
GROUP BY mfgr2, brand,size
having max(retailprice) < 1890
   and min(retailprice) > 910
   and avg(retailprice) < 1390
```
