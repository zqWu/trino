# 解决 groupBy having
- 前提: 不同groupBy, having => having, 函数支持 max/min/sum/avg/count

# base_table & mv
```sql
DROP TABLE if exists iceberg.kernel_db01.part04_5;

CREATE TABLE iceberg.kernel_db01.part04_5
as
select partkey, mfgr, brand, type, size, retailprice, container
from tpch.tiny.part;


-- mv
create or replace materialized view iceberg.kernel_db01.mv_part04_5 as
SELECT 	mfgr mfgr2,
          iceberg.kernel_db01.part04_5.brand,
          sum(size) as sum_size,
          count(size) as cnt_size,
          max(retailprice) as max_price,
          min(retailprice) as min_price,
          sum(retailprice) as sum_price,
          avg(retailprice) as avg_price,
          count(*) as cnt_star,
          count(999) as cnt_const
from iceberg.kernel_db01.part04_5
GROUP BY mfgr, brand, container
;

refresh MATERIALIZED VIEW iceberg.kernel_db01.mv_part04_5;

select count(1) from iceberg.kernel_db01.mv_part04_5;
select * from iceberg.kernel_db01.mv_part04_5;
-- drop materialized view iceberg.kernel_db01.mv_part04_5 ;
-- drop table iceberg.kernel_db01.part04_5;
```

## 测试sql
```sql
-- 不同groupBy, original 多了having max(colA), 且 having字段直接存在, 可以替换
SELECT mfgr, brand
from iceberg.kernel_db01.part04_5
GROUP BY mfgr, brand
having max(retailprice) > 1620;
-- 手动改写
select mfgr2, brand
from iceberg.kernel_db01.mv_part04_5
GROUP BY mfgr2, brand
having max(max_price) > 1620





-- 不同groupBy, original 多了having max/min
SELECT mfgr, brand, avg(retailprice) as avg_price
from iceberg.kernel_db01.part04_5
GROUP BY mfgr, brand
having max(retailprice) < 1890
   and min(retailprice) > 910
   and avg(retailprice) < 1390
-- 手动改写
SELECT mfgr2, brand, sum(sum_price)/sum(cnt_star) as avg_price
from iceberg.kernel_db01.mv_part04_5
GROUP BY mfgr2, brand
having max(max_price) < 1890
   and min(min_price) > 910
   and sum(sum_price)/sum(cnt_star) < 1390


-- max/min/avg
SELECT mfgr, brand -- , avg(retailprice) as avg_price
from iceberg.kernel_db01.part04_5
GROUP BY mfgr, brand
having max(retailprice) < 1890
   and min(retailprice) > 910
   and avg(retailprice) < 1390
-- 手动改写
SELECT mfgr2, brand -- , sum(sum_price)/sum(cnt_star) as avg_price
from iceberg.kernel_db01.mv_part04_5
GROUP BY mfgr2, brand
having max(max_price) < 1890
   and min(min_price) > 910
   and sum(sum_price)/sum(cnt_star) < 1390
```
