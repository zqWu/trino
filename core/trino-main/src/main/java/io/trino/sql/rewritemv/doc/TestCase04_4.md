# 解决 groupBy having
- 前提: 不同groupBy, having => having, 函数支持 max/min/sum

# base_table & mv
```sql
USE iceberg.kernel_db01;

DROP TABLE if exists part04_4;

CREATE TABLE part04_4
as
select partkey, mfgr, brand, type, size, retailprice
from tpch.tiny.part;


-- mv
create or replace materialized view mv_part04_4 as
SELECT 	mfgr mfgr2,
        part04_4.brand,
        size s0,
        max(retailprice) as max_price,
        min(retailprice) as min_price,
        sum(retailprice) as sum_price,
        avg(retailprice) as avg_price,
        count(*) as cnt_star,
        count(999) as cnt_const,
        count(size) as cnt_size
from part04_4
where size between 30 and 31
GROUP BY mfgr, brand, size
;

refresh MATERIALIZED VIEW mv_part04_4;

select count(1) from mv_part04_4;
select * from mv_part04_4;
-- drop materialized view mv_part04_4 ;
-- drop table part04_4;
```

## 测试sql
```sql
set session query_rewrite_with_materialized_view_status = 2;

-- 不同groupBy, original 多了having max(colA), 且 having字段直接存在, 可以替换
SELECT mfgr, brand
from part04_4
where size=30
GROUP BY mfgr, brand
having max(retailprice) > 1620;
-- 这个是上面 手动 rewrite的结果
select mfgr2, brand
from mv_part04_4
where s0=30
GROUP BY mfgr2, brand
having max(max_price) > 1620;


-- 不同groupBy, original 多了having max(colA), 且 having字段直接存在, 可以替换
SELECT mfgr, brand, max(retailprice) as max_price
from part04_4
where size=30
GROUP BY mfgr, brand;
-- 这个是上面 手动 rewrite的结果
select mfgr2, brand, max(max_price) as max_price
from mv_part04_4
where s0=30
GROUP BY mfgr2, brand;


-- 不同groupBy, original 多了having max/min/sum, 预期 fit
SELECT mfgr, brand
from part04_4
where size=30
GROUP BY mfgr, brand
having
    max(retailprice) < 1620
   and min(retailprice) > 1200
   and sum(retailprice) > 1300;
```
