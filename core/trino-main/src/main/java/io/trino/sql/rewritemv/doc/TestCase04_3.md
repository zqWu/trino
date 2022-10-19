# 解决 groupBy having
- 前提: 相同groupBy, having => where

# base_table & mv
```sql
USE iceberg.kernel_db01;

DROP TABLE if exists part04_3;

CREATE TABLE part04_3
as
select partkey, mfgr, brand, type, size, retailprice
    from tpch.tiny.part;


-- mv
create or replace materialized view mv_part04_3 as
SELECT 	mfgr mfgr2,
        part04_3.brand,
        size s0,
        max(retailprice) as max_price,
        min(retailprice) as min_price,
        sum(retailprice) as sum_price,
        avg(retailprice) as avg_price,
        count(*) as cnt_star,
        count(999) as cnt_const,
        count(size) as cnt_size
from part04_3
where size between 30 and 31
GROUP BY mfgr, brand, size
;

refresh MATERIALIZED VIEW mv_part04_3;

select count(1) from mv_part04_3;
select * from mv_part04_3;
-- drop materialized view mv_part04_3 ;
-- drop table part04_3;
```

## 测试sql
```sql
set session query_rewrite_with_materialized_view_status = 2;

-- 相同groupBy, original 多了having max(colA), 且 having字段直接存在, 可以替换
SELECT mfgr, brand, size
from part04_3
where size=30
GROUP BY mfgr, brand, size
having max(retailprice) > 1620;

-- 这个是上面 手动 rewrite的结果
select mfgr2, brand, s0
from mv_part04_3
where s0=30 and max_price > 1620;


-- max/min
SELECT mfgr, brand, size
from part04_3
where size=30
GROUP BY mfgr, brand, size
having max(retailprice) < 2620 and min(retailprice) > 1400;
-- 这个是上面 手动 rewrite的结果
select mfgr2, brand, s0
from mv_part04_3
where s0=30 and max_price < 2620 and min_price > 1400;



-- max/min/count
SELECT mfgr, brand, size
from part04_3
where size=30
GROUP BY mfgr, brand, size
having
    max(retailprice) < 2620
   and min(retailprice) > 1200
   and count(*) > 1
   and count(1) > 1
   and count(size) > 1;
-- 手动改写
select mfgr2, brand, s0
from mv_part04_3
where s0=30 
  and max_price < 2620 and min_price > 1200
  and cnt_star > 1 and cnt_const > 1 and cnt_size > 1;


-- max/min/count/avg
SELECT mfgr, brand, size
from part04_3
where size=30
GROUP BY mfgr, brand, size
having
    max(retailprice) < 2600
   and min(retailprice) > 1200
   and count(*) > 1
   and avg(retailprice) > 1400;
-- 手动改写
select mfgr2, brand, s0
from mv_part04_3
where s0=30
  and max_price < 2600
  and min_price > 1200
  and cnt_star > 1
  and avg_price > 1400;


-- max(size), 预期无法改写, 因为mv 没有 select max(size)
SELECT mfgr, brand, size
from part04_3
where size between 30 and 31
GROUP BY mfgr, brand, size
having max(size) > 30
```
