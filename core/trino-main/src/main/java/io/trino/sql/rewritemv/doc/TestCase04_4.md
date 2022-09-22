# 解决 groupBy having
- 前提: 相同groupBy, having => where

# base_table & mv
```sql
DROP TABLE if exists iceberg.kernel_db01.part04_4;

CREATE TABLE iceberg.kernel_db01.part04_4
as
select partkey, mfgr, brand, type, size, retailprice
from tpch.tiny.part;


-- mv
create or replace materialized view iceberg.kernel_db01.mv_part04_4 as
SELECT 	mfgr mfgr2,
        iceberg.kernel_db01.part04_4.brand,
        size s0,
        max(retailprice) as max_price,
        min(retailprice) as min_price,
        sum(retailprice) as sum_price,
        avg(retailprice) as avg_price,
        count(*) as cnt_star,
        count(999) as cnt_const,
        count(size) as cnt_size
from iceberg.kernel_db01.part04_4
where size between 30 and 31
GROUP BY mfgr, brand, size
;

refresh MATERIALIZED VIEW iceberg.kernel_db01.mv_part04_4;

select count(1) from iceberg.kernel_db01.mv_part04_4;
select * from iceberg.kernel_db01.mv_part04_4;
-- drop materialized view iceberg.kernel_db01.mv_part04_4 ;
-- drop table iceberg.kernel_db01.part04_4;
```

## 测试sql
```sql
-- 不同groupBy, original 多了having max(colA), 且 having字段直接存在, 可以替换
SELECT mfgr, brand
from iceberg.kernel_db01.part04_4
where size=30
GROUP BY mfgr, brand
having max(retailprice) > 1620

-- 这个是上面 手动 rewrite的结果
select mfgr2, brand
from iceberg.kernel_db01.mv_part04_4
where s0=30
GROUP BY mfgr2, brand
having max(max_price) > 1620



-- 不同groupBy, original 多了having max/min/sum
SELECT mfgr, brand
from iceberg.kernel_db01.part04_4
where size=30
GROUP BY mfgr, brand
having
    max(retailprice) < 1620
   and min(retailprice) > 1200
   and sum(retailprice) > 1300
```
