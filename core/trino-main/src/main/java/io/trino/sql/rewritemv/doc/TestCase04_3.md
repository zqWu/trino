# 解决 groupBy having
- mv有 groupBy, query有 groupBy, 且 groupBy 能fit
- mv没 having, query有 having

# base_table & mv
```sql
create or replace materialized view iceberg.kernel_db01.mv_part04_3 as
SELECT 	mfgr mfgr2,
        iceberg.kernel_db01.part04_3.brand,
        size s0,
        max(retailprice) as max_price,
        min(retailprice) as min_price,
        sum(retailprice) as sum_price,
        count(*) as _cnt
from iceberg.kernel_db01.part04_3
where size between 30 and 31
GROUP BY mfgr, brand, size
;

refresh MATERIALIZED VIEW iceberg.kernel_db01.mv_part04_3;

select count(1) from iceberg.kernel_db01.mv_part04_3;
select * from iceberg.kernel_db01.mv_part04_3;
-- drop materialized view iceberg.kernel_db01.mv_part04_3 ;
-- drop table iceberg.kernel_db01.part04_3;
```

## 测试sql, 预期能够替换
```sql
-- 相同groupBy, 多having, 且 having字段直接存在
SELECT mfgr, brand, size, count(*) as _cnt
from iceberg.kernel_db01.part04_3
where size=30
GROUP BY mfgr, brand, size
having max(retailprice) > 1600

-- 这个是上面 手动 rewrite的结果
select mfgr2, brand, s0
from iceberg.kernel_db01.mv_part04_3
where s0=30 and max_price > 1600
  
```
