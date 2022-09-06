# mv 定义
```sql
create or replace materialized view iceberg.kernel_db01.mv_agg_nation
WITH (  
   format_version=2,
   format='PARQUET'
) 
as
SELECT regionkey, count(1) AS num_country
from iceberg.kernel_db01.nation n
GROUP BY regionkey;

refresh MATERIALIZED VIEW iceberg.kernel_db01.mv_agg_nation;
```

- sql定义
```sql
SELECT regionkey, count(1) AS num_country
from iceberg.kernel_db01.nation n
GROUP BY regionkey;
```

