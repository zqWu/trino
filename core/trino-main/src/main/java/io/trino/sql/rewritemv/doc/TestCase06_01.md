# 单表简单处理
- 跨 catalog 的 materialized view
- 目前不支持, 支持创建, 但是不支持 refresh
- https://github.com/trinodb/trino/issues/13131
  - 目前社区定位为 bug



# sql
```
CREATE TABLE mysql.db01.part01_1
as select * from tpch.tiny.part;

create or replace materialized view iceberg.kernel_db01.mv_part06_1 as
SELECT mfgr, brand, type, size
from mysql.db01.part01_1
where mfgr='Manufacturer#1';


-- SQL Error [13]: Query failed (#20221013_021610_00045_ngd8x): Cross connector materialized views are not supported
refresh MATERIALIZED VIEW iceberg.kernel_db01.mv_part06_1;
```
