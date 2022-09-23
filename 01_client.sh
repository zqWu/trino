#!/bin/bash

client/trino-cli/target/trino-cli-*-executable.jar \
--server 127.0.0.1:8080 \
--debug \
--execute "
set session query_rewrite_with_materialized_view_status = 1;

SELECT mfgr, brand, size, avg(retailprice) as avg_price
from iceberg.kernel_db01.part04_6

where iceberg.kernel_db01.part04_6.size>=30 and -size<= -32
-- and size in (select distinct size from iceberg.kernel_db01.part04_6)
and size in (31,32,33)

GROUP BY mfgr, brand,size
having max(retailprice) < 1890
   and min(retailprice) > 910
   and avg(retailprice) < 1390
order by avg_price desc
;
"


<<comment
--
--
comment
