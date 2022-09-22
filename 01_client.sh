#!/bin/bash

client/trino-cli/target/trino-cli-*-executable.jar \
--server 127.0.0.1:8080 \
--debug \
--execute "
SELECT mfgr, brand, size -- , avg(retailprice) as avg_price
from iceberg.kernel_db01.part04_6
where size between 30 and 32
GROUP BY mfgr, brand,size
having max(retailprice) < 1890
   and min(retailprice) > 910
   and avg(retailprice) < 1390
;
"


<<comment
--
--
comment
