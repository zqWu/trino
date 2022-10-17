#!/bin/bash

client/trino-cli/target/trino-cli-*-executable.jar \
--server 127.0.0.1:8080 \
--debug \
--execute "
set session query_rewrite_with_materialized_view_status = 1;

SELECT mfgr, brand, max(retailprice) as max_price
from iceberg.kernel_db01.part04_4
where size=30
GROUP BY mfgr, brand
;
"


<<comment
--
--
comment
