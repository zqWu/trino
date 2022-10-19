#!/bin/bash

client/trino-cli/target/trino-cli-*-executable.jar \
--server 127.0.0.1:8080 \
--debug \
--execute "
set session query_rewrite_with_materialized_view_status = 2;

SELECT mfgr, brand, type
from iceberg.kernel_db01.part01_1
    where 1=1
    and mfgr=mfgr
    and 'Manufacturer#1'=mfgr
    and (brand='Brand#14' and size = 7)
GROUP BY mfgr, brand, type
;
"


<<comment
--
--
comment
