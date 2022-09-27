#!/bin/bash

client/trino-cli/target/trino-cli-*-executable.jar \
--server 127.0.0.1:8080 \
--debug \
--execute "
set session query_rewrite_with_materialized_view_status = 1;

SELECT mfgr mfgr2, brand, type type2, size
from iceberg.kernel_db01.part03_3
where 1=1
  and mfgr='Manufacturer#1'
  and size>=30 and size<40
  and brand like 'Brand#2%'
GROUP BY mfgr, brand, type, size;
;
"


<<comment
--
--
comment
