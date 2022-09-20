#!/bin/bash

client/trino-cli/target/trino-cli-*-executable.jar \
--server 127.0.0.1:8080 \
--debug \
--execute "
SELECT mfgr mfgr2, brand, type type2, size
from iceberg.kernel_db01.part03_2
where 1=1
  and mfgr='Manufacturer#1'
  and size>=10 and size<40
GROUP BY mfgr, brand, type, size;
;
"


<<comment
--
--
comment
