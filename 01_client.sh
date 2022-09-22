#!/bin/bash

client/trino-cli/target/trino-cli-*-executable.jar \
--server 127.0.0.1:8080 \
--debug \
--execute "
SELECT mfgr, brand, size, count(*) as _cnt
from iceberg.kernel_db01.part04_3
where size=30
GROUP BY mfgr, brand, size
having max(retailprice+retailprice) > 3200
;
"


<<comment
--
--
comment
