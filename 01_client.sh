#!/bin/bash

client/trino-cli/target/trino-cli-*-executable.jar \
--server 127.0.0.1:8080 \
--debug \
--execute "
SELECT mfgr, brand
from iceberg.kernel_db01.part04_5
GROUP BY mfgr, brand
having max(retailprice) > 1620;
;
"


<<comment
--
--
comment
