#!/bin/bash

client/trino-cli/target/trino-cli-*-executable.jar \
--server 127.0.0.1:8080 \
--debug \
--execute "
SELECT mfgr, brand, s0, s3
from iceberg.kernel_db01.part04_2
where mfgr='Manufacturer#4'
  and s0=s1
  and s0=s2
GROUP BY mfgr, brand, s0, s3
;
"


<<comment
--
--
comment
