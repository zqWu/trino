#!/bin/bash

client/trino-cli/target/trino-cli-*-executable.jar \
--server 127.0.0.1:8080 \
--debug \
--execute "
SELECT iceberg.kernel_db01.part02.mfgr, brand, type, s2
from iceberg.kernel_db01.part02
where 2=2
  and mfgr=mfgr
  and 'Manufacturer#1'=mfgr
  and (brand='Brand#14' and (s2=18 and 1+1=2) and s0+1>=10+1)
  and s1=s2 and s1=s3 and s1=s4 and s1=s5 and s1=s6 and s1=s7 and s1=s0
  and s0 between 10 and 40
  and type like 'LARGE%'
  and brand like 'Brand%'
  and brand not in ('haha')
  and s0+1 in (1,2,19)
GROUP BY mfgr, brand, type, s2
;
"


<<comment
--
--
comment
