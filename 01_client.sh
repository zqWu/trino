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
  and brand='Brand#14'
  and s1=s2 and s1=s3 and s1=s4 and s1=s5 and s1=s6 and s1=s7 and s1=s0
  and s0>10
GROUP BY mfgr, brand, type, s2
;
"


<<comment
--
SELECT mfgr, brand, type, count(1) as _cnt
from iceberg.kernel_db01.part
where 1=1
    and mfgr = mfgr
    and 'Manufacturer#1' = mfgr
    and (iceberg.kernel_db01.part.brand='Brand#14' and size = 7 and type=brand)
GROUP BY mfgr, brand, type
--
SELECT mfgr, brand , type, count(1) as _cnt
from iceberg.kernel_db01.part
where mfgr = 'Manufacturer#1'
GROUP BY mfgr, brand, type
--
SELECT regionkey, count(1) AS num_country
from iceberg.kernel_db01.nation kk
where 1=1 and 2=2
    and kk.regionkey=1 and 1=kk.regionkey and regionkey=2 and regionkey=kk.regionkey
GROUP BY regionkey
--
insert into iceberg.kernel_db01.region values (5, 'GG', 'a')
--
select * from iceberg.tpch_tiny.region;
--
use tpch.tiny;
with t1 as (select name ,regionkey from region)
select n.name, t1.name as r_name
from nation n, t1
where n.regionkey=t1.regionkey;
--
use tpch.tiny;
with t1 as (select name ,regionkey from region)
select n.name, t1.name as r_name
from nation n, t1
where n.regionkey=t1.regionkey;
--
comment
