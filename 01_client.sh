#!/bin/bash

client/trino-cli/target/trino-cli-*-executable.jar \
--server 127.0.0.1:8080 \
--debug \
--execute "
use tpch.tiny;
with t1 as (select name ,regionkey from region)
select n.name, t1.name as r_name
from nation n, t1
where n.regionkey=t1.regionkey;
"


<<comment
--
select * from iceberg.tpch_tiny.region;
--
use tpch.tiny;
with t1 as (select name ,regionkey from region)
select n.name, t1.name as r_name
from nation n, t1
where n.regionkey=t1.regionkey;
--
comment
