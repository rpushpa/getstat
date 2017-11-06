use getstat;


set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=500;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.created.files=50000;


 ---cannot create the partitions since I am using single node cluster which has restrictions in creating partitons and space issue.
 ----   So just creating on date partition.
----CREATE TABLE if not exists partitioned_kw_rank(
----keyword string,
---market string,
----location string,
----device string,
----crawl_dt  date,
----kw_rank TINYINT,
----kw_URL string
----)
----PARTITIONED BY (dt date,rank_part TINYINT);





----insert overwrite table partitioned_kw_rank
----partition (dt,rank_part)
---select kr.*,crawl_dt,kw_rank
---from stg_keyword_rank kr where kr.crawl_dt is not null and kw_rank is not null ; 





CREATE TABLE if not exists partitioned_kw_rank(
keyword string,
market string,
location string,
device	  string,
crawl_dt  date,
kw_rank TINYINT,
kw_URL string
)
PARTITIONED BY (dt date);

insert overwrite table partitioned_kw_rank
partition (dt)
select kr.*,crawl_dt 
from stg_keyword_rank kr where kr.crawl_dt is not null;

