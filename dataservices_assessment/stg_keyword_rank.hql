----- create stage table 

use getstat;

CREATE TABLE if not exists stg_keyword_rank(
keyword string,
market string,
location string,
device	  string,
crawl_dt  string,
kw_rank TINYINT,
kw_URL string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ","
tblproperties("skip.header.line.count"="1");


LOAD DATA INPATH '${hiveconf:file_path}' OVERWRITE INTO TABLE stg_keyword_rank;
