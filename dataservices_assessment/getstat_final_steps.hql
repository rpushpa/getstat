use getstat;

--For second scenario creating a  table to store intermediate results

create table if not exists kw_rank1_ptn row format delimited fields terminated by ',' STORED AS textfile AS
select  keyword,crawl_dt,market,device,kw_url,dense_rank()over(partition by keyword,market,device order by kw_url) as r_kw_url 
from   partitioned_kw_rank where kw_rank=1 and dt <'2017-08-01' 
group by keyword,crawl_dt,kw_url,market,device order by keyword,market,device,crawl_dt ; 

---For third scenario creating a table to store the convergence/divergence results

create table if not exists kw_convergence_detail as 
select  pkr1.keyword, 
pkr1.market, pkr1.crawl_dt,
pkr1.kw_url as dt_url,
pkr2.kw_url as sm_url,
CASE WHEN (pkr1.kw_url= null or  pkr2.kw_url= null) then  -100 else (pkr1.kw_rank-pkr2.kw_rank)  end convergance_score
from partitioned_kw_rank pkr1 full join   
partitioned_kw_rank pkr2 
on (pkr1.keyword=pkr2.keyword and pkr1.market=pkr2.market and pkr1.crawl_dt=pkr2.crawl_dt and pkr1.kw_url=pkr2.kw_url)
where   pkr2.device='smartphone' and pkr1.device='desktop' ;