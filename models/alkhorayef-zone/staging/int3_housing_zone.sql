{{ config(materialized='ephemeral') }}
with cte as ( select * 
FROM  {{ ref('int2_time_zone')}} where class_type='housing'and id is not null),
cte1 as (select min(col_timestamp)as event_time,
date,hour,cast(count(*)/60 as float) as total_minute,
shift, 
id  ,
zone  
from cte 
group by date,hour,zone,shift,id
having id >0 and total_minute>0
order by zone ,event_time),
cte2 as (select  *,sum(total_minute)over (partition by date,hour,shift,zone)  as sum_total_minute
FROM  cte1),
cte3 as ( select  *,count(id)over (partition by date,hour,shift,zone) as count_housing  from cte2 )
select event_time,date,hour,shift,
case 
  when zone='zone1' then 'station1'
  when zone='zone2' then 'station2'
  when zone='zone3' then 'station3'
  when zone='zone4' then 'station4'
  else '0' end as station,id,total_minute,sum_total_minute,count_housing ,
sum_total_minute/count_housing  as capacity from cte3