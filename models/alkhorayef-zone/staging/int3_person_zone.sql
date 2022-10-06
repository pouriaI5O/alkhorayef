{{ config(materialized='ephemeral') }}
with cte as ( select * 
FROM  {{ ref('int2_time_zone')}} where class_type='person'and id is not null),
cte1 as (select min(col_timestamp)as event_time,
date,hour,cast(count(*)/60 as float) as total_minute,
shift, 
id ,
zone  
from cte 
group by date,hour,zone,shift,id
having id >0 and total_minute>0),

cte2 as (select event_time,date,hour,shift,case 
  when zone='zone1' then 'station1'
  when zone='zone2' then 'station2'
  when zone='zone3' then 'station3'
  when zone='zone4' then 'station4'
  else '0' end as station,id,total_minute,count(id) over (partition by date,hour,shift,zone) as person_count  from cte1)
select  *,sum(total_minute)over (partition by date,hour,shift,station)  as sum_total_minute
FROM  cte2 