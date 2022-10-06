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
  else '0' end as station,id,total_minute,count(id) over (partition by date,hour,shift,zone) as count_person  from cte1),
cte3 as (select  *,sum(total_minute)over (partition by date,hour,shift,station)  as sum_total_minute
FROM  cte2 ),
cte4 as (select*,case when shift in ('wk_night_Ot','day_ot','night_ot','weekend_ot') then sum_total_minute else 0 end 
as over_time from cte3),
cte5 as (select *,sum(over_time) over (partition by date,station) as daily_over_time from cte4)
select event_time,date,hour,shift,station,id,total_minute,count_person,sum_total_minute,daily_over_time,((daily_over_time/60)*1.5*9.2) as over_time_cost from cte5