{{ config(materialized='ephemeral') }}
with cte as ( select * 
FROM  {{ ref('int2_time_zone')}} where class_type='housing'and id is not null)
select min(col_timestamp)as event_time,
date,hour,cast(count(*)/60 as float) as total_minute,
shift, 
id  ,
zone  
from cte 
group by date,hour,zone,shift,id
having id >0 and total_minute>0