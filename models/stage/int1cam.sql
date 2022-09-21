{{ config(materialized='ephemeral') }}
with cte as ( select *,
        EXTRACT(HOUR FROM end_time) AS Hour, 
        DATE(end_time) as Date
        from {{ source('public','alkhorayef_cams') }}),
cte1 as (select *,datediff(second,start_time,end_time) as total_second from cte),
cte2 as( select date,time_zone ,min(start_time) as first_observation,max(end_time)as last_observation ,hour,status,camera,sum(total_second) as total_second from cte1
 group by date,hour,status,camera,time_zone  ),
 cte3 as (select date,time_zone ,first_observation,last_observation,hour,status,camera,round((cast(total_second as decimal(8,2))/60),2) as camera_minute from cte2 )
 select * from cte3
 

