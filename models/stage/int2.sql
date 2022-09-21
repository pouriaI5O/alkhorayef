{{ config(materialized='ephemeral') }}
with cte as ( select *,
        EXTRACT(HOUR FROM end_time) AS Hour, 
        DATE(end_time) as Date
        from {{ ref('int1')}}),
cte1 as ( select date,hour,time_zone,
            avg(station1) as station1 ,
            avg(station2) as station2,
            avg(station3) as station3,
            avg(station4) as station4,
            avg(station5 ) as station5,
            min(start_time)  as first_observation,
            max(end_time)  as last_observation
            from cte group by date,hour,time_zone)
select * from cte1 order by date ,hour
