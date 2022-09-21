{{ config(materialized='ephemeral') }}
with cte as (SELECT * 
FROM (SELECT date,first_observation,last_observation,hour,time_zone,station1, station2, station3,station4,station5 
FROM {{ ref('int2')}}) UNPIVOT (
    productivity for  station  IN (station1, station2, station3,station4,station5 )

))
select *,(productivity * 60) as productivity_minute from cte order by date,hour,station