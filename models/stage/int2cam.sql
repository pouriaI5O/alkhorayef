{{ config(materialized='ephemeral') }}
with cte as (select*,
case 
    when camera='w1c1' or camera='w1c2' then 'station1'
    when camera='w2c3' or camera='w2c4' then 'station2'
    when camera='w3c5' or camera='w3c6' then 'station3'
    when camera='w4c7' or camera='w4c8' then 'station4'
    when camera='c9' or camera='c10' then 'station5'
    else null end as station
from {{ ref('int1cam')}})
select * from cte 