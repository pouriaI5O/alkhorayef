{{ config(
    materialized='table'
)}}
select* from {{ ref('int2cam')}} order by date,hour,camera,station