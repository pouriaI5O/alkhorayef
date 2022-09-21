{{ config(
    materialized='table'
)}}
select* from {{ ref('int3')}} order by date,hour,station