{{ config(
    materialized='table'
)}}
select *,'Asia/Riyadh' as time_zone FROM  {{ ref('int3_housing_zone')}}