{{ config(materialized='ephemeral') }}
select col_timestamp,class_type,zone,new_id
FROM {{ source('public','alkhorayef_zone') }} 
group by col_timestamp,class_type,zone,new_id