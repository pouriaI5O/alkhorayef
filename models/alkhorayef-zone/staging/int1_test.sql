{{ config(materialized='ephemeral') }}
with cte as (select
    *,
     lead(col_timestamp) over(partition by class_type,zone order by col_timestamp)as new_time
from {{ ref('src_zone')}} )
select col_timestamp,class_type,type_count,zone,new_id,new_time from cte  where new_time is not null order by col_timestamp desc 