{{ config(materialized='ephemeral') }}
with cte as (select *
 from {{ ref('src_zone')}}),

NS AS (
  select 1 as n union all
  select 2 union all
  select 3 union all
  select 4 union all
  select 5 union all
  select 6 union all
  select 7 union all
  select 8 union all
  select 9 union all
  select 10
)
select*,
  TRIM(SPLIT_PART(B.new_id, ',', NS.n)) AS id
from NS
inner join cte B ON NS.n <= REGEXP_COUNT(B.new_id, ',') + 1

