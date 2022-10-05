with cte  as (select id as p ,class_type,zone,minute
FROM  {{ ref('int2_time_zone')}}
where hour=3 and date='2022-10-04' )
select p,minute  from cte where (class_type='housing' and zone='zone2') and p=3