{{ config(materialized='ephemeral') }}
with cte as (select class_type,zone,id,
EXTRACT(HOUR FROM col_timestamp) AS Hour,
          EXTRACT(MINUTE FROM col_timestamp) AS Minute,
          EXTRACT(SECOND FROM col_timestamp) AS Second, 
          DATE(col_timestamp) as Date,col_timestamp,
          cast(col_timestamp AS time) as Time,
          EXTRACT(dayofweek FROM col_timestamp) AS weekday
FROM  {{ ref('int1_zone')}}

)
select *,
  case 
     when  time >='07:00:00' and time <'16:30:00' then 'day'
     when  time >='16:30:00' and time <'18:30:00' then 'day_ot'
     when (weekday>6 or weekday<5) and ( (time >='18:30:00' and time <='23:59:59') or (time >='00:00:00' and time <'04:00:00')) then 'night'
     when (weekday>4 or weekday<7) and ( (time >='18:30:00' and time <='23:59:59') or (time >='00:00:00' and time <'04:00:00')) then 'wk_night_Ot'
     when (weekday>6 or weekday<5) and ( time >='04:00:00' and time <'06:00:00') then 'night_ot'
     when (weekday>6 or weekday<5) and ( time >='06:00:00' and time <'07:00:00') then 'handover'
     when (weekday>4 or weekday<7) and  ( time >='06:00:00' and time <'07:00:00') then 'weekend_ot'
     else 'p' end as shift

 from cte