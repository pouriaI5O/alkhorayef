{{ config(materialized='ephemeral') }}
with cte as (select * FROM {{ source('public','alkhorayef_cams') }} order by start_time asc),
cte1 as (SELECT *
FROM (SELECT start_time,
             end_time,
             camera,
             status,
             time_zone 
      FROM cte) 
      PIVOT (
             max(status) FOR 
             camera IN ('w1c1', 'w1c2', 'w2c3', 'w2c4', 'w3c5', 'w3c6', 'w4c7', 'w4c8', 'c9', 'c10')
)),
cte2 as( select * ,
           datediff(second,start_time,end_time) as total_second,
           case 
               when w1c1='idle' and w1c2='downtime' then 0.25
               when w1c1='productive' and w1c2='downtime' then 0.5
               when w1c1='downtime' and w1c2='downtime' then 0
               when w1c1='idle' and w1c2='idle' then 0
               when w1c1='downtime' and w1c2='idle' then 0.25
               when w1c1='productive' and w1c2='idle' then 0.75
               when w1c1='idle' and w1c2='productive' then 0.75
               when w1c1='productive' and w1c2='productive' then 1
               when w1c1='downtime' and w1c2='productive' then 0.5
               else -1 end as station1,
            case 
               when w2c3='idle' and w2c4='downtime' then 0.25
               when w2c3='productive' and w2c4='downtime' then 0.5
               when w2c3='downtime' and w2c4='downtime' then 0
               when w2c3='idle' and w2c4='idle' then 0
               when w2c3='downtime' and w2c4='idle' then 0.25
               when w2c3='productive' and w2c4='idle' then 0.75
               when w2c3='idle' and w2c4='productive' then 0.75
               when w2c3='productive' and w2c4='productive' then 1
               when w2c3='downtime' and w2c4='productive' then 0.5
                else -1 end as station2,
            case 
               when w3c5='idle' and w3c6='downtime ' then 0.25
               when w3c5='productive' and w3c6='downtime ' then 0.5
               when w3c5='downtime' and w3c6='downtime ' then 0
               when w3c5='idle' and w3c6='idle ' then 0
               when w3c5='downtime' and w3c6='idle ' then 0.25
               when w3c5='productive' and w3c6='idle' then 0.75
               when w3c5='idle' and w3c6='productive' then 0.75
               when w3c5='productive' and w3c6='productive ' then 1
               when w3c5='downtime' and w3c6='productive' then 0.5
                else -1 end as station3,
            case 
               when w4c7='idle' and w4c8='downtime ' then 0.25
               when w4c7='productive' and w4c8='downtime' then 0.5
               when w4c7='downtime' and w4c8='downtime' then 0
               when w4c7='idle' and w4c8='idle' then 0
               when w4c7='downtime' and w4c8='idle' then 0.25
               when w4c7='productive' and w4c8='idle' then 0.75
               when w4c7='idle' and w4c8='productive' then 0.75
               when w4c7='productive' and w4c8='productive' then 1
               when w4c7='downtime' and w4c8='productive' then 0.5
                else -1 end as station4,
            case 
               when c9='idle' and c10='downtime ' then 0.25
               when c9='productive' and c10='downtime' then 0.5
               when c9='downtime' and c10='downtime' then 0
               when c9='idle' and c10='idle' then 0
               when c9='downtime' and c10='idle' then 0.25
               when c9='productive' and c10='idle' then 0.75
               when c9='idle' and c10='productive' then 0.75
               when c9='productive' and c10='productive' then 1
               when c9='downtime' and c10='productive' then 0.5
               else -1 end as station5           
            from cte1 order by start_time asc)
select * from cte2  