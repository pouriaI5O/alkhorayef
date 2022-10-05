class_type,zone,new_id,count(*) as ct ,
EXTRACT(HOUR FROM col_timestamp) AS Hour,
          EXTRACT(MINUTE FROM col_timestamp) AS Minute,
          EXTRACT(SECOND FROM col_timestamp) AS Second, 
          DATE(col_timestamp) as Date
FROM {{ source('public','alkhorayef_zone') }} 
group by col_timestamp ,new_id,zone,class_type
having ct >5