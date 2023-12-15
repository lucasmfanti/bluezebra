WITH cte AS (  
  SELECT DISTINCT
    extracted_at AS datetime_id,
    DATETIME(extracted_at) AS datetime
  FROM {{ source('raw_bz', 'aqi') }}
)

SELECT
  datetime_id,
  datetime,
  EXTRACT(YEAR FROM datetime) AS year,
  EXTRACT(MONTH FROM datetime) AS month,
  EXTRACT(DAY FROM datetime) AS day,
  EXTRACT(HOUR FROM datetime) AS hour,
  EXTRACT(DAYOFWEEK FROM datetime) AS weekday
FROM cte