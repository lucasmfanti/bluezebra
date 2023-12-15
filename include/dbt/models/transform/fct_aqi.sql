WITH cte AS (
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['city_id', 'extracted_at']) }} AS aqi_id,
        extracted_at AS datetime_id,
        city_id,
        air_quality,
        no AS no_concentration,
        no2 AS no2_concentration,
        o3 AS o3_concentration,
        so2 AS so2_concentration,
        pm2_5 AS pm2_5_concentration,
        pm10 AS pm10_concentration,
        nh3 AS nh3_concentration,
    FROM {{ source('raw_bz', 'aqi') }}
)

SELECT
    aqi_id,
    dt.datetime_id,
    dc.city_id,
    air_quality,
    no_concentration,
    no2_concentration,
    o3_concentration,
    so2_concentration,
    pm2_5_concentration,
    pm10_concentration,
    nh3_concentration
FROM cte fa
INNER JOIN {{ ref('dim_datetime') }} dt ON fa.datetime_id = dt.datetime_id
INNER JOIN {{ ref('dim_cities') }} dc ON fa.city_id = dc.city_id