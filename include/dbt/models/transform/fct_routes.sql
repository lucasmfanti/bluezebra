WITH cte AS (
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['origin_city_id', 'destination_city_id', 'extracted_at']) }} AS route_id,
        extracted_at AS datetime_id,
        origin_city_id,
        destination_city_id,
        distance,
        duration
    FROM {{ source('raw_bz', 'routes') }}
)

SELECT
    route_id,
    dt.datetime_id,
    dc1.city_id AS origin_city_id,
    dc2.city_id AS destination_city_id,
    distance,
    duration
FROM cte fr
INNER JOIN {{ ref('dim_datetime') }} dt ON fr.datetime_id = dt.datetime_id
INNER JOIN {{ ref('dim_cities') }} dc1 ON fr.origin_city_id = dc1.city_id
INNER JOIN {{ ref('dim_cities') }} dc2 ON fr.destination_city_id = dc2.city_id