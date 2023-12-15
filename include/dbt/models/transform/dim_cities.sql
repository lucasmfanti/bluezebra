SELECT DISTINCT
    id AS city_id,
    name,
    country_code,
    latitude,
    longitude
FROM {{  source('raw_bz', 'cities') }}