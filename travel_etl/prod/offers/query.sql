REPLACE INTO `%(target)s`
SELECT src.hotel_id AS hotel_id,
    src.title AS title,
    src.country_name AS country_name,
    src.city_name AS city_name,
    src.price AS price,
    src.airport_distance AS airport_distance,
    src.sand_beach_flg AS sand_beach_flg,
    src.start_date AS start_date,
    src.end_date AS end_date,
    src.rating AS rating,
    src.num_nights AS num_nights,
    src.room_type AS room_type,
    src.mealplan AS mealplan,
    src.is_flight_included AS is_flight_included,
    src.beach_line AS beach_line,
    src.num_stars AS num_stars,
    src.price - coalesce(src_ft.price, src.price) as price_change,
    src.link AS link,
    src.website AS website,
    src.offer_hash AS offer_hash,
    src.key AS key,
    src.bucket AS bucket,
    src.parsing_id AS parsing_id,
    src.row_id AS row_id,
    src.row_extracted_dttm_utc AS row_extracted_dttm_utc,
    src.created_dttm_utc AS created_dttm_utc
FROM `%(source)s` src
JOIN `%(source_first_time)s` as src_ft
ON src.hotel_id = src_ft.hotel_id
    AND src.start_date = src_ft.start_date
    AND src.end_date = src_ft.end_date
    AND src.room_type = src_ft.room_type
    AND src.mealplan = src_ft.room_type
    AND src.website = src_ft.website;