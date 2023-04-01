$data = (
SELECT
    title,
    hotel_id,
    country_name,
    city_name,
    price,
    airport_distance,
    sand_beach_flg,
    start_date,
    end_date,
    rating,
    num_nights,
    is_flight_included,
    beach_line,
    num_stars,
    link,
    website,
    offer_hash,
    key,
    bucket,
    parsing_id,
    row_id,
    row_extracted_dttm_utc,
    created_dttm_utc
FROM`%(source_teztour)s`
WHERE row_extracted_dttm_utc >= CurrentUtcDatetime() - DateTime::IntervalFromHours(%(hours)s)
UNION ALL
SELECT
    title,
    hotel_id,
    country_name,
    location_name as city_name,
    price,
    airport_distance,
    sand_beach_flg,
    start_date,
    end_date,
    rating,
    num_nights,
    is_flight_included,
    beach_line,
    num_stars,
    link,
    website,
    offer_hash,
    key,
    bucket,
    parsing_id,
    row_id,
    row_extracted_dttm_utc,
    created_dttm_utc
FROM `%(source_travelata)s`
WHERE row_extracted_dttm_utc >= CurrentUtcDatetime() - DateTime::IntervalFromHours(%(hours)s)
);

REPLACE INTO `%(target)s`
SELECT *
FROM $data;