$data = (
SELECT
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
FROM `parser/det/teztour`
WHERE row_extracted_dttm_utc >= CurrentUtcDatetime() - DateTime::IntervalFromDays(2)
UNION ALL
SELECT
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
FROM `parser/det/travelata`
WHERE row_extracted_dttm_utc >= CurrentUtcDatetime() - DateTime::IntervalFromDays(2)
);

REPLACE INTO `parser/det/pivot` 
SELECT *
FROM $data;