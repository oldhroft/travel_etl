DELETE FROM `parser/prod/offers` 
WHERE
    row_extracted_dttm_utc >= CurrentUtcDatetime() - DateTime::IntervalFromDays(2)
    and start_date >= CurrentUtcDate();

$data = (
SELECT p.*,
    row_number() over (
        partition by website, hotel_id, start_date, end_date
        order by row_extracted_dttm_utc desc
    ) as rn
FROM `parser/det/pivot` p
WHERE  row_extracted_dttm_utc >= CurrentUtcDatetime() - DateTime::IntervalFromDays(4)
    and start_date >= CurrentUtcDate());

REPLACE INTO `parser/prod/offers`  
SELECT hotel_id,
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
FROM $data
WHERE rn = 1;