REPLACE INTO `%(target)s`
SELECT hotel_id,
    title,
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
FROM `%(source)s`
WHERE  row_extracted_dttm_utc >= %(dttm)s  - DateTime::IntervalFromHours(%(hours)s)
    and hotel_id is not null
    and start_date >= CurrentUtcDate();