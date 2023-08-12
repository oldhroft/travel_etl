REPLACE INTO `%(target)s`
SELECT hotel_id,
    start_date,
    end_date,
    room_type,
    mealplan,
    link,
    website,
    min_by(price, row_extracted_dttm_utc) as price,
    min_by(offer_hash, row_extracted_dttm_utc) as offer_hash,
    min_by(key, row_extracted_dttm_utc) as key,
    min_by(bucket, row_extracted_dttm_utc) as bucket,
    min_by(parsing_id, row_extracted_dttm_utc)as parsing_id,
    min_by(row_id, row_extracted_dttm_utc) as row_id,
    min(row_extracted_dttm_utc) as row_extracted_dttm_utc,
    min_by(created_dttm_utc, row_extracted_dttm_utc) as created_dttm_utc
FROM `%(source)s`
WHERE row_extracted_dttm_utc >= CurrentUtcDateTime() - DateTime::IntervalFromDays(%(days_init)s) 
    AND start_date <= CurrentUtcDate()
GROUP BY hotel_id,
    start_date,
    end_date,
    room_type,
    mealplan,
    link,
    website;
