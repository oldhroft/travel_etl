REPLACE INTO `%(target)s`
SELECT 
    website, 
    hotel_id, 
    start_date, 
    end_date, 
    room_type,
    mealplan,
    min_by(price, row_extracted_dttm_utc) AS price,
    min_by(link, row_extracted_dttm_utc) AS link,
    min_by(offer_hash, row_extracted_dttm_utc) AS offer_hash,
    min_by(key, row_extracted_dttm_utc) AS key,
    min_by(bucket, row_extracted_dttm_utc) AS bucket,
    min_by(parsing_id, row_extracted_dttm_utc) AS parsing_id,
    min_by(row_id, row_extracted_dttm_utc) AS row_id,
    min(row_extracted_dttm_utc) AS row_extracted_dttm_utc,
    min_by(created_dttm_utc, row_extracted_dttm_utc) AS created_dttm_utc
FROM `%(source)s`
WHERE  row_extracted_dttm_utc >= %(dttm)s  - DateTime::IntervalFromDays(%(days_tracking)s)
    and hotel_id is not null
    and start_date >= CurrentUtcDate()
GROUP BY website, 
    hotel_id, 
    start_date, 
    end_date, 
    room_type,
    mealplan;