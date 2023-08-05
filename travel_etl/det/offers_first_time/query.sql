REPLACE INTO `%(target)s`
SELECT src.hotel_id AS hotel_id,
    src.price AS price,
    src.start_date AS start_date,
    src.end_date AS end_date,
    src.room_type AS room_type,
    src.mealplan AS mealplan,
    src.link AS link,
    src.website AS website,
    src.offer_hash AS offer_hash,
    src.key AS key,
    src.bucket AS bucket,
    src.parsing_id AS parsing_id,
    src.row_id AS row_id,
    src.row_extracted_dttm_utc AS row_extracted_dttm_utc,
    src.created_dttm_utc AS created_dttm_utc
FROM `%(source)s` as src
LEFT JOIN `%(source_first_time)s` as tgt
ON src.hotel_id = tgt.hotel_id
    AND src.start_date = tgt.start_date
    AND src.end_date = tgt.end_date
    AND src.room_type = tgt.room_type
    AND src.mealplan = tgt.room_type
    AND src.website = tgt.website
WHERE tgt.hotel_id IS NULL