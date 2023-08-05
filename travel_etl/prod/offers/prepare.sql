DELETE FROM `%(target)s`
WHERE 
    row_extracted_dttm_utc <= %(dttm)s - DateTime::IntervalFromDays(%(days_offer)s)
    and start_date >= CurrentUtcDate();;