DELETE FROM `%(target)s`
WHERE row_extracted_dttm_utc >= CurrentUtcDatetime() - DateTime::IntervalFromDays(%(days_offer)s);