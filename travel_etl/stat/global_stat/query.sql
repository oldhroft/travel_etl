REPLACE INTO `%(target)s`
SELECT
    global_id,
    website,
    count(*) as cnt_total,
    sum(cast(failed as int64)) as cnt_failed,
    min(parsing_started) as utc_started_dttm,
    max(parsing_ended) as utc_ended_dttm
FROM `%(source)s`
WHERE global_id != ''
    and parsing_started >= CurrentUtcDate() - DateTime::IntervalFromDays(%(days)s)
GROUP BY global_id,
    website;