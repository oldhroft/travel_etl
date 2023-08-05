$format = DateTime::Format("%%Y-%%m-%%d");

select 
    cast($format(min(start_date)) as utf8) as min_start_date,
    cast($format(max(start_date)) as utf8) as max_start_date,
    cast($format(min(end_date)) as utf8) as min_end_date,
    cast($format(max(end_date)) as utf8) as max_end_date
from `parser/prod/offers`;