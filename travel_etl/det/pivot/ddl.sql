CREATE TABLE `parser/det/pivot` (
    title utf8,
    hotel_id int,
    country_name utf8,
    city_name utf8,
    price double,
    airport_distance double,
    sand_beach_flg bool,
    start_date date,
    end_date date,
    rating double,
    num_nights double,
    is_flight_included bool,
    beach_line int,
    num_stars double,
    link utf8,
    website utf8,
    offer_hash utf8,
    key utf8,
    bucket utf8,
    parsing_id utf8,
    row_id utf8,
    row_extracted_dttm_utc datetime,
    created_dttm_utc datetime,
    PRIMARY KEY (parsing_id, row_extracted_dttm_utc, row_id) 
);
