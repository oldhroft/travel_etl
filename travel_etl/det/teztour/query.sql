$match_stars = Re2::Match('^star_\\d+');
$capture_stars = Re2::Capture('^star_(\\d+)');
$capture_info = Re2::Capture('^на (\\d+) ноч\\p{Cyrillic}{1,2} до (\\d+)\\.(\\d+)');
$price_currency = Re2::Capture("^Эквивалент цены в валюте: (\\d+) \\$ \\| (\\d+) €");
$capture_city = Re2::Capture('^(.+)( \\- город)?');
$contains_line = Re2::Match('^Линия от моря\\: \\d+\\-я');
$contains_line_capture = Re2::Capture('^Линия от моря\\: (\\d+)\\-я');
$airport_match = Re2::Match('^До аэропорта: \\d+\\.?\\d* \\p{Cyrillic}{1,2}\\.');
$airport_capture = Re2::Capture('^До аэропорта: (\\d+\\.?\\d*) (\\p{Cyrillic}{1,2})\\.');
$hotel_id_capture = Re2::Capture('hotel/(\\d+)');

$data = (
SELECT
    cast(case when hotel_id is not null and hotel_id != 'None' then hotel_id
              else $hotel_id_capture(href)._1 end as int) as hotel_id,
    cast(
        case when hotel_rating = '0.0' then null
        else hotel_rating end as double) as hotel_rating,
    cast(currency as int) as currency_code,
    cast(case when latitude = 'None' then null
        else latitude end as double) as latitude,
    cast(case when longitude = 'None' then null
        else longitude end as double) as longitude,
    cast(price as int) as price,
    cast($capture_stars(ListHead(ListFilter(
        String::SplitToList(stars_class, ';'),
        $match_stars
        )))._1 as int) as num_stars,
        
    DateTime::GetYear(created_dttm) as year_created,
    ListLast(String::SplitToList(location_name, ',')) as country_name,
    ListSkip(ListReverse(String::SplitToList(location_name, ',')), 1) as location_list,
    $capture_info(till_info) as till_info_capture,
    $price_currency(price_box) as price_box_capture,
    String::SplitToList(amenities_list, ';') as amenities_list,
    String::Contains(price_include, 'перелет туда и обратно') as is_flight_included,
    room_type,
    mealplan,
    preview_img,
    title,
    link,
    website,
    offer_hash,
    key,
    bucket,
    parsing_id,
    row_id,
    created_dttm as row_extracted_dttm_utc,
    CurrentUtcDatetime() as created_dttm_utc
FROM `parser/raw/teztour`
WHERE created_dttm >= CurrentUtcDatetime() - DateTime::IntervalFromDays(1)
);

$data_proc = (
SELECT hotel_id,
    hotel_rating,
    currency_code,
    latitude,
    longitude,
    price,
    num_stars,
    country_name,
    is_flight_included,
    room_type,
    mealplan,
    preview_img,
    title,
    cast(price_box_capture._1 as double) as price_dollars,
    cast(price_box_capture._2 as double) as price_euros,
    $capture_city(ListHead(location_list))._1 as city_name,
    case when ListLength(location_list) == 2 then ListLast(location_list)
        else null end  as location_name,
    cast($contains_line_capture(
        ListHead(ListFilter(amenities_list,  $contains_line)))._1 as int) as beach_line,
    ListHas(amenities_list, "Бесплатный интернет") as is_free_internet,
    $airport_capture(
        ListHead(ListFilter(amenities_list,  $airport_match))) as airport_captured, 
    ListHas(amenities_list, "Пляж: песчаный") as sand_beach_flg,
    cast(till_info_capture._1 as int) as num_nights,
    cast(ListConcat(
        AsList(cast(year_created as string), 
            cast(till_info_capture._3 as string),
            cast(till_info_capture._2 as string)), '-') as date) as start_date,
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
);

REPLACE INTO `parser/det/teztour`
SELECT hotel_id,
    hotel_rating,
    currency_code,
    latitude,
    longitude,
    price,
    num_stars,
    cast(country_name as utf8) as country_name,
    is_flight_included,
    cast(room_type as utf8) as room_type,
    cast(mealplan as utf8) as mealplan,
    cast(preview_img as utf8) as preview_img,
    cast(title as utf8) as title,
    price_dollars,
    price_euros,
    cast(location_name as utf8) as location_name,
    beach_line,
    is_free_internet,
    case when airport_captured._2 = "км" 
        then 1000 * cast(airport_captured._1 as double)
        when airport_captured._2 = "м" 
        then cast(airport_captured._1 as double)
        else null
        end as airport_distance,
    num_nights,
    start_date,
    start_date + DateTime::IntervalFromDays(num_nights) as end_date,
    sand_beach_flg,
    link,
    website,
    offer_hash,
    key,
    bucket,
    parsing_id,
    row_id,
    row_extracted_dttm_utc,
    created_dttm_utc
FROM $data_proc;