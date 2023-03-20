$capture = Re2::Capture('^(\\d+)');

$capture_info = Re2::Capture('^(\\d+) взро[cс]лых [cс] (\\d+) (\\p{Cyrillic}+) на (\\d+) ноч\\p{Cyrillic}{1,2}\\, перелет (.+)');

$capture_orders = Re2::Capture('Забронировано (\\d+) раз');

$capture_oil_tax = Re2::Capture('вкл\\. топл\\. сбор: ([\\s\\d]+) руб');

$airport_match = Re2::Match('^\\d+\\.?\\d* \\p{Cyrillic}{1,2} до аэропорта');

$airport_match_capture = Re2::Capture('^(\\d+\\.?\\d*) (\\p{Cyrillic}{1,2}) до аэропорта');

$beach_match = Re2::Match('^\\d+\\.?\\d* \\p{Cyrillic}{1,2} до пляжа');

$beach_match_capture = Re2::Capture('^(\\d+\\.?\\d*) (\\p{Cyrillic}{1,2}) до пляжа');

$hotel_id_capture = Re2::Capture('/hotel/(\\d+)/');

$contains_line = ($y) -> {
    RETURN String::Contains($y, 'линия') and not String::Contains($y, '?')
};

$contains_wifi = ($y) -> {
    RETURN String::Contains($y, 'wi-fi') and String::Contains($y, '?')
};

$contains_wifi_unkn = ($y) -> {
    RETURN String::Contains($y, 'wi-fi') and not String::Contains($y, '?')
};

$contains_sand = ($y) -> {
    RETURN String::Contains($y, 'песок')
};

$contains_pebble = ($y) -> {
    RETURN String::Contains($y, 'галька')
};

$contains_pebble_or_sand = ($y) -> {
    RETURN (String::Contains($y, 'галька') or String::Contains($y, 'песок'))
        and String::Contains($y, '?')
};

$contains_condi = ($y) -> {
    RETURN String::Contains($y, 'конди') and String::Contains($y, '?')
};

$contains_cond_unkn = ($y) -> {
    RETURN String::Contains($y, 'конди') and not String::Contains($y, '?')
};

$data = (
SELECT title,
    link,
    website,
    offer_hash,
    key,
    bucket,
    parsing_id,
    row_id,
    created_dttm as row_extracted_dttm_utc,
    cast(num_stars as int) as num_stars,
    less_places != 'None' as flag_less_places,
    cast(ListConcat(
        ListReverse(
        ListSkip(
        ListReverse(
            String::SplitToList(location, ",")), 1)), ',') as utf8) as location_name,
    
    cast(ListLast(
        String::SplitToList(location, ",")) as utf8) as country_name,
    if(rating = 'None', NULL, cast(rating as double)) as rating,
    cast(
        if(reviews = 'None', NULL, $capture(reviews))._1
        as int) as num_reviews,
    $capture_info(criteria) as criteria_parsed,
    cast(
        ($capture_orders(orders_count))._1 as int) as orders_count,
    String::SplitToList(attributes, ';') as attributes_list,
    String::SplitToList(distances, ';') as distances_list,
    cast(
        String::ReplaceAll(
        ($capture_oil_tax(oil_tax))._1, ' ', '') as double) as oil_tax_value,
    cast($hotel_id_capture(href)._1 as int) as hotel_id,
    CurrentUtcDatetime() as created_dttm_utc
FROM `parser/raw/travelata`);

$data_prep = (
SELECT
    num_stars,
    flag_less_places,
    location_name,
    country_name,
    rating,
    num_reviews,
    orders_count,
    oil_tax_value,
    hotel_id,
    cast(criteria_parsed._1 as int) as num_people,
    cast(criteria_parsed._2 as int) as from_date_num,
    
    case when criteria_parsed._3 = 'января' then 1
        when criteria_parsed._3 = 'февраля' then 2
        when criteria_parsed._3 = 'марта' then 3
        when criteria_parsed._3 = 'апреля' then 4
        when criteria_parsed._3 = 'мая' then 5
        when criteria_parsed._3 = 'июня' then 6
        when criteria_parsed._3 = 'июлю' then 7
        when criteria_parsed._3 = 'августа' then 8
        when criteria_parsed._3 = 'сентября' then 9
        when criteria_parsed._3 = 'октября' then 10
        when criteria_parsed._3 = 'ноября' then 11
        when criteria_parsed._3 = 'декабря' then 12 
        else null
        end as mnth,
    distances_list,
    $airport_match_capture(
        ListHead(ListFilter(distances_list, $airport_match))) as match_airport_distance,
    $beach_match_capture(
        ListHead(ListFilter(distances_list, $beach_match))) as match_beach_distance,
    DateTime::GetYear(created_dttm_utc) as year_created,
    cast(criteria_parsed._4 as int) as num_nights,
    criteria_parsed._5 = 'включен' as is_flight_included,
    cast($capture(ListHead(ListFilter(attributes_list, $contains_line)))._1 as int) as beach_line,
    ListLast(String::SplitToList(
        ListHead(ListFilter(attributes_list, $contains_wifi)),
        '?')) as wifi_option,
    ListAny(ListMap(attributes_list, $contains_wifi_unkn)) as wifi_unknown,
    ListAny(ListMap(attributes_list, $contains_sand)) as sand_beach_flg,
    ListAny(ListMap(attributes_list, $contains_pebble)) as pebble_beach_flg,
    ListLast(String::SplitToList(
        ListHead(ListFilter(attributes_list, $contains_pebble_or_sand)),
        '?')) as beach_options,
    ListLast(String::SplitToList(
        ListHead(ListFilter(attributes_list, $contains_condi)),
        '?')) as condi_option,
    ListAny(ListMap(attributes_list, $contains_cond_unkn)) as condi_unknown,
    link,
    website,
    offer_hash,
    key,
    bucket,
    parsing_id,
    row_id,
    row_extracted_dttm_utc,
    created_dttm_utc
FROM $data);

REPLACE INTO `parser/det/travelata`
SELECT
    num_stars,
    flag_less_places,
    location_name,
    country_name,
    rating,
    num_reviews,
    orders_count,
    oil_tax_value,
    hotel_id,
    num_people,
    cast(ListConcat(
        AsList(cast(year_created as string), 
            cast(mnth as string),
            cast(from_date_num as string)), '-') as date) as start_date,
    cast(ListConcat(
        AsList(cast(year_created as string), 
            cast(mnth as string),
            cast(from_date_num as string)), '-') as date)
    + DateTime::IntervalFromDays(num_nights) as end_date,
    
    case when match_airport_distance._2 = "км" 
        then 1000 * cast(match_airport_distance._1 as double)
        when match_airport_distance._2 = "м" 
        then cast(match_airport_distance._1 as double)
        else null
        end as airport_distance,
        
    case when match_beach_distance._2 = "км" 
        then 1000 * cast(match_beach_distance._1 as double)
        when match_beach_distance._2 = "м" 
        then cast(match_beach_distance._1 as double)
        else null
        end as beach_distance,
    
    cast(
        case when wifi_unknown then cast("Бесплатно?" as Utf8)
            else cast(wifi_option as Utf8) end as Utf8) as wifi_option,
    cast(
        case when condi_unknown then cast("Бесплатно?"  as Utf8)
            else cast(condi_option as Utf8) end  as Utf8) as condi_option,
    
    num_nights,
    is_flight_included,
    beach_line,
    sand_beach_flg,
    pebble_beach_flg,
    cast(beach_options as Utf8) as beach_options,
    link,
    website,
    offer_hash,
    key,
    bucket,
    parsing_id,
    row_id,
    row_extracted_dttm_utc,
    created_dttm_utc
FROM $data_prep
WHERE row_extracted_dttm_utc >= CurrentUtcDatetime() - DateTime::IntervalFromDays(1);