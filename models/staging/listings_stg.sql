{{
    config(
        unique_key='LISTING_ID'
    )
}}

with

source  as (

    select * from "postgres"."raw"."listings"

),

listings_stg as (
    SELECT
        LISTING_ID,
        SCRAPE_ID,
        to_date(SCRAPED_DATE, 'YYYY-MM-DD') as SCRAPED_DATE,
        HOST_ID,
        HOST_NAME,
        to_date(HOST_SINCE, 'DD/MM/YYYY') as HOST_SINCE,
        CASE WHEN HOST_IS_SUPERHOST = 't' THEN 1 ELSE 0 END as HOST_IS_SUPERHOST,
        CASE WHEN HOST_NEIGHBOURHOOD = 'NaN' THEN 'UNKNOWN' ELSE upper(HOST_NEIGHBOURHOOD) END as HOST_NEIGHBOURHOOD,
        upper(LISTING_NEIGHBOURHOOD) as LISTING_NEIGHBOURHOOD,
        PROPERTY_TYPE,
        ROOM_TYPE,
        ACCOMMODATES,
        PRICE,
        CASE WHEN HAS_AVAILABILITY = 't' THEN 1 ELSE 0 END as HAS_AVAILABILITY,
        AVAILABILITY_30,
        NUMBER_OF_REVIEWS,
        REVIEW_SCORES_RATING,
        REVIEW_SCORES_ACCURACY,
        REVIEW_SCORES_CLEANLINESS,
        REVIEW_SCORES_CHECKIN,
        REVIEW_SCORES_COMMUNICATION,
        REVIEW_SCORES_VALUE,
        inserted_datetime
    FROM source
)

select * from listings_stg