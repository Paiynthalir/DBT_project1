{{
        config(
          unique_key='combined_key',
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
        -- date values had - and / and different order of year, month and date. So transforming the date values from string to correct date format
        CASE
            WHEN POSITION('-' IN SCRAPED_DATE) > 0 THEN to_date(SCRAPED_DATE, 'YYYY-MM-DD')
            WHEN POSITION('/' IN SCRAPED_DATE) > 0 THEN to_date(SCRAPED_DATE, 'DD/MM/YYYY')
            WHEN POSITION('/' IN SCRAPED_DATE) > 0 THEN to_date(SCRAPED_DATE, 'YYYY/MM/DD')
            WHEN POSITION('-' IN SCRAPED_DATE) > 0 THEN to_date(SCRAPED_DATE, 'DD-MM-YYYY')
        ELSE '1900-01-01'::date  
        END AS SCRAPED_DATE,
        CASE WHEN PROPERTY_TYPE = 'NaN' THEN 'UNKNOWN' ELSE upper(PROPERTY_TYPE) END as  PROPERTY_TYPE, -- replacing missing values
        CASE WHEN ROOM_TYPE = 'NaN' THEN 'UNKNOWN' ELSE upper(ROOM_TYPE) END as ROOM_TYPE, -- replacing missing values
        CASE WHEN LISTING_NEIGHBOURHOOD = 'NaN' THEN 'UNKNOWN' ELSE upper(LISTING_NEIGHBOURHOOD) END as  LISTING_NEIGHBOURHOOD, -- replacing missing values
        CASE WHEN HOST_ID IS NULL THEN 0 ELSE HOST_ID END AS HOST_ID, -- replacing missing/NULL values
        CASE WHEN ACCOMMODATES IS NULL THEN 0 ELSE ACCOMMODATES END AS ACCOMMODATES, -- replacing missing/NULL values
        PRICE,
        -- replacing missing/NULL values
        CASE WHEN HAS_AVAILABILITY = 'NaN' THEN 'f' ELSE HAS_AVAILABILITY::boolean END as HAS_AVAILABILITY, 
        CASE WHEN AVAILABILITY_30 IS NULL THEN 0 ELSE AVAILABILITY_30 END AS AVAILABILITY_30, 
        CASE WHEN NUMBER_OF_REVIEWS IS NULL THEN 0 ELSE NUMBER_OF_REVIEWS END AS NUMBER_OF_REVIEWS, 
        CASE WHEN REVIEW_SCORES_RATING = 'NaN' THEN 0 ELSE REVIEW_SCORES_RATING END AS REVIEW_SCORES_RATING, 
        CASE WHEN REVIEW_SCORES_ACCURACY = 'NaN' THEN 0 ELSE REVIEW_SCORES_ACCURACY END AS REVIEW_SCORES_ACCURACY,
        CASE WHEN REVIEW_SCORES_CLEANLINESS = 'NaN' THEN 0 ELSE REVIEW_SCORES_CLEANLINESS END AS REVIEW_SCORES_CLEANLINESS,
        CASE WHEN REVIEW_SCORES_CHECKIN = 'NaN' THEN 0 ELSE REVIEW_SCORES_CHECKIN  END AS REVIEW_SCORES_CHECKIN,
        CASE WHEN REVIEW_SCORES_COMMUNICATION = 'NaN' THEN 0 ELSE REVIEW_SCORES_COMMUNICATION END AS REVIEW_SCORES_COMMUNICATION,
        CASE WHEN REVIEW_SCORES_VALUE = 'NaN' THEN 0 ELSE CAST(REVIEW_SCORES_VALUE AS NUMERIC) END AS REVIEW_SCORES_VALUE,
        inserted_datetime
    FROM source
),
combined as (
    SELECT
        LISTING_ID,
        CONCAT(LISTING_ID,'_',CAST(HOST_ID AS VARCHAR), '_', CAST(SCRAPED_DATE AS VARCHAR)) AS combined_key,
        SCRAPE_ID,
        SCRAPED_DATE,
        PROPERTY_TYPE,
        ROOM_TYPE,
        LISTING_NEIGHBOURHOOD,
        HOST_ID,
        ACCOMMODATES,
        PRICE,
        HAS_AVAILABILITY,
        AVAILABILITY_30,
        NUMBER_OF_REVIEWS,
        REVIEW_SCORES_RATING,
        REVIEW_SCORES_ACCURACY,
        REVIEW_SCORES_CLEANLINESS,
        REVIEW_SCORES_CHECKIN,
        REVIEW_SCORES_COMMUNICATION,
        REVIEW_SCORES_VALUE,
        inserted_datetime
    FROM listings_stg
)

select * from combined
