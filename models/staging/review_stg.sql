{{
        config(
          strategy='timestamp',
          unique_key='listing_id',
          updated_at='inserted_datetime'
        )
    }}

with
source  as (

    select * from {{ ref('review_snapshot') }}
),
review_stg as (
    SELECT
        LISTING_ID,
        to_date(SCRAPED_DATE, 'YYYY-MM-DD') as SCRAPED_DATE,
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

select * from review_stg