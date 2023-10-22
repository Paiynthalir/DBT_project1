{{
        config(
          strategy='timestamp',
          unique_key='listing_id',
          updated_at='inserted_datetime'
        )
    }}

with
source  as (

    select * from {{ ref('room_snapshot') }}
),
room_stg as (
    SELECT
        LISTING_ID,
        to_date(SCRAPED_DATE, 'YYYY-MM-DD') as SCRAPED_DATE,
        ROOM_TYPE,
        ACCOMMODATES,
        CASE WHEN HAS_AVAILABILITY = 't' THEN 1 ELSE 0 END as HAS_AVAILABILITY,
        AVAILABILITY_30,
        inserted_datetime
    FROM source
)

select * from room_stg