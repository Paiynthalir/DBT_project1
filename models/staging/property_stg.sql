{{
        config(
          strategy='timestamp',
          unique_key='listing_id',
          updated_at='inserted_datetime'
        )
    }}

with
source  as (

    select * from {{ ref('property_snapshot') }}
),
property_stg as (
    SELECT
        LISTING_ID,
        to_date(SCRAPED_DATE, 'YYYY-MM-DD') as SCRAPED_DATE,
        upper(LISTING_NEIGHBOURHOOD) as LISTING_NEIGHBOURHOOD,
        PROPERTY_TYPE,
        inserted_datetime 
    FROM source
)

select * from property_stg