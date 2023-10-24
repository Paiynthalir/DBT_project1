{{
        config(
          strategy='timestamp',
          unique_key='listing_id',
          updated_at='inserted_datetime'
        )
    }}

with
source  as (

    select * from {{ ref('host_snapshot') }}

),
host_stg as (
    SELECT
        LISTING_ID,
        to_date(SCRAPED_DATE, 'YYYY-MM-DD') as SCRAPED_DATE,
        HOST_ID,
        HOST_NAME,
        to_date(HOST_SINCE, 'DD/MM/YYYY') as HOST_SINCE,
        HOST_IS_SUPERHOST,
        inserted_datetime
    FROM source
)

select * from host_stg