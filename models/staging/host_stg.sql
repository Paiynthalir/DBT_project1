{{
        config(
          strategy='timestamp',
          unique_key='host_id',
          updated_at='inserted_datetime'
        )
    }}

with
source  as (

    select * from {{ ref('host_snapshot') }}

),
host_stg as (
    SELECT
        to_date(SCRAPED_DATE, 'YYYY-MM-DD') as SCRAPED_DATE,
        HOST_ID,
        HOST_NAME,
        to_date(HOST_SINCE, 'DD/MM/YYYY') as HOST_SINCE,
        CASE WHEN HOST_NEIGHBOURHOOD = 'NaN' THEN 'UNKNOWN' ELSE upper(HOST_NEIGHBOURHOOD) END as HOST_NEIGHBOURHOOD,
        CASE WHEN HOST_IS_SUPERHOST = 'NaN' THEN 'f' ELSE HOST_IS_SUPERHOST END as HOST_IS_SUPERHOST,
        inserted_datetime
    FROM source
)

select * from host_stg 
WHERE host_stg.HOST_ID IS NOT NULL-- Filter out rows with missing LGA_CODE