{{
        config(
          unique_key='host_id',
        )
    }}

with
source  as (

    select * from {{ ref('host_snapshot') }}

),
host_stg as (
    SELECT
        combined_key,
        SCRAPED_DATE,
        inserted_datetime,
        HOST_ID,
        CASE WHEN HOST_NAME = 'NaN' THEN 'UNKNOWN' ELSE upper(HOST_NAME) END as HOST_NAME, -- replacing missing values
        HOST_SINCE,
        CASE WHEN HOST_NEIGHBOURHOOD = 'NaN' THEN 'UNKNOWN' ELSE upper(HOST_NEIGHBOURHOOD) END as HOST_NEIGHBOURHOOD, -- replacing missing values
        CASE WHEN HOST_IS_SUPERHOST = 'NaN' THEN 'f' ELSE HOST_IS_SUPERHOST::boolean END as HOST_IS_SUPERHOST, -- replacing missing values
        dbt_updated_at,
        dbt_valid_from,
        dbt_valid_to
    FROM source
),

cleaned as (select * from host_stg),

unknown as (
    select 
        'UNKNOWN' as combined_key,
        '1900-01-01'::date as SCRAPED_DATE,
        '1900-01-01'::timestamp as inserted_datetime,
        0 as HOST_ID,
        'UNKNOWN' as HOST_NAME,
        '1900-01-01'::date as HOST_SINCE,
        'UNKNOWN' as HOST_NEIGHBOURHOOD,
        'f'::boolean as HOST_IS_SUPERHOST,
        '1900-01-01'::date as dbt_updated_at,
        '1900-01-01'::date as dbt_valid_from,
        null::date as dbt_valid_to
)

select * from unknown
union all 
select * from cleaned

