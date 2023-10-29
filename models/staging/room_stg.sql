{{
        config(
          unique_key='room_type',
        )
    }}

with
source  as (

    select * from {{ ref('room_snapshot') }}
),
room_stg as (
    SELECT
        SCRAPED_DATE,
        inserted_datetime,
        CASE WHEN ROOM_TYPE = 'NaN' THEN 'UNKNOWN' ELSE upper(ROOM_TYPE) END as ROOM_TYPE,
        dbt_updated_at,
        dbt_valid_from,
        dbt_valid_to
    FROM source
),
cleaned as (
select * from room_stg
Where room_type is not NULL),

unknown as (
    select 
        '1900-01-01'::date as SCRAPED_DATE,
        '1900-01-01'::date as inserted_datetime,
        'UNKNOWN' as ROOM_TYPE,
        '1900-01-01'::date as dbt_updated_at,
        '1900-01-01'::date  as dbt_valid_from,
        null::date as dbt_valid_to
)
select * from unknown
union all 
select * from cleaned