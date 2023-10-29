{{
        config(
          unique_key='PROPERTY_TYPE',
        )
    }}

with
source  as (

    select * from {{ ref('property_snapshot') }}
),
property_stg as (
    SELECT
        SCRAPED_DATE,
        inserted_datetime,
        CASE WHEN PROPERTY_TYPE = 'NaN' THEN 'UNKNOWN' ELSE upper(PROPERTY_TYPE) END as  PROPERTY_TYPE,
        dbt_updated_at,
        dbt_valid_from,
        dbt_valid_to
    FROM source
),
cleaned as (
select * from property_stg),

unknown as (
    select 
        '1900-01-01'::date as SCRAPED_DATE,
        '1900-01-01'::timestamp as inserted_datetime,
        'UNKNOWN' as PROPERTY_TYPE,
        '1900-01-01'::date as dbt_updated_at,
        '1900-01-01'::date as dbt_valid_from,
        null::date as dbt_valid_to
)
select * from unknown
union all 
select * from cleaned