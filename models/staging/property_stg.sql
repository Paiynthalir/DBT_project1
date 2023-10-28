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
        LISTING_ID,
        CASE
            WHEN POSITION('-' IN SCRAPED_DATE) > 0 THEN to_date(SCRAPED_DATE, 'YYYY-MM-DD')
            WHEN POSITION('/' IN SCRAPED_DATE) > 0 THEN to_date(SCRAPED_DATE, 'DD/MM/YYYY')
            ELSE NULL 
        END AS SCRAPED_DATE,
        CASE WHEN LISTING_NEIGHBOURHOOD = 'NaN' THEN 'UNKNOWN' ELSE upper(LISTING_NEIGHBOURHOOD) END as  LISTING_NEIGHBOURHOOD,
        CASE WHEN PROPERTY_TYPE = 'NaN' THEN 'UNKNOWN' ELSE upper(PROPERTY_TYPE) END as  PROPERTY_TYPE,
        inserted_datetime,
      CASE
            WHEN POSITION('-' IN dbt_updated_at) > 0 THEN to_date(dbt_updated_at, 'YYYY-MM-DD')
            WHEN POSITION('/' IN dbt_updated_at) > 0 THEN to_date(dbt_updated_at, 'DD/MM/YYYY')
            ELSE NULL 
        END AS dbt_updated_at,
        CASE
            WHEN POSITION('-' IN dbt_valid_from) > 0 THEN to_date(dbt_valid_from, 'YYYY-MM-DD')
            WHEN POSITION('/' IN dbt_valid_from) > 0 THEN to_date(dbt_valid_from, 'DD/MM/YYYY')
            ELSE NULL 
        END AS dbt_valid_from,
        CASE
            WHEN POSITION('-' IN dbt_valid_to) > 0 THEN to_date(dbt_valid_to, 'YYYY-MM-DD')
            WHEN POSITION('/' IN dbt_valid_to) > 0 THEN to_date(dbt_valid_to, 'DD/MM/YYYY')
            ELSE NULL 
        END AS dbt_valid_to
    FROM source
),
cleaned as (
select * from property_stg
Where PROPERTY_TYPE is not NULL),

unknown as (
    select 
        0 as LISTING_ID,
        '1900-01-01'::date as SCRAPED_DATE,
        'UNKNOWN' as LISTING_NEIGHBOURHOOD,
        'UNKNOWN' as PROPERTY_TYPE,
        '1900-01-01'::timestamp as inserted_datetime,
        '1900-01-01'::date as dbt_updated_at,
        '1900-01-01'::date as dbt_valid_from,
        null::date as dbt_valid_to
)
select * from unknown
union all 
select * from cleaned