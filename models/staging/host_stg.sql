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
        CASE
            WHEN POSITION('-' IN SCRAPED_DATE) > 0 THEN to_date(SCRAPED_DATE, 'YYYY-MM-DD')
            WHEN POSITION('/' IN SCRAPED_DATE) > 0 THEN to_date(SCRAPED_DATE, 'DD/MM/YYYY')
            WHEN POSITION('/' IN SCRAPED_DATE) > 0 THEN to_date(SCRAPED_DATE, 'YYYY/MM/DD')
            WHEN POSITION('-' IN SCRAPED_DATE) > 0 THEN to_date(SCRAPED_DATE, 'DD-MM-YYYY')
            ELSE NULL 
        END AS SCRAPED_DATE,
        inserted_datetime,
        HOST_ID,
        CASE WHEN HOST_NAME = 'NaN' THEN 'UNKNOWN' ELSE upper(HOST_NAME) END as HOST_NAME,
        CASE
            WHEN POSITION('-' IN SCRAPED_DATE) > 0 THEN to_date(SCRAPED_DATE, 'YYYY-MM-DD')
            WHEN POSITION('/' IN SCRAPED_DATE) > 0 THEN to_date(SCRAPED_DATE, 'DD/MM/YYYY')
            WHEN POSITION('/' IN SCRAPED_DATE) > 0 THEN to_date(SCRAPED_DATE, 'YYYY/MM/DD')
            WHEN POSITION('-' IN SCRAPED_DATE) > 0 THEN to_date(SCRAPED_DATE, 'DD-MM-YYYY')
            ELSE NULL 
        END AS HOST_SINCE,
        CASE WHEN HOST_NEIGHBOURHOOD = 'NaN' THEN 'UNKNOWN' ELSE upper(HOST_NEIGHBOURHOOD) END as HOST_NEIGHBOURHOOD,
        CASE WHEN HOST_IS_SUPERHOST = 'NaN' THEN 'f' ELSE HOST_IS_SUPERHOST::boolean END as HOST_IS_SUPERHOST,
        CASE
            WHEN POSITION('-' IN SCRAPED_DATE) > 0 THEN to_date(SCRAPED_DATE, 'YYYY-MM-DD')
            WHEN POSITION('/' IN SCRAPED_DATE) > 0 THEN to_date(SCRAPED_DATE, 'DD/MM/YYYY')
            WHEN POSITION('/' IN SCRAPED_DATE) > 0 THEN to_date(SCRAPED_DATE, 'YYYY/MM/DD')
            WHEN POSITION('-' IN SCRAPED_DATE) > 0 THEN to_date(SCRAPED_DATE, 'DD-MM-YYYY')
            ELSE NULL 
        END AS dbt_updated_at,
        CASE
            WHEN POSITION('-' IN SCRAPED_DATE) > 0 THEN to_date(SCRAPED_DATE, 'YYYY-MM-DD')
            WHEN POSITION('/' IN SCRAPED_DATE) > 0 THEN to_date(SCRAPED_DATE, 'DD/MM/YYYY')
            WHEN POSITION('/' IN SCRAPED_DATE) > 0 THEN to_date(SCRAPED_DATE, 'YYYY/MM/DD')
            WHEN POSITION('-' IN SCRAPED_DATE) > 0 THEN to_date(SCRAPED_DATE, 'DD-MM-YYYY')
            ELSE NULL 
        END AS dbt_valid_from,
        CASE
            WHEN POSITION('-' IN SCRAPED_DATE) > 0 THEN to_date(SCRAPED_DATE, 'YYYY-MM-DD')
            WHEN POSITION('/' IN SCRAPED_DATE) > 0 THEN to_date(SCRAPED_DATE, 'DD/MM/YYYY')
            WHEN POSITION('/' IN SCRAPED_DATE) > 0 THEN to_date(SCRAPED_DATE, 'YYYY/MM/DD')
            WHEN POSITION('-' IN SCRAPED_DATE) > 0 THEN to_date(SCRAPED_DATE, 'DD-MM-YYYY')
            ELSE NULL 
        END AS dbt_valid_to
    FROM source
)
    select * from host_stg 
    WHERE host_stg.HOST_ID IS NOT NULL-- Filter out rows with missing LGA_CODE


