{% snapshot host_snapshot %}

{{
        config(
          target_schema='raw',
          strategy='timestamp',
          unique_key='combined_key', 
          updated_at='SCRAPED_DATE'
        )
}}

with source as (SELECT 
    LISTING_ID,
    CASE
      WHEN POSITION('-' IN SCRAPED_DATE) > 0 THEN to_date(SCRAPED_DATE, 'YYYY-MM-DD')
      WHEN POSITION('/' IN SCRAPED_DATE) > 0 THEN to_date(SCRAPED_DATE, 'DD/MM/YYYY')
      WHEN POSITION('/' IN SCRAPED_DATE) > 0 THEN to_date(SCRAPED_DATE, 'YYYY/MM/DD')
      WHEN POSITION('-' IN SCRAPED_DATE) > 0 THEN to_date(SCRAPED_DATE, 'DD-MM-YYYY')
      ELSE '1900-01-01'::date  
    END AS SCRAPED_DATE,
    HOST_ID,
    HOST_NAME,
    CASE
      WHEN POSITION('-' IN SCRAPED_DATE) > 0 THEN to_date(SCRAPED_DATE, 'YYYY-MM-DD')
      WHEN POSITION('/' IN SCRAPED_DATE) > 0 THEN to_date(SCRAPED_DATE, 'DD/MM/YYYY')
      WHEN POSITION('/' IN SCRAPED_DATE) > 0 THEN to_date(SCRAPED_DATE, 'YYYY/MM/DD')
      WHEN POSITION('-' IN SCRAPED_DATE) > 0 THEN to_date(SCRAPED_DATE, 'DD-MM-YYYY')
      ELSE '1900-01-01'::date 
    END AS HOST_SINCE,
    HOST_IS_SUPERHOST,
    HOST_NEIGHBOURHOOD,
    inserted_datetime 
FROM {{ source('raw', 'listings') }}
)

SELECT distinct
    SCRAPED_DATE,
    HOST_ID,
    HOST_NAME,
    HOST_SINCE,
    HOST_IS_SUPERHOST,
    HOST_NEIGHBOURHOOD,
    inserted_datetime,
    CONCAT(LISTING_ID,'_',CAST(HOST_ID AS VARCHAR), '_', CAST(SCRAPED_DATE AS VARCHAR)) AS combined_key 
FROM source


{% endsnapshot %}