{% snapshot room_snapshot %}

{{
        config(
          target_schema='raw',
          strategy='timestamp',
          unique_key = 'room_type',
          updated_at='SCRAPED_DATE'
        )
    }}

select distinct
    ROOM_TYPE, 
    CASE
      WHEN POSITION('-' IN SCRAPED_DATE) > 0 THEN to_date(SCRAPED_DATE, 'YYYY-MM-DD')
      WHEN POSITION('/' IN SCRAPED_DATE) > 0 THEN to_date(SCRAPED_DATE, 'DD/MM/YYYY')
      WHEN POSITION('/' IN SCRAPED_DATE) > 0 THEN to_date(SCRAPED_DATE, 'YYYY/MM/DD')
      WHEN POSITION('-' IN SCRAPED_DATE) > 0 THEN to_date(SCRAPED_DATE, 'DD-MM-YYYY')
      ELSE '1900-01-01'::date  
    END AS SCRAPED_DATE,
    inserted_datetime
from {{ source('raw', 'listings') }}

{% endsnapshot %}