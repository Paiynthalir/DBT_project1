{% snapshot property_snapshot %}

{{
        config(
          target_schema='raw',
          strategy='timestamp',
          unique_key='PROPERTY_TYPE',
          updated_at='SCRAPED_DATE'
        )
    }}

select 
listing_id, SCRAPED_DATE, LISTING_NEIGHBOURHOOD, PROPERTY_TYPE, inserted_datetime
from {{ source('raw', 'listings') }}

{% endsnapshot %}