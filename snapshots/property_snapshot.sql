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
SCRAPED_DATE, PROPERTY_TYPE, inserted_datetime
from {{ source('raw', 'listings') }}

{% endsnapshot %}