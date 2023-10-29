{% snapshot property_snapshot %}

{{
        config(
          target_schema='raw',
          strategy='timestamp',
          unique_key='PROPERTY_TYPE',
          updated_at='SCRAPED_DATE'
        )
    }}

select distinct
SCRAPED_DATE, PROPERTY_TYPE
from {{ source('raw', 'listings') }}

{% endsnapshot %}