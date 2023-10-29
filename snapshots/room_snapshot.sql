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
ROOM_TYPE, SCRAPED_DATE 
from {{ source('raw', 'listings') }}

{% endsnapshot %}