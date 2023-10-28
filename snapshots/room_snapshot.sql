{% snapshot room_snapshot %}

{{
        config(
          target_schema='raw',
          strategy='timestamp',
          unique_key = 'room_type',
          updated_at='SCRAPED_DATE'
        )
    }}

select 
listing_id,ROOM_TYPE, SCRAPED_DATE, inserted_datetime  
from {{ source('raw', 'listings') }}

{% endsnapshot %}