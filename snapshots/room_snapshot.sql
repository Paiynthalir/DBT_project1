{% snapshot room_snapshot %}

{{
        config(
          target_schema='raw',
          strategy='timestamp',
          unique_key='listing_id',
          updated_at='inserted_datetime'
        )
    }}

select 
ROOM_TYPE, inserted_datetime  
from {{ source('raw', 'listings') }}

{% endsnapshot %}