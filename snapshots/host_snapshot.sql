{% snapshot host_snapshot %}

{{
        config(
          target_schema='raw',
          strategy='timestamp',
          unique_key='host_id',
          updated_at='inserted_datetime'
        )
    }}

select 
HOST_ID,HOST_NAME,HOST_SINCE,HOST_IS_SUPERHOST,HOST_NEIGHBOURHOOD, inserted_datetime 
from {{ source('raw', 'listings') }}

{% endsnapshot %}