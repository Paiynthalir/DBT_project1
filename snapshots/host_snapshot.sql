{% snapshot host_snapshot %}

{{
        config(
          target_schema='raw',
          strategy='timestamp',
          unique_key='host_id',
          updated_at='SCRAPED_DATE'
        )
    }}

select 
SCRAPED_DATE, HOST_ID,HOST_NAME,HOST_SINCE,HOST_IS_SUPERHOST,HOST_NEIGHBOURHOOD, inserted_datetime
from {{ source('raw', 'listings') }}

{% endsnapshot %}