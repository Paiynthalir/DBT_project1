{% snapshot room_snapshot %}

{{
        config(
          target_schema='raw',
          strategy='timestamp',
          unique_key='listing_id',
          updated_at='inserted_datetime'
        )
    }}

select listing_id, SCRAPED_DATE, ROOM_TYPE, ACCOMMODATES, HAS_AVAILABILITY, AVAILABILITY_30, inserted_datetime  from {{ source('raw', 'listings') }}

{% endsnapshot %}