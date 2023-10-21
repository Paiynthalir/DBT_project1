{% snapshot property_snapshot %}

{{
        config(
          target_schema='raw',
          strategy='timestamp',
          unique_key='listing_id',
          updated_at='inserted_datetime'
        )
    }}

select listing_id, SCRAPED_DATE, LISTING_NEIGHBOURHOOD, PROPERTY_TYPE, PRICE, inserted_datetime  from {{ source('raw', 'listings') }}

{% endsnapshot %}