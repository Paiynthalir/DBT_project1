{{
        config(
          strategy='timestamp',
          unique_key='listing_id',
          updated_at='inserted_datetime'
        )
    }}

with
source  as (

    select * from {{ ref('room_snapshot') }}
),
room_stg as (
    SELECT
        ROOM_TYPE,
        inserted_datetime
    FROM source
)

select * from room_stg