{{
    config(
        unique_key='ROOM_TYPE'
    )
}}


select * from {{ ref('room_stg') }}
