{{
    config(
        unique_key='suburb_name'
    )
}}

select * from {{ ref('suburb_stg') }}