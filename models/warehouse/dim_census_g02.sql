{{
    config(
        unique_key='LGA_CODE'
    )
}}


select * from {{ ref('censusg02_stg') }}
