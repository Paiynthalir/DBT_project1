{{
    config(
        unique_key='LGA_CODE'
    )
}}


select * from {{ ref('censusg01_stg') }}
