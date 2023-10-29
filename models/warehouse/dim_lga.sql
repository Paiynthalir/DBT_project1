{{
    config(
        unique_key='lga_code'
    )
}}

select * from {{ ref('lga_stg') }}
