{{
    config(
        unique_key='PROPERTY_TYPE'
    )
}}


select * from {{ ref('property_stg') }}
