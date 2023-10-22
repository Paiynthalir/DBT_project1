{{
    config(
        unique_key='lga_code'
    )
}}

with

source  as (

    select * from "postgres"."raw"."lgacode"

),

lga_stg as (
    select
        LGA_CODE as LGACode,
        upper(LGA_NAME) as LGA_Name
    from source
)

select * from lga_stg
