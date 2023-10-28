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
        LGA_CODE,
        upper(LGA_NAME) as LGA_Name
    from source
)

select * from lga_stg
WHERE lga_stg.LGA_CODE IS NOT NULL-- Filter out rows with missing LGA_CODE
