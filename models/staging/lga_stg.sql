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
        CASE WHEN LGA_NAME = 'NaN' THEN 'UNKNOWN' ELSE upper(LGA_NAME) END as LGA_Name -- replacing missing values
    from source
),
cleaned as (
select * from lga_stg
WHERE lga_stg.LGA_CODE IS NOT NULL-- Filter out rows with missing LGA_CODE
),
unknown as (
    select 
        0 as LGA_CODE,
        'UNKNOWN' as LGA_NAME
)
select * from unknown
union all 
select * from cleaned