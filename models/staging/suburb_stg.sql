{{
    config(
        unique_key='LGA_NAME_SUBURB_NAME'
    )
}}

with

source  as (

    select * from "postgres"."raw"."lgasuburb"

),

suburb_stg as (
    select
        upper(LGA_NAME) || '_' || SUBURB_NAME as LGA_NAME_SUBURB_NAME,
        upper(LGA_NAME) as lga_name,
        SUBURB_NAME
    from source
),
cleaned as (
select * from suburb_stg
where LGA_NAME is not NULL
),
unknown as (
    select 
        'UNKNOWN_UNKNOWN' as LGA_NAME_SUBURB_NAME,
        'UNKNOWN' as LGA_NAME,
        'UNKNOWN' as SUBURB_NAME
)
select * from unknown
union all 
select * from cleaned

