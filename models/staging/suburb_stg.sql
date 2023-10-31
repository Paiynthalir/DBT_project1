{{
    config(
        unique_key='SUBURB_NAME'
    )
}}

with

source  as (

    select * from "postgres"."raw"."lgasuburb"

),

suburb_stg as (
    select
    -- changing values to upper case to reduce issue during joins in the future queries. 
        upper(LGA_NAME) as lga_name,
        upper(SUBURB_NAME) as SUBURB_NAME
    from source
),
cleaned as (
select * from suburb_stg
where LGA_NAME is not NULL
),
unknown as (
    select 
        'UNKNOWN' as LGA_NAME,
        'UNKNOWN' as SUBURB_NAME
)
select * from unknown
union all 
select * from cleaned

