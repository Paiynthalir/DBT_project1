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
)

select * from suburb_stg
where LGA_NAME is not NULL



