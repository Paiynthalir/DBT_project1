{{
    config(
        unique_key='LGA_CODE'
    )
}}

with
source as (
    select * from "postgres"."raw"."censusgtwo"
),
lgacode as (
    select LGA_CODE_2016 as LGA_CODE_SOURCE, CAST(SUBSTRING(LGA_CODE_2016 FROM 4) AS NUMERIC) AS LGA_CODE
    from source
),
combined as (
    select source.*, lgacode.LGA_CODE as LGA_CODE
    from source
    LEFT JOIN lgacode
    ON source.LGA_CODE_2016 = lgacode.LGA_CODE_SOURCE
)

select combined.* -- Exclude the columns used in the join
from combined
WHERE combined.LGA_CODE IS NOT NULL -- Filter out rows with missing LGA_CODE
