{{
    config(
        unique_key='LGA_CODE'
    )
}}

with
source as (
    select * from "postgres"."raw"."censusgtwo"
),
-- Removing "LGA_" from the LGA code values
lgacode as (
    select LGA_CODE_2016 as LGA_CODE_SOURCE, CAST(SUBSTRING(LGA_CODE_2016 FROM 4) AS NUMERIC) AS LGA_CODE
    from source
),
combined as (
    select source.*, lgacode.LGA_CODE as LGA_CODE
    from source
    LEFT JOIN lgacode
    ON source.LGA_CODE_2016 = lgacode.LGA_CODE_SOURCE
),
cleaned as (
select combined.* -- Exclude the columns used in the join
from combined
WHERE combined.LGA_CODE IS NOT NULL -- Filter out rows with missing LGA_CODE
),
unknown as (
    select
        'UNKNOWN' as LGA_CODE_2016,
        0 as Median_age_persons,
        0 as Median_mortgage_repay_monthly,
        0 as Median_tot_prsnl_inc_weekly,
        0 as Median_rent_weekly,
        0 as Median_tot_fam_inc_weekly,
        0 as Average_num_psns_per_bedroom,
        0 as Median_tot_hhd_inc_weekly,
        0 as Average_household_size,
        0 as LGA_CODE
)

select * from cleaned
union all
select * from unknown