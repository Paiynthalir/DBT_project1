{{
    config(
        unique_key='combined_key'
    )
}}

with
source as (
    select * from {{ ref('listings_stg') }}
),
-- CTE to check the dimensions
check_dimensions as
(select
	combined_key,
    LISTING_ID,
    SCRAPED_DATE,
    case when HOST_ID in (select distinct HOST_ID from {{ ref('host_stg') }}) then HOST_ID else 0 end as HOST_ID,
    LISTING_NEIGHBOURHOOD,
    ACCOMMODATES,
    PROPERTY_TYPE,
    ROOM_TYPE,
    PRICE,
    HAS_AVAILABILITY,
    AVAILABILITY_30,
    NUMBER_OF_REVIEWS,
    REVIEW_SCORES_RATING,
    inserted_datetime
from source),
-- CTE to have LGA_Code, name and suburb names in the same set of results
lga_suburbs as (
select
    a.SUBURB_NAME,
    a.LGA_NAME,
    b.LGA_CODE
    from {{ ref('suburb_stg') }} a
    left join {{ ref('lga_stg') }} b on a.lga_name = b.lga_name
),
-- CTE to join the host details in the facts_listing table and also bring in the LGA_Code for listing_neighbourhood
addhost_details as(
select
	a.combined_key,
    a.LISTING_ID,
    a.SCRAPED_DATE,
    a.LISTING_NEIGHBOURHOOD,
    b.LGA_CODE as LISTING_NEIGHBOURHOOD_LGA_CODE,
    a.HOST_ID,
    c.HOST_NAME,
    c.HOST_SINCE,
    c.HOST_IS_SUPERHOST,
    c.HOST_NEIGHBOURHOOD,
    a.PROPERTY_TYPE,
    a.ROOM_TYPE,
    a.PRICE,
    a.ACCOMMODATES,
    a.HAS_AVAILABILITY,
    a.AVAILABILITY_30,
    a.NUMBER_OF_REVIEWS,
    a.REVIEW_SCORES_RATING,
    a.inserted_datetime
from check_dimensions a
left join {{ ref('host_stg') }} c on a.combined_key = c.combined_key and a.SCRAPED_DATE::date >= c.dbt_valid_from and a.SCRAPED_DATE::date < coalesce(c.dbt_valid_to, '9999-12-31'::timestamp)
left join {{ ref('lga_stg') }} b  on a.LISTING_NEIGHBOURHOOD = b.LGA_NAME
)
-- finally to bring in the host_neighbourhood lga names and lga codes by joining with sub urb names. 
select
	a.combined_key,
    a.LISTING_ID,
    a.SCRAPED_DATE,
    a.LISTING_NEIGHBOURHOOD,
    a.LISTING_NEIGHBOURHOOD_LGA_CODE,
    a.HOST_ID,
    a.HOST_NAME,
    a.HOST_SINCE,
    a.HOST_IS_SUPERHOST,
    a.HOST_NEIGHBOURHOOD,
    b.LGA_CODE as HOST_NEIGHBOURHOOD_LGA_CODE,
    b.LGA_NAME as HOST_NEIGHBOURHOOD_LGA_NAME,
    a.PROPERTY_TYPE,
    a.ROOM_TYPE,
    a.PRICE,
    a.ACCOMMODATES,
    a.HAS_AVAILABILITY,
    a.AVAILABILITY_30,
    a.NUMBER_OF_REVIEWS,
    a.REVIEW_SCORES_RATING,
    a.inserted_datetime
from addhost_details a
left join lga_suburbs b  on a.HOST_NEIGHBOURHOOD = b.suburb_name





