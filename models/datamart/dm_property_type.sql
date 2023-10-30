
{{
    config(
        unique_key=['property_type', 'room_type','accommodates', 'year_month']
    )
}}

-- dm_property_type
with property_stat as (
SELECT
  property_type,
  room_type,
  accommodates,
  to_char(scraped_date, 'YYYY-MM') AS year_month,
  --Active listings rate calculation - Active Listing Rate = (total Active listings / total listing) * 100
  ROUND(
    (COUNT(CASE WHEN has_availability = 't' THEN 1 ELSE NULL END) * 100.0) / COUNT(*),
    2
  ) AS active_listings_rate,
  -- Minimum, maximum, median and average price for active listings calculation
  MIN(CASE WHEN has_availability = 't' THEN price ELSE 0 END) AS min_price,
  MAX(CASE WHEN has_availability = 't' THEN price ELSE 0 END) AS max_price,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CASE WHEN has_availability = 't' THEN price ELSE 0 END) AS median_price,
  ROUND(
    AVG(CASE WHEN has_availability = 't' THEN price ELSE 0 END),
    2
  ) AS avg_price,
  -- Number of distinct hosts calculation
  COUNT(DISTINCT host_id) AS distinct_hosts,
  -- Calculating Superhost Rate =  (total distinct hosts with "host_is_superhost" = 't' / total distinct hosts) * 100
  ROUND(
    (COUNT(DISTINCT CASE WHEN host_is_superhost = 't' THEN host_id ELSE NULL END) * 100.0) / COUNT(DISTINCT host_id),
    2
  ) AS superhost_rate,
  -- Calculating Number of stays (only for active listings) = 30 - availability_30 
  SUM(CASE WHEN has_availability = 't' THEN (30 - availability_30) ELSE 0 END) AS total_stays,
  -- Average of review_scores_rating for active listings calculations
  ROUND(
    AVG(CASE WHEN has_availability = 't' THEN review_scores_rating ELSE 0 END),
    2
  ) AS avg_review_scores,
  -- Average Estimated revenue per active listings - Estimated revenue per active listings = for each active listing/period: number of stays * price
  SUM(CASE WHEN has_availability = 't' THEN (30 - availability_30) * price ELSE 0 END) AS avg_estimated_revenue,
  -- calculating the count of active and inactive listings
  SUM(CASE WHEN has_availability = 't' THEN 1 ELSE 0 END) AS active_count,
  SUM(CASE WHEN has_availability = 'f' THEN 1 ELSE 0 END) AS inactive_count
FROM {{ ref('facts_listings') }}
GROUP BY property_type, room_type,accommodates, year_month
ORDER BY property_type, room_type,accommodates, year_month
)
SELECT
  property_type,
  room_type,
  accommodates,
  year_month,
  active_listings_rate,
  min_price,
  max_price,
  median_price,
  avg_price,
  distinct_hosts,
  superhost_rate,
  avg_review_scores,
  total_stays,
  avg_estimated_revenue,
  CASE
    WHEN LAG(active_count) OVER w = 0 THEN 0
    ELSE
      (active_count - LAG(active_count) OVER w) * 100.0 / LAG(active_count) OVER w
  END AS percentage_change_active,
  CASE
    WHEN LAG(inactive_count) OVER w = 0 THEN 0
    ELSE
      (inactive_count - LAG(inactive_count) OVER w) * 100.0 / LAG(inactive_count) OVER w
  END AS percentage_change_inactive
FROM property_stat
WINDOW w AS (PARTITION BY property_type, room_type,accommodates ORDER BY year_month)