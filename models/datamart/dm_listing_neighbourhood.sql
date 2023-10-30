
{{
    config(
        unique_key=['listing_neighbourhood','year_month']
    )
}}

-- dm_listing_neighbourhood
with monthly_perneighbourhood as (
SELECT
  listing_neighbourhood,
  CONCAT(EXTRACT(YEAR FROM scraped_date), '-', LPAD(EXTRACT(MONTH FROM scraped_date)::TEXT, 2, '0')) AS year_month,
  ROUND(
    (COUNT(CASE WHEN has_availability = 't' THEN 1 ELSE NULL END) * 100.0) / COUNT(*),
    2
  ) AS active_listings_rate,
  MIN(CASE WHEN has_availability = 't' THEN price ELSE 0 END) AS min_price,
  MAX(CASE WHEN has_availability = 't' THEN price ELSE 0 END) AS max_price,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CASE WHEN has_availability = 't' THEN price ELSE 0 END) AS median_price,
  ROUND(
    AVG(CASE WHEN has_availability = 't' THEN price ELSE 0 END),
    2
  ) AS avg_price,
  COUNT(DISTINCT host_id) AS distinct_hosts,
  ROUND(
    (COUNT(DISTINCT CASE WHEN host_is_superhost = 't' THEN host_id ELSE NULL END) * 100.0) / COUNT(DISTINCT host_id),
    2
  ) AS superhost_rate,
  SUM(CASE WHEN has_availability = 't' THEN (30 - availability_30) ELSE 0 END) AS total_stays,
  ROUND(
    AVG(CASE WHEN has_availability = 't' THEN review_scores_rating ELSE 0 END),
    2
  ) AS avg_review_scores,
  SUM(CASE WHEN has_availability = 't' THEN 1 ELSE 0 END) AS active_count,
  SUM(CASE WHEN has_availability = 'f' THEN 1 ELSE 0 END) AS inactive_count
FROM {{ ref('facts_listings') }}
GROUP BY listing_neighbourhood, year_month
ORDER BY listing_neighbourhood, year_month
)
SELECT
  *,
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
FROM monthly_perneighbourhood
WINDOW w AS (PARTITION BY listing_neighbourhood ORDER BY year_month)