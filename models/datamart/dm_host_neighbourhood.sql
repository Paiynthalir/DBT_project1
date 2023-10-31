{{
    config(
        unique_key=['host_neighbourhood_lga', 'year_month']
    )
}}


-- dm-host-neighbourhood
SELECT
  host_neighbourhood_lga_name as host_neighbourhood_lga,
  CONCAT(EXTRACT(YEAR FROM scraped_date), '-', LPAD(EXTRACT(MONTH FROM scraped_date)::TEXT, 2, '0')) AS year_month,
  -- Calculating no of distinct hosts
  COUNT(DISTINCT host_id) AS distinct_hosts,
  -- Estimated revenue calculations 
  SUM(CASE WHEN has_availability = 't' THEN (30 - availability_30) * price ELSE 0 END) AS estimated_revenue,
  -- Estimated revenue calculations - Estimated revenue per host= Total Estimated revenue per active listings/ total distinct hosts
  round((sum((30 - availability_30)*price)/(count(distinct host_id)))::numeric,2) as estimated_revenue_host
FROM {{ ref('facts_listings') }}
GROUP BY host_neighbourhood_lga, year_month
ORDER BY host_neighbourhood_lga, year_month