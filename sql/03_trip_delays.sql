-- Routes ranked by delay: avg, p90 and % of delayed trips
SELECT route_id,
       route_name,
       mode,
       trips_measured,
       avg_delay_min,
       p90_delay_min,
       pct_delayed,
       RANK() OVER (ORDER BY pct_delayed DESC) AS delay_rank
FROM gold_delay_summary
ORDER BY delay_rank
