-- Daily network ridership with 7-day moving average and week-over-week change
WITH daily AS (
    SELECT trip_date, SUM(total_passengers) AS passengers
    FROM gold_route_daily
    GROUP BY trip_date
)
SELECT trip_date,
       passengers,
       ROUND(AVG(passengers) OVER (ORDER BY trip_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 1) AS moving_avg_7d,
       ROUND(100 * (passengers - LAG(passengers, 7) OVER (ORDER BY trip_date))
             / NULLIF(LAG(passengers, 7) OVER (ORDER BY trip_date), 0), 2) AS wow_change_pct
FROM daily
ORDER BY trip_date
