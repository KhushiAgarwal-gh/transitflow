-- Peak vs off-peak: hour-wise ridership and delay by mode
SELECT trip_hour,
       SUM(total_passengers)            AS passengers,
       ROUND(AVG(avg_delay_min), 2)     AS avg_delay_min,
       CASE WHEN trip_hour IN (7, 8, 9, 17, 18, 19) THEN 'Peak' ELSE 'Off-peak' END AS period
FROM gold_hourly
GROUP BY trip_hour
ORDER BY trip_hour
