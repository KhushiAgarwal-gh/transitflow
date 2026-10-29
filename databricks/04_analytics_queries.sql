-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Analytics queries (Databricks SQL / dashboard ke liye)
-- MAGIC Tables: `main.transitflow.gold_*`

-- COMMAND ----------
-- Route-wise ridership
SELECT route_id, route_name, mode, SUM(total_passengers) AS total_passengers,
       ROUND(100 * SUM(total_passengers) / SUM(SUM(total_passengers)) OVER (), 2) AS pct_of_network
FROM main.transitflow.gold_route_daily
GROUP BY route_id, route_name, mode
ORDER BY total_passengers DESC;

-- COMMAND ----------
-- Passenger trend: 7-day moving average
SELECT trip_date, SUM(total_passengers) AS passengers,
       ROUND(AVG(SUM(total_passengers)) OVER (ORDER BY trip_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 1) AS moving_avg_7d
FROM main.transitflow.gold_route_daily
GROUP BY trip_date ORDER BY trip_date;

-- COMMAND ----------
-- Worst routes by delay
SELECT route_name, avg_delay_min, p90_delay_min, pct_delayed
FROM main.transitflow.gold_delay_summary
ORDER BY pct_delayed DESC;
