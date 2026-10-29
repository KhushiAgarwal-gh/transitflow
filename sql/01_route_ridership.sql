-- Route-wise ridership (total, avg per trip, share of network)
SELECT route_id,
       route_name,
       mode,
       SUM(total_passengers)                                            AS total_passengers,
       SUM(total_trips)                                                 AS total_trips,
       ROUND(SUM(total_passengers) / NULLIF(SUM(total_trips), 0), 2)    AS passengers_per_trip,
       ROUND(100 * SUM(total_passengers) / SUM(SUM(total_passengers)) OVER (), 2) AS pct_of_network
FROM gold_route_daily
GROUP BY route_id, route_name, mode
ORDER BY total_passengers DESC
