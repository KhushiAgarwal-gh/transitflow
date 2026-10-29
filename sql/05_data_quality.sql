-- Silver layer data quality checks
SELECT 'trips_missing_actual_departure' AS check_name,
       COUNT(*) AS bad_rows
FROM silver_trips WHERE actual_departure IS NULL AND status != 'Cancelled'
UNION ALL
SELECT 'trips_unknown_route', COUNT(*)
FROM silver_trips t LEFT JOIN silver_routes r ON t.route_id = r.route_id
WHERE r.route_id IS NULL
UNION ALL
SELECT 'negative_passengers', COUNT(*) FROM silver_passenger_counts WHERE passengers_boarded < 0
UNION ALL
SELECT 'duplicate_trip_ids', COUNT(*) - COUNT(DISTINCT trip_id) FROM silver_trips
