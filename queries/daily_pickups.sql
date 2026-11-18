SELECT pickup_zone, COUNT(*) AS trips, SUM(total_usd) AS total_usd
FROM eligible_trips
WHERE service_day = :service_day
GROUP BY pickup_zone
ORDER BY pickup_zone
