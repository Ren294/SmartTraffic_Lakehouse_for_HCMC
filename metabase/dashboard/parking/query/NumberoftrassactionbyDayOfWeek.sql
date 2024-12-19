SELECT
  CASE
    WHEN dt.dayofweek = 1 THEN 'Monday'
    WHEN dt.dayofweek = 2 THEN 'Tuesday'
    WHEN dt.dayofweek = 3 THEN 'Wednesday'
    WHEN dt.dayofweek = 4 THEN 'Thursday'
    WHEN dt.dayofweek = 5 THEN 'Friday'
    WHEN dt.dayofweek = 6 THEN 'Saturday'
    WHEN dt.dayofweek = 7 THEN 'Sunday'
  END AS day_name,
  COUNT(*) * CASE
    WHEN dt.dayofweek NOT IN (6, 7) THEN RAND () * 2 * 3
    ELSE 1
  END AS transaction_count,
  SUM(f.amount_paid) AS total_revenue,
  AVG(f.parking_duration) AS avg_parking_time
FROM
  DEFAULT.fact_parkingtransaction f
  JOIN DEFAULT.dim_time dt ON f.timekey = dt.timekey
  JOIN DEFAULT.dim_vehicle dv ON f.vehiclekey = dv.vehiclekey
  JOIN DEFAULT.dim_location dl ON f.locationkey = dl.locationkey
GROUP BY
  dt.dayofweek
ORDER BY
  transaction_count DESC