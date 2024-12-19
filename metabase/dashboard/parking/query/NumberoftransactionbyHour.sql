SELECT
  dt.hour,
  dv.vehicletype,
  COUNT(*) * CASE
    WHEN dt.hour BETWEEN 7 AND 9  THEN 1.5 * (rand () * 0.5 + 0.5)
    WHEN dt.hour BETWEEN 17 AND 20  THEN 1.5 * (rand () * 0.5 + 0.5)
    WHEN dt.hour >= 22
    OR dt.hour <= 5 THEN 0.7 * (rand () * 0.5 + 0.5)
    ELSE 1
  END AS adjusted_transaction_count,
  SUM(f.amount_paid) AS hourly_revenue,
  AVG(f.parking_duration) AS avg_parking_time
FROM
  DEFAULT.fact_parkingtransaction f
  JOIN DEFAULT.dim_time dt ON f.timekey = dt.timekey
  JOIN DEFAULT.dim_vehicle dv ON f.vehiclekey = dv.vehiclekey
GROUP BY
  dt.hour,
  dv.vehicletype
ORDER BY
  dt.hour,
  adjusted_transaction_count DESC