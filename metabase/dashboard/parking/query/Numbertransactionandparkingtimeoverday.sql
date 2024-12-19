SELECT
  dt.date,
  dv.vehicletype,
  COUNT(*) AS adjusted_transaction_count,
  SUM(f.amount_paid) AS hourly_revenue,
  AVG(f.parking_duration) AS avg_parking_time
FROM
  DEFAULT.fact_parkingtransaction f
  JOIN DEFAULT.dim_time dt ON f.timekey = dt.timekey
  JOIN DEFAULT.dim_vehicle dv ON f.vehiclekey = dv.vehiclekey
GROUP BY
  dt.date,
  dv.vehicletype
ORDER BY
  dt.date,
  adjusted_transaction_count DESC