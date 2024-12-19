SELECT
  dv.vehicletype,
  dl.district,
  COUNT(*) AS transaction_count,
  SUM(f.amount_paid) AS total_revenue,
  AVG(f.parking_duration) AS avg_parking_time
FROM
  DEFAULT.fact_parkingtransaction f
  JOIN DEFAULT.dim_time dt ON f.timekey = dt.timekey
  JOIN DEFAULT.dim_vehicle dv ON f.vehiclekey = dv.vehiclekey
  JOIN DEFAULT.dim_location dl ON f.locationkey = dl.locationkey
GROUP BY
  dv.vehicletype,
  dl.district
ORDER BY
  transaction_count DESC