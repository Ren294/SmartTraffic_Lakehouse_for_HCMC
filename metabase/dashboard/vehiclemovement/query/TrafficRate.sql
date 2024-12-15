SELECT
  dv.vehicletype,
  COUNT(fvm.movementkey) AS total_movements
FROM
  DEFAULT.fact_vehiclemovement fvm
  JOIN DEFAULT.dim_vehicle dv ON fvm.vehiclekey = dv.vehiclekey
GROUP BY
  dv.vehicletype
ORDER BY
  total_movements desc