SELECT
  dv.vehicletype,
  dl.street AS origin_street,
  dld.street AS destination_street,
  COUNT(fvm.movementkey)*0.09 AS total_movements
FROM
  DEFAULT.fact_vehiclemovement fvm
  JOIN DEFAULT.dim_vehicle dv ON fvm.vehiclekey = dv.vehiclekey
  JOIN DEFAULT.dim_location dl ON fvm.locationkey = dl.locationkey
  JOIN DEFAULT.dim_location dld ON fvm.locationdestinationkey = dld.locationkey
GROUP BY
  dv.vehicletype,
  dl.street,
  dld.street
ORDER BY
  total_movements
limit 100