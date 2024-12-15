SELECT
  dl.street,
  AVG(fvm.passengercount) AS avg_passager,
  AVG(fvm.speed) * (RAND () * 0.5 + 0.6) AS avg_speed
FROM
  DEFAULT.fact_vehiclemovement fvm
  JOIN DEFAULT.dim_location dl ON fvm.locationkey = dl.locationkey
GROUP BY
  dl.street