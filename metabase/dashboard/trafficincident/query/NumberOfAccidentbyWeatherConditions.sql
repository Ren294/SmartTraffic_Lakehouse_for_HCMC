SELECT
  dt.date,
  dt.hour,
  dw.conditions,
  COUNT(*) AS accident_count
FROM
  DEFAULT.fact_trafficincident ft
  JOIN DEFAULT.dim_time dt ON round(ft.timekey, -1) = dt.timekey
  JOIN DEFAULT.dim_weather dw ON round(ft.timekey, -3) = dw.datetime / 1000000
GROUP BY
  dt.date,
  dt.hour,
  dw.conditions
ORDER BY
  dt.date,
  dt.hour,
  dw.conditions