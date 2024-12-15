SELECT
  t.date,
  SUM(f.totalamount) AS total_revenue,
  SUM(f.quantitysold) AS total_quantity_sold
FROM
  default.fact_gasstationtransaction f
  JOIN default.dim_time t ON round(f.timekey / 1000000, -1) = t.timekey
GROUP BY
  t.date
ORDER BY
  t.date