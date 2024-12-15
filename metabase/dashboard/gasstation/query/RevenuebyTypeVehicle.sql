SELECT
  c.vehicletypename,
  SUM(f.totalamount) * (RAND () * 0.8 + 0.2) AS total_revenue,
  SUM(f.quantitysold) * (RAND () * 0.8 + 0.2) AS total_quantity_sold
FROM
  DEFAULT.fact_gasstationtransaction f
  JOIN DEFAULT.dim_customer c ON f.customerkey = c.customerkey
GROUP BY
  c.vehicletypename
ORDER BY
  total_revenue DESC