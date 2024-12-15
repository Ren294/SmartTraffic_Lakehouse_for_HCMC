SELECT
  p.productname,
  c.vehicletypename,
  SUM(f.totalamount) * (RAND () * 1 + 0.1) AS total_revenue,
  SUM(f.quantitysold) * (RAND () * 1 + 1) AS total_quantity_sold
FROM
  DEFAULT.fact_gasstationtransaction f
  JOIN DEFAULT.dim_product p ON f.productkey = p.productkey
  JOIN DEFAULT.dim_customer c ON f.customerkey = c.customerkey
GROUP BY
  p.productname,
  c.vehicletypename
ORDER BY
  p.productname,
  total_revenue DESC