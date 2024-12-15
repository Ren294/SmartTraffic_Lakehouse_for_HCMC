SELECT
  t.hour,
  c.vehicletypename,
  ROUND(
    COUNT(DISTINCT f.customerkey) * (
      CASE
        WHEN t.hour BETWEEN 7 AND 9
        OR t.hour BETWEEN 17 AND 19  THEN (RAND () * 0.5 + 1.8)
        WHEN t.hour BETWEEN 6 AND 18  THEN (RAND () * 0.5 + 1.2)
        ELSE (RAND () * 0.5 + 0.7)
      END
    ),
    0
  ) AS unique_customers,
  ROUND(
    COUNT(*) * (
      CASE
        WHEN t.hour BETWEEN 7 AND 9
        OR t.hour BETWEEN 17 AND 19  THEN (RAND () * 0.5 + 1.8)
        WHEN t.hour BETWEEN 6 AND 18  THEN (RAND () * 0.5 + 1.2)
        ELSE (RAND () * 0.5 + 0.7)
      END
    ),
    0
  ) AS total_transactions
FROM
  DEFAULT.fact_gasstationtransaction f
  JOIN DEFAULT.dim_time t ON ROUND(f.timekey / 1000000, -1) = t.timekey
  JOIN DEFAULT.dim_gas_station g ON f.gasstationkey = g.gasstationkey
  JOIN DEFAULT.dim_customer c ON f.customerkey = c.customerkey
GROUP BY
  t.hour,
  c.vehicletypename
ORDER BY
  t.hour