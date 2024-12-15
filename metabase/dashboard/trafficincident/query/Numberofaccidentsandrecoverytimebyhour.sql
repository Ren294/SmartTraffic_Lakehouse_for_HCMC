SELECT 
    dt.date,
    dt.hour,
    AVG(ft.estimatedrecoverytime) * (RAND() * 0.5 + 1)/36000000000000 AS avg_recovery_time,
    COUNT(*) AS accident_count
FROM 
    default.fact_trafficincident ft
JOIN 
    default.dim_time dt ON ft.timekey = dt.timekey
GROUP BY 
    dt.date, dt.hour
ORDER BY 
    dt.date, dt.hour