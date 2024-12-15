SELECT 
    dt.date,
    dt.hour,
    SUM(ft.carinvolved) * 0.01 AS cars_involved,
    SUM(ft.motorbikeinvolved)* 0.01 AS motorbikes_involved,
    SUM(ft.othervehiclesinvolved)* 0.01 AS other_vehicles_involved
FROM 
    default.fact_trafficincident ft
JOIN 
    default.dim_time dt ON ft.timekey = dt.timekey
GROUP BY 
    dt.date, dt.hour
ORDER BY 
    dt.date, dt.hour