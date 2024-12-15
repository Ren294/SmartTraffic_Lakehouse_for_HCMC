SELECT 
    dl.city,
    dl.district,
    SUM(CASE WHEN ft.accidentseverity = 1 THEN 1 ELSE 0 END) *0.01 AS minor_accidents,
    SUM(CASE WHEN ft.accidentseverity = 2 THEN 1 ELSE 0 END) *0.01 AS moderate_accidents,
    SUM(CASE WHEN ft.accidentseverity = 3 THEN 1 ELSE 0 END) *0.01 AS severe_accidents
FROM 
    default.fact_trafficincident ft
JOIN 
    default.dim_location dl ON ft.locationkey = dl.locationkey
GROUP BY 
    dl.city, dl.district
ORDER BY 
    dl.city, dl.district