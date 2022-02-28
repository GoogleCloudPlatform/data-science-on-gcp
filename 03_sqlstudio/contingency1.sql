SELECT 
    COUNT(*) AS true_positives
FROM dsongcp.flights
WHERE dep_delay < 15 AND arr_delay < 15
