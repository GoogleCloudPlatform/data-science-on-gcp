SELECT 
    COUNT(*) AS true_negatives
FROM dsongcp.flights
WHERE dep_delay < 15 AND arr_delay < 15
