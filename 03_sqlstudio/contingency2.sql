DECLARE THRESH INT64;
SET THRESH = 15;
 
SELECT 
    COUNTIF(dep_delay < THRESH AND arr_delay < 15) AS true_positives,
    COUNTIF(dep_delay < THRESH AND arr_delay >= 15) AS false_positives,
    COUNTIF(dep_delay >= THRESH AND arr_delay < 15) AS false_negatives,
    COUNTIF(dep_delay >= THRESH AND arr_delay >= 15) AS true_negatives,
    COUNT(*) AS total
FROM dsongcp.flights
WHERE arr_delay IS NOT NULL AND dep_delay IS NOT NULL
