-- off | 44475
-- #1  | 44477
-- #2  | 44478
-- #3  | 44480
-- #4  | 44479

WITH earliest_decisions AS (
    SELECT visitor_id, MIN(timestamp) as timestamp
    FROM type_decisions d
     WHERE experiment_id = '9300000061857'
        AND variation_id = '44480'
        AND is_holdback = false
    GROUP BY visitor_id
)
SELECT COUNT (distinct e.visitor_id) as visitor_count
FROM type_events e
INNER JOIN earliest_decisions d
    ON e.visitor_id = d.visitor_id
    AND e.timestamp >= d.timestamp
WHERE (element_at(e.experiments, 1)).experiment_id = '9300000061857'
    AND (element_at(e.experiments, 1)).variation_id = '44480'
