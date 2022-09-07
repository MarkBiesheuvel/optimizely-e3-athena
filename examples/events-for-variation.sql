-- off | 44475
-- #1  | 44477
-- #2  | 44478
-- #3  | 44480
-- #4  | 44479

WITH input AS (
    SELECT '9300000061857' AS experiment_id, '44479' AS variation_id
),
earliest_decisions AS (
    SELECT visitor_id, MIN(timestamp) as timestamp
    FROM input i
    INNER JOIN type_decisions d
        ON d.experiment_id = i.experiment_id
        AND d.variation_id = i.variation_id
    WHERE is_holdback = false
    GROUP BY visitor_id
)
SELECT COUNT (distinct e.visitor_id) as visitor_count
FROM input i
INNER JOIN  type_events e
    ON (element_at(e.experiments, 1)).experiment_id = i.experiment_id
    AND (element_at(e.experiments, 1)).variation_id = i.variation_id
INNER JOIN earliest_decisions d
    ON e.visitor_id = d.visitor_id AND e.timestamp >= d.timestamp;
