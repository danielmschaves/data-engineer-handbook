WITH streak_started AS (
    SELECT 
        actor_name,
        actorid,
        current_season,
        quality_class,
        is_active,
        LAG(quality_class, 1) OVER (PARTITION BY actorid ORDER BY current_season) <> quality_class
        OR LAG(is_active, 1) OVER (PARTITION BY actorid ORDER BY current_season) <> is_active
        OR LAG(quality_class, 1) OVER (PARTITION BY actorid ORDER BY current_season) IS NULL
        AS did_change
    FROM actors
),
streak_identified AS (
    SELECT
        actor_name,
        actorid,
        quality_class,
        is_active,
        current_season,
        SUM(CASE WHEN did_change THEN 1 ELSE 0 END)
            OVER (PARTITION BY actorid ORDER BY current_season) as streak_identifier
    FROM streak_started
),
aggregated AS (
    SELECT
        actor_name,
        actorid,
        quality_class,
        is_active,
        streak_identifier,
        MIN(current_season) AS start_season,
        MAX(current_season) AS end_season
    FROM streak_identified
    GROUP BY 1,2,3,4,5
)
INSERT INTO actors_history_scd (
    actor_name,
    actorid,
    quality_class,
    is_active,
    start_season,
    end_season,
    current_season
)
SELECT 
    actor_name,
    actorid,
    quality_class,
    is_active,
    start_season,
    end_season,
    end_season as current_season  -- usando end_season como current_season
FROM aggregated
ORDER BY actorid, start_season;