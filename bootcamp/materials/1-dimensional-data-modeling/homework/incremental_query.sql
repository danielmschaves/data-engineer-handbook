-- Task 5: Query incremental para actors_history_scd
WITH vars AS (
    SELECT 
        1975 as previous_year,
        1976 as current_year
),
last_season_scd AS ( 
    SELECT * FROM actors_history_scd, vars
    WHERE current_year = previous_year
    AND end_date = previous_year
),
historical_scd AS (
    SELECT 
        actor_name,
        actorid,
        quality_class,
        is_active,
        start_date,
        end_date
    FROM actors_history_scd, vars
    WHERE current_year = previous_year
    AND end_date < previous_year
),
this_season_data AS (
    SELECT * FROM actors, vars
    WHERE current_year = current_year
),
unchanged_records AS (
    SELECT 
        ts.actor_name,
        ts.actorid,
        ts.quality_class,
        ts.is_active,
        ls.start_date,
        ts.current_year as end_date
    FROM this_season_data ts
    JOIN last_season_scd ls ON ls.actorid = ts.actorid
    WHERE ts.quality_class = ls.quality_class
    AND ts.is_active = ls.is_active
),
changed_records AS (
    SELECT 
        ts.actor_name,
        ts.actorid,
        UNNEST(ARRAY[
            ROW(
                ls.quality_class,
                ls.is_active,
                ls.start_date,
                ls.end_date
            )::actor_scd_type,
            ROW(
                ts.quality_class,
                ts.is_active,
                ts.current_year,
                ts.current_year
            )::actor_scd_type
        ]) as records
    FROM this_season_data ts
    LEFT JOIN last_season_scd ls ON ls.actorid = ts.actorid
    WHERE (ts.quality_class <> ls.quality_class
        OR ts.is_active <> ls.is_active)
),
unnested_changed_records AS (
    SELECT 
        actor_name,
        actorid,
        (records::actor_scd_type).quality_class,
        (records::actor_scd_type).is_active,
        (records::actor_scd_type).start_date,
        (records::actor_scd_type).end_date
    FROM changed_records
),
new_records AS (
    SELECT 
        ts.actor_name,
        ts.actorid,
        ts.quality_class,
        ts.is_active,
        ts.current_year AS start_date,
        ts.current_year AS end_date
    FROM this_season_data ts
    LEFT JOIN last_season_scd ls ON ts.actorid = ls.actorid
    WHERE ls.actorid IS NULL
),
all_records AS (
    SELECT actor_name, actorid, quality_class, is_active, start_date, end_date
    FROM historical_scd
    UNION ALL
    SELECT actor_name, actorid, quality_class, is_active, start_date, end_date
    FROM unchanged_records
    UNION ALL
    SELECT actor_name, actorid, quality_class, is_active, start_date, end_date
    FROM unnested_changed_records
    UNION ALL
    SELECT actor_name, actorid, quality_class, is_active, start_date, end_date
    FROM new_records
)
SELECT 
    actor_name,
    actorid,
    quality_class,
    is_active,
    start_date,
    end_date,
    (SELECT current_year FROM vars) as current_year
FROM all_records
ORDER BY actorid, start_date;