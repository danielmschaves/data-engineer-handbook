WITH vars AS (
    SELECT 
        1975 as previous_year,
        1976 as current_year
),
-- Registros do ano anterior que ainda estão ativos (end_season = current_season)
last_season_scd AS ( 
    SELECT * FROM actors_history_scd, vars
    WHERE current_season = previous_year
    AND end_season = previous_year
),
-- Registros históricos já fechados (end_season < current_season)
historical_scd AS (
    SELECT 
        actor_name,
        actorid,
        quality_class,
        is_active,
        start_season,
        end_season
    FROM actors_history_scd, vars
    WHERE current_season = previous_year
    AND end_season < previous_year
),
-- Dados do ano atual da tabela principal
this_season_data AS (
    SELECT * FROM actors, vars
    WHERE current_season = current_year
),
-- Registros que não mudaram quality_class nem is_active
unchanged_records AS (
    SELECT 
        ts.actor_name,
        ts.actorid,
        ts.quality_class,
        ts.is_active,
        ls.start_season,
        ts.current_season as end_season
    FROM this_season_data ts
    JOIN last_season_scd ls ON ls.actorid = ts.actorid
    WHERE ts.quality_class = ls.quality_class
    AND ts.is_active = ls.is_active
),
-- Registros que tiveram mudança
-- Gera dois registros: um fechando o antigo e outro iniciando o novo
changed_records AS (
    SELECT 
        ts.actor_name,
        ts.actorid,
        UNNEST(ARRAY[
            ROW(
                ls.quality_class,
                ls.is_active,
                ls.start_season,
                ls.end_season
            )::actor_scd_type,
            ROW(
                ts.quality_class,
                ts.is_active,
                ts.current_season,
                ts.current_season
            )::actor_scd_type
        ]) as records
    FROM this_season_data ts
    LEFT JOIN last_season_scd ls ON ls.actorid = ts.actorid
    WHERE (ts.quality_class <> ls.quality_class
        OR ts.is_active <> ls.is_active)
),
-- Desnormaliza os registros que mudaram em linhas separadas
unnested_changed_records AS (
    SELECT 
        actor_name,
        actorid,
        (records::actor_scd_type).quality_class,
        (records::actor_scd_type).is_active,
        (records::actor_scd_type).start_season,
        (records::actor_scd_type).end_season
    FROM changed_records
),
-- Novos atores que não existiam antes
new_records AS (
    SELECT 
        ts.actor_name,
        ts.actorid,
        ts.quality_class,
        ts.is_active,
        ts.current_season AS start_season,
        ts.current_season AS end_season
    FROM this_season_data ts
    LEFT JOIN last_season_scd ls ON ts.actorid = ls.actorid
    WHERE ls.actorid IS NULL
),
-- Une todos os tipos de registros
all_records AS (
    SELECT 
        actor_name,
        actorid,
        quality_class,
        is_active,
        start_season,
        end_season
    FROM historical_scd
    UNION ALL
    SELECT 
        actor_name,
        actorid,
        quality_class,
        is_active,
        start_season,
        end_season
    FROM unchanged_records
    UNION ALL
    SELECT 
        actor_name,
        actorid,
        quality_class,
        is_active,
        start_season,
        end_season
    FROM unnested_changed_records
    UNION ALL
    SELECT 
        actor_name,
        actorid,
        quality_class,
        is_active,
        start_season,
        end_season
    FROM new_records
)
-- Retorna todos os registros ordenados, adicionando o current_season
SELECT 
    actor_name,
    actorid,
    quality_class,
    is_active,
    start_season,
    end_season,
    (SELECT current_year FROM vars) as current_season
FROM all_records
ORDER BY actorid, start_season;
