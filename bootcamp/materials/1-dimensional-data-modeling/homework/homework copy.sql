-- Limpeza inicial das estruturas existentes
-- drop type film_details cascade
-- drop type quality_class cascade
-- drop table actors

-- Definição do tipo film_details
-- Estrutura que armazena informações de filmes para construção do array cumulativo

-- CREATE TYPE FILM_DETAILS - CUMULATIVE
CREATE TYPE film_details AS (
    film text,
    votes integer,
    rating real,
    filmid text
);

-- Definição do tipo quality_class
-- Enumeração que classifica a qualidade do ator baseado na média de ratings do ano mais recente
-- CREATE TYPE QUALITY_CLASS - MOST RECENT YEAR
CREATE TYPE quality_class AS ENUM ('star', 'good', 'average', 'bad');

-- TASK 1
-- CREATE TABLE
CREATE TABLE actors (
    actor_name text,
    actorid text,
    films film_details[],
    quality_class quality_class,
    is_active boolean,
    current_season integer,
    PRIMARY KEY (actorid, current_season)
);

--TESTS 
select * from actors 

SELECT MIN(year) as first_year
FROM actor_films;

select * from actors
where current_season = 1971

--TASK 2
-- INSERT FIRST HISTORIC REGISTER
INSERT INTO actors
WITH first_year AS ( 
   SELECT -- CTE que calcula métricas básicas do primeiro ano
       actor,
       actorid,
       year,
       film,
       votes,
       rating,
       filmid,
       AVG(rating) OVER (PARTITION BY actorid) as avg_year_rating, -- Média de ratings por ator no ano
       ROW_NUMBER() OVER (PARTITION BY actorid ORDER BY rating DESC) as rn -- Numeração para garantir um registro por ator
   FROM actor_films
   WHERE year = (SELECT MIN(year) FROM actor_films)
),
first_year_films AS (
   SELECT -- CTE que agrupa os filmes em array por ator
       actor, 
       actorid,
       ARRAY_AGG(ROW(film, votes, rating, filmid)::film_details ORDER BY rating DESC) as films, -- Array ordenado por rating
       avg_year_rating
   FROM first_year
   GROUP BY actor, actorid, avg_year_rating
)
SELECT 
   actor as actor_name,
   actorid,
   films,
   case  -- Determina quality_class baseado na média dos ratings do ano
       WHEN avg_year_rating > 8 THEN 'star'
       WHEN avg_year_rating > 7 THEN 'good'
       WHEN avg_year_rating > 6 THEN 'average'
       ELSE 'bad'
   END::quality_class as quality_class,
   true as is_active, -- Todos ativos no primeiro ano
   (SELECT MIN(year) FROM actor_films) as current_season
FROM first_year_films;



-- TESTS
select * from actors
where actor_name = 'Marlon Brando'

DELETE FROM actors 
WHERE current_season = 1978;

--INSERT FOLLOWING YEARS

INSERT INTO actors
WITH yesterday AS (
    SELECT * FROM actors -- CTE com dados do ano anterior
    WHERE current_season = 1980 -- Ano anterior
),
today AS (
    SELECT -- CTE com dados do ano atual incluindo métricas calculadas
        actor,
        actorid,
        year,
        film,
        votes,
        rating,
        filmid,
        AVG(rating) OVER (PARTITION BY actorid) as avg_year_rating, -- Média de ratings por ator no ano atual
        ROW_NUMBER() OVER (PARTITION BY actorid ORDER BY rating DESC) as rn
    FROM actor_films
    WHERE year = 1980 -- Ano atual
)
SELECT 
    COALESCE(t.actor, y.actor_name) as actor_name,
    COALESCE(t.actorid, y.actorid) as actorid,
    CASE
        WHEN y.films IS NULL THEN -- Construção do array cumulativo de filmes
            ARRAY[ROW(t.film, t.votes, t.rating, t.filmid)::film_details]
        WHEN t.film IS NOT NULL THEN 
            y.films || ARRAY[ROW(t.film, t.votes, t.rating, t.filmid)::film_details]
        ELSE y.films
    END as films,
    CASE
        WHEN t.film IS NOT NULL then -- Atualização do quality_class baseado apenas nos filmes do ano atual
            CASE
                WHEN t.avg_year_rating > 8 THEN 'star'
                WHEN t.avg_year_rating > 7 THEN 'good'
                WHEN t.avg_year_rating > 6 THEN 'average'
                ELSE 'bad'
            END::quality_class
        ELSE y.quality_class
    END as quality_class,
    case -- Atualização do status de atividade
        WHEN t.film IS NOT NULL THEN true
        ELSE false
    END as is_active,
    COALESCE(t.year, y.current_season + 1) as current_season -- Incremento do ano
FROM today t
FULL OUTER JOIN yesterday y ON t.actorid = y.actorid
WHERE t.rn = 1 OR t.rn IS NULL; -- Garante um registro por ator


-- DDL FOR actors_history_scd
-- Implements type 2 dimension modeling (i.e., includes `start_date` and `end_date` fields).
-- Tracks `quality_class` and `is_active` status for each actor in the `actors` table.

drop table actors_history_scd 

-- Primeiro criamos o tipo SCD que vai armazenar o histórico de mudanças
CREATE TYPE actor_scd_type AS (
    quality_class quality_class,  -- Classificação de qualidade do ator
    is_active boolean,           -- Status de atividade
    start_season INTEGER,        -- Ano inicial do registro
    end_season INTEGER           -- Ano final do registro
);

-- TASK 3
-- Criação da tabela SCD tipo 2
CREATE TABLE actors_history_scd (
    actor_name text,
    actorid text,
    quality_class quality_class,
    is_active boolean,
    start_season integer,
    end_season integer,
    current_season integer,
    PRIMARY KEY (actorid, start_season, end_season)
);


--TASK 4
-- Query de backfill para actors_history_scd
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

-- TASK 5
-- Parâmetros de anos a serem processados
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

