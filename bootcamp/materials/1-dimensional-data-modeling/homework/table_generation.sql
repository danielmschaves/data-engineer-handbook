-- Task 2: Query para geração cumulativa da tabela actors
-- Initial load (primeiro ano)
WITH first_year AS ( 
   SELECT
       actor,
       actorid,
       year,
       film,
       votes,
       rating,
       filmid,
       AVG(rating) OVER (PARTITION BY actorid) as avg_year_rating,
       ROW_NUMBER() OVER (PARTITION BY actorid ORDER BY rating DESC) as rn
   FROM actor_films
   WHERE year = (SELECT MIN(year) FROM actor_films)
),
first_year_films AS (
   SELECT
       actor, 
       actorid,
       ARRAY_AGG(ROW(film, votes, rating, filmid)::film_details ORDER BY rating DESC) as films,
       avg_year_rating
   FROM first_year
   GROUP BY actor, actorid, avg_year_rating
)
SELECT 
   actor as actor_name,
   actorid,
   films,
   CASE
       WHEN avg_year_rating > 8 THEN 'star'
       WHEN avg_year_rating > 7 THEN 'good'
       WHEN avg_year_rating > 6 THEN 'average'
       ELSE 'bad'
   END::quality_class as quality_class,
   true as is_active,
   (SELECT MIN(year) FROM actor_films) as current_year
FROM first_year_films;

-- Task 2 (Parte 2): Query para inserção dos anos subsequentes na tabela actors
INSERT INTO actors
WITH yesterday AS (
    SELECT * FROM actors
    WHERE current_year = 1980  
),
today AS (
    SELECT
        actor,
        actorid,
        year,
        film,
        votes,
        rating,
        filmid,
        AVG(rating) OVER (PARTITION BY actorid) as avg_year_rating,
        ROW_NUMBER() OVER (PARTITION BY actorid ORDER BY rating DESC) as rn
    FROM actor_films
    WHERE year = 1981  -- Ano atual
)
SELECT 
    COALESCE(t.actor, y.actor_name) as actor_name,
    COALESCE(t.actorid, y.actorid) as actorid,
    CASE
        WHEN y.films IS NULL THEN
            ARRAY[ROW(t.film, t.votes, t.rating, t.filmid)::film_details]
        WHEN t.film IS NOT NULL THEN 
            y.films || ARRAY[ROW(t.film, t.votes, t.rating, t.filmid)::film_details]
        ELSE y.films
    END as films,
    CASE
        WHEN t.film IS NOT NULL then
            CASE
                WHEN t.avg_year_rating > 8 THEN 'star'
                WHEN t.avg_year_rating > 7 THEN 'good'
                WHEN t.avg_year_rating > 6 THEN 'average'
                ELSE 'bad'
            END::quality_class
        ELSE y.quality_class
    END as quality_class,
    CASE
        WHEN t.film IS NOT NULL THEN true
        ELSE false
    END as is_active,
    COALESCE(t.year, y.current_year + 1) as current_year 
FROM today t
FULL OUTER JOIN yesterday y ON t.actorid = y.actorid
WHERE t.rn = 1 OR t.rn IS NULL;