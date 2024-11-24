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

-- next


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