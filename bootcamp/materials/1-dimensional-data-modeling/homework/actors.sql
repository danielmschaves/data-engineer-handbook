-- Task 1: DDL para tabela actors e tipos necessários
CREATE TYPE film_details AS (
    film text,
    votes integer,
    rating real,
    filmid text
);

CREATE TYPE quality_class AS ENUM ('star', 'good', 'average', 'bad');

CREATE TABLE actors (
    actor_name text,
    actorid text,
    films film_details[],
    quality_class quality_class,
    is_active boolean,
    current_year integer,
    PRIMARY KEY (actorid, current_year)
);