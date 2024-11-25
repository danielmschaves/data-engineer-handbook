-- Task 3: DDL para tabela actors_history_scd
CREATE TYPE actor_scd_type AS (
    quality_class quality_class,
    is_active boolean,
    start_date INTEGER,
    end_date INTEGER
);

CREATE TABLE actors_history_scd (
    actor_name text,
    actorid text,
    quality_class quality_class,
    is_active boolean,
    start_date integer,
    end_date integer,
    current_year integer,
    PRIMARY KEY (actorid, start_date, end_date)
);