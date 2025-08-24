SELECT min(year) FROM actor_films;

CREATE TYPE film_struct AS(
    film VARCHAR,
    votes INT,
    rating FLOAT,
    filmid VARCHAR
);

CREATE TYPE quality_class AS ENUM(
    'star', 'good', 'average', 'bad'
    );

CREATE TABLE actors (
    actor TEXT,
    year INT,
    films film_struct[],
    quality_class quality_class,
    is_active BOOLEAN,
    PRIMARY KEY (actor, year)
);

INSERT INTO actors
WITH agg_year AS (
    SELECT
        actor,
        year,
        ARRAY_AGG(
            ROW (
                film,
                votes,
                rating,
                filmid
            )::film_struct
        ) AS films,
        AVG(rating) AS avg_rating
    FROM actor_films
    WHERE film IS NOT NULL
    GROUP BY actor, year
),
previous_year AS (
    SELECT * FROM agg_year WHERE year = 2000
),
current_year AS (
    SELECT * FROM agg_year WHERE year = 2001
)
SELECT
    COALESCE(py.actor, cy.actor) AS actor,
    COALESCE(cy.year, py.year) AS year,
    COALESCE(py.films, ARRAY[]::film_struct[])
        || COALESCE(cy.films, ARRAY[]::film_struct[]) AS films,
    CASE
        WHEN cy.avg_rating IS NOT NULL THEN
            CASE
                WHEN cy.avg_rating > 8 THEN 'star'
                WHEN cy.avg_rating > 7 THEN 'good'
                WHEN cy.avg_rating > 6 THEN 'average'
                ELSE 'bad'
            END::quality_class
        ELSE NULL
    END AS quality_class,
    cy.year IS NOT NULL AS is_active
FROM previous_year py
FULL OUTER JOIN current_year cy
    ON py.actor = cy.actor
ON CONFLICT (actor, year) DO UPDATE
SET films = EXCLUDED.films,
    quality_class = EXCLUDED.quality_class,
    is_active = EXCLUDED.is_active;

SELECT * FROM actors;

WITH last_year AS (
    SELECT * FROM actors WHERE year = 2000
),
this_year AS (
    SELECT
        actor,
        year,
        ARRAY_AGG(ROW(film, votes, rating, filmid)::film_struct) AS films,
        AVG(rating) AS avg_rating
    FROM actor_films
    WHERE year = 2001
    GROUP BY actor, year
)
SELECT
    COALESCE(ly.actor, ty.actor) AS actor,
    2001 AS year,
    ty.films,
    CASE
        WHEN ty.avg_rating IS NOT NULL THEN
            CASE
                WHEN ty.avg_rating > 8 THEN 'star'
                WHEN ty.avg_rating > 7 THEN 'good'
                WHEN ty.avg_rating > 6 THEN 'average'
                ELSE 'bad'
            END::quality_class
        ELSE ly.quality_class
    END AS quality_class,
    ty.year IS NOT NULL AS is_active
FROM last_year ly
FULL OUTER JOIN this_year ty
    ON ly.actor = ty.actor;


