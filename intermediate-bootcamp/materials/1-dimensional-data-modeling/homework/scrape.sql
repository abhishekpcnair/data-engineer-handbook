SELECT min(year) FROM actor_films;

CREATE TYPE film_struct AS(
    film VARCHAR,
    votes INT,
    rating FLOAT,
    filmid VARCHAR
);

-- DROP TYPE films;

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

DROP TABLE actors;

-- with yesterday AS (
--     SELECT * FROM actors WHERE year = 1970
-- ),
-- today AS (
--     SELECT * FROM actor_films WHERE year = 1971
-- )
-- SELECT
--     COALESCE(yesterday.actor, today.actor) as actor,
--     COALESCE(yesterday.year, today.year) AS year,
--     COALESCE(yesterday.rating, today.rating) AS rating,
--     (CASE
--         WHEN today.rating > 8 THEN 'star'
--         WHEN today.rating > 7 THEN 'good'
--         WHEN today.rating > 6 THEN 'average'
--         ELSE 'bad' END
--         )::quality_class,
--     COALESCE(yesterday.films, ARRAY[]::film_struct[]) ||
--         CASE WHEN today.film IS NOT NULL THEN
--         ARRAY[ROW(
--             today.film,
--             today.votes,
--             today.rating,
--             today.filmid
--             )::film_struct] ELSE ARRAY[]:: film_struct[] END
--         as film
-- FROM yesterday FULL OUTER JOIN today ON yesterday.filmid = today.filmid;


-- with yesterday AS (
--     SELECT * FROM actor_films WHERE year = 1971
-- ),
-- today AS (
--     SELECT * FROM actor_films WHERE year = 1972
-- )
-- SELECT
--     COALESCE(yesterday.actor, today.actor) as actor,
--     COALESCE(yesterday.year, today.year) AS year,
--     COALESCE(yesterday.rating, today.rating) AS rating,
--     yesterday.rating as y_rating,
--     today.rating as t_rating,
--     COALESCE(yesterday.film, today.film) AS film,
--     (CASE
--         WHEN today.rating > 8 THEN 'star'
--         WHEN today.rating > 7 THEN 'good'
--         WHEN today.rating > 6 THEN 'average'
--         ELSE 'bad' END
--         )::quality_class
-- FROM yesterday FULL OUTER JOIN today ON yesterday.actor = today.actor where COALESCE(yesterday.actor, today.actor) = 'Alain Delon';
--
-- SELECT * FROM actor_films where actor = 'Alain Delon' AND year = 1971;

---

WITH agg_year AS (
SELECT actor, avg(rating) as rating,ARRAY_AGG(
        ROW (
                  film,
                  votes,
                  rating,
                  filmid
                  )::film_struct
        ) AS films,
    year
FROM actor_films WHERE film IS NOT NULL GROUP BY actor,year ORDER BY actor),
previous_year AS (
    SELECT * FROM agg_year WHERE year = 1998
),
current_year AS (
    SELECT * FROM agg_year WHERE year = 1999
),
agg_result AS (
SELECT COALESCE(current_year.year, previous_year.year) as year,
        coalesce(previous_year.actor, current_year.actor) as actor, CASE
    WHEN previous_year.films IS NOT NULL AND current_year.films is NOT NULL
        THEN previous_year.films || current_year.films
    WHEN previous_year.films IS NULL AND current_year.films IS NULL
        THEN ARRAY[]::film_struct[]
    ELSE
        COALESCE(previous_year.films, current_year.films)
    END as films,
    CASE
    WHEN previous_year.rating IS NOT NULL AND current_year.rating IS NOT NULL
        THEN (previous_year.rating + current_year.rating)/2
    WHEN previous_year.rating IS NULL THEN current_year.rating
    WHEN current_year.rating IS NULL THEN previous_year.rating END AS average_rating
FROM previous_year
    LEFT OUTER JOIN current_year ON
    previous_year.actor = current_year.actor)
SELECT actor, year, films, (CASE
    WHEN average_rating > 8 THEN 'star'
    WHEN average_rating > 7 THEN 'good'
    WHEN average_rating > 6 THEN 'average'
    ELSE 'bad' END)::quality_class
FROM agg_result;

SELECT * FROM actors;

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
