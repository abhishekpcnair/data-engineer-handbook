-- creating the type for the quality_class column
CREATE TYPE quality_class AS ENUM ('star', 'good', 'average', 'bad');

-- creating the films array
create type films as (
			film text,
			votes INTEGER,
			rating real,
			filmid text
);

-- the DDL for the actors table
create table actors (
		actor text,
		actorid text,
		films films[],
		quality_class quality_class,
		is_active boolean,
		current_year integer,
		primary key (actorid, current_year)
);

-- cumulative table generation query
INSERT INTO actors
WITH yesterday AS (
    SELECT *
    FROM actors
    WHERE current_year = 1973
),
today AS (
    SELECT
        actorid,
        actor,
        year,
        ARRAY_AGG(
            ROW(
                film,
                votes,
                rating,
                filmid
            )::films
        ) AS films,
        AVG(rating) AS avg_rating
    FROM actor_films
    WHERE year = 1974
    GROUP BY actorid, actor, year
),
final_data AS (
    SELECT
        COALESCE(t.actorid, y.actorid) AS actorid,
        COALESCE(t.actor, y.actor) AS actor,

        -- merge yesterday’s films with today’s (if any)
        COALESCE(y.films, ARRAY[]::films[])
            || COALESCE(t.films, ARRAY[]::films[]) AS films,

        -- quality_class comes from *this year’s avg rating* if active,
        -- otherwise keep last year's value
        CASE
            WHEN t.year IS NOT NULL THEN
                CASE
                    WHEN t.avg_rating > 8 THEN 'star'
                    WHEN t.avg_rating > 7 THEN 'good'
                    WHEN t.avg_rating > 6 THEN 'average'
                    ELSE 'bad'
                END::quality_class
            ELSE y.quality_class
        END AS quality_class,

        -- active if in today’s data
        (t.year IS NOT NULL) AS is_active,

        -- carry forward year correctly
        COALESCE(t.year, y.current_year + 1) AS current_year
    FROM today t
    FULL OUTER JOIN yesterday y
        ON t.actorid = y.actorid
)

SELECT
    actorid,
    actor,
    films,
    quality_class,
    is_active,
    current_year
FROM final_data;

