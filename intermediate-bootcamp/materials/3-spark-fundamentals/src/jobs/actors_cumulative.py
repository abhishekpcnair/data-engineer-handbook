from pyspark.sql import SparkSession

query = """

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
        COLLECT_LIST(
            STRUCT(
                film,
                votes,
                rating,
                filmid
            )
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

        CONCAT(
            COALESCE(y.films, ARRAY()), 
            COALESCE(t.films, ARRAY())
        ) AS films,

        CASE
            WHEN t.year IS NOT NULL THEN
                CASE
                    WHEN t.avg_rating > 8 THEN 'star'
                    WHEN t.avg_rating > 7 THEN 'good'
                    WHEN t.avg_rating > 6 THEN 'average'
                    ELSE 'bad'
                END
            ELSE y.quality_class
        END AS quality_class,

        (t.year IS NOT NULL) AS is_active,

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
FROM final_data

"""


def do_actors_scd_transformation(spark, df_actors, df_actor_films):
    # Register inputs as temporary views
    df_actors.createOrReplaceTempView("actors")
    df_actor_films.createOrReplaceTempView("actor_films")

    # Run SQL query and return resulting DataFrame
    return spark.sql(query)


def main():
    spark = SparkSession.builder \
        .appName("actors_scd") \
        .master("local") \
        .getOrCreate()

    # Load data here or read from persisted store
    # For example, replace with actual paths or data sources
    df_actors = spark.read.option("header", "true").csv("/home/iceberg/data/actors.csv")
    df_actor_films = spark.read.option("header", "true").csv("/home/iceberg/data/actor_films.csv")

    output_df = do_actors_scd_transformation(spark, df_actors, df_actor_films)

    # Write result (overwrite existing cumulative table or location)
    output_df.write.mode("overwrite").saveAsTable("actors_cumulative")

