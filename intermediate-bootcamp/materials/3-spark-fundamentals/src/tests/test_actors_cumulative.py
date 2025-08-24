from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import SparkSession
from ..jobs.actors_cumulative import do_actors_scd_transformation
from collections import namedtuple

Actor = namedtuple("Actor", "actorid actor current_year quality_class films is_active")
ActorFilm = namedtuple("ActorFilm", "actorid actor film votes rating filmid year")


def test_actors_scd_transformation(spark: SparkSession):
    # Create empty actors table for 'yesterday'
    actors_data = [
        Actor("a1", "Actor A", 1973, "good", None, False)
    ]
    actors_df = spark.createDataFrame(actors_data)

    # Create films data for 'today'
    actor_films_data = [
        ActorFilm("a1", "Actor A", "Film A", 100, 8.5, "f1", 1974),
        ActorFilm("a1", "Actor A", "Film B", 150, 7.8, "f2", 1974),
        ActorFilm("a2", "Actor B", "Film C", 200, 9.0, "f3", 1974)
    ]
    actor_films_df = spark.createDataFrame(actor_films_data)

    # Run the transformation
    actual_df = do_actors_scd_transformation(spark, actors_df, actor_films_df)

    # Prepare expected films as arrays of structs
    expected_films_a1 = [
        {"film": "Film A", "votes": 100, "rating": 8.5, "filmid": "f1"},
        {"film": "Film B", "votes": 150, "rating": 7.8, "filmid": "f2"}
    ]
    expected_films_a2 = [
        {"film": "Film C", "votes": 200, "rating": 9.0, "filmid": "f3"},
    ]

    expected_data = [
        ("a1", "Actor A", expected_films_a1, "good", True, 1974),
        ("a2", "Actor B", expected_films_a2, "star", True, 1974),
    ]
    expected_schema = actual_df.schema
    expected_df = spark.createDataFrame(expected_data, schema=expected_schema)

    # Assert DataFrames are equal ignoring row order
    assert_df_equality(actual_df, expected_df, ignore_row_order=True, ignore_column_order=True)
