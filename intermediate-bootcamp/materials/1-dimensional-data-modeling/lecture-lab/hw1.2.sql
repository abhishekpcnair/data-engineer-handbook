create table players_scd_table
(
	player_name text,
	scoring_class scoring_class,
	is_active boolean,
	start_season integer,
	end_date integer,
	current_season INTEGER,
    PRIMARY KEY (player_name, current_season)
);

WITH with_previous AS (SELECT player_name,
                              current_season,
                              scoring_class,
                              LAG(scoring_class, 1) OVER (PARTITION BY player_name ORDER BY current_season) as previous_scoring_class,
                              LAG(is_active, 1) OVER (PARTITION BY player_name ORDER BY current_season) as previous_active_class,
                              is_active
                       FROM players),
with_indicators AS (
    SELECT *, CASE
    WHEN scoring_class <> previous_scoring_class THEN 1
    WHEN is_active <> previous_active_class THEN 1
    ELSE 0
    END AS change_indicator
    FROM with_previous
),
with_streaks as (
SELECT *, SUM(change_indicator)
          OVER (PARTITION BY player_name ORDER BY current_season) AS streak_indicator FROM with_indicators
)
SELECT player_name,
       streak_indicator,
       is_active,
       scoring_class,
       MIN(current_season) as start_season,
       MAX(current_season) as end_season
FROM  with_streaks
GROUP BY player_name, streak_indicator, is_active, scoring_class
order by player_name, streak_indicator;