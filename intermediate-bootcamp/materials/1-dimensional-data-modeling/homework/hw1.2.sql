CREATE TYPE quality_class AS ENUM ('star', 'good', 'average', 'bad');

create table actors_history_scd (
		actorid text,
		actor text,
		quality_class quality_class,
		is_active boolean,
		start_year integer,
		end_year integer,
		current_year integer,
		streak_identifier integer,
		primary key (actorid, start_year)
);

INSERT INTO actors_history_scd
with with_previous as (
select actorid,
		actor,
	current_year,
	quality_class,
	is_active,
	lag(quality_class, 1) over (partition by actorid order by current_year) as previous_quality_class,
	lag(is_active, 1) over (partition by actorid order by current_year) as previous_is_active
from actors
where current_year <= 1973
),
with_indicators as (
select *,
		case
			when quality_class <> previous_quality_class then 1
			when is_active <> previous_is_active then 1
			else 0
		end as change_indicator
from with_previous
),
with_streaks as (
select *,
	sum(change_indicator) over (partition by actorid order by current_year) as streak_identifier
from with_indicators
)
select actorid,
		actor,
		quality_class,
		is_active,
		min(current_year) as start_year,
		max(current_year) as end_year,
		1973 as current_year,
		streak_identifier
from with_streaks
group by actorid, actor, streak_identifier, is_active, quality_class
order by actorid, streak_identifier;

SELECT * FROM actors_history_scd;

-- incremental query for actors_history_scd table
with last_year_scd as (
		select *
		from actors_history_scd
		where current_year = 1976
		and end_year = 1976
),
	historical_scd as (
		select actorid,
				actor,
				quality_class,
				is_active,
				start_year,
				end_year
		from actors_history_scd
		where current_year = 1976
		and end_year < 1976
	),
	this_year_data as (
		select *
		from actors
		where current_year = 1977
	),
	unchanged_records as (
		select ts.actorid,
				ts.actor,
				ts.quality_class,
				ts.is_active,
				ls.start_year,
				ts.current_year as end_year
		from this_year_data ts
		join last_year_scd ls
		on ls.actorid = ts.actorid
		where ts.quality_class = ls.quality_class
		and ts.is_active = ls.is_active
	),
	changed_records as (
		select ts.actorid,
				ts.actor,
			unnest(array [
					row (
						ls.quality_class,
						ls.is_active,
						ls.start_year,
						ls.end_year
					)::scd_type_actor,
					row (
						ts.quality_class,
						ts.is_active,
						ts.current_year,
						ts.current_year
					)::scd_type_actor
				]) as records
		from this_year_data ts
		left join last_year_scd ls
		on ls.actorid = ts.actorid
		where (ts.quality_class <> ls.quality_class
		or ts.is_active <> ls.is_active)
		or ls.actorid is null
	),
	unnested_changed_records as (
			select actorid,
					actor,
					(records::scd_type_actor).quality_class,
					(records::scd_type_actor).is_active,
					(records::scd_type_actor).start_year,
					(records::scd_type_actor).end_year
			from changed_records
	),
	new_records as (
			select ts.actorid,
					ts.actor,
					ts.quality_class,
					ts.is_active,
					ts.current_year as start_year,
					ts.current_year as end_year
			from this_year_data ts
			join last_year_scd ls
			on ts.actorid = ls.actorid
			where ls.actorid is null

	)
	select * from historical_scd

	union all

	select * from unchanged_records

	union all

	select * from unnested_changed_records

	union all

	select * from new_records