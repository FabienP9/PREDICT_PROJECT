/* 
    The purpose of this table is to retrieve seasons which changed in curated
    Inputs:
        curated_season: we get seasons from there
    Primary key:
        SEASON_KEY: from curated
    Filter:
        Only new seasons or seasons which changed since last run
    Materialization:
        Incremental to avoid deleting old seasons already in
*/
{{config(
    tags=['init_compet'],
    materialized="incremental",
    unique_key = ['SEASON_KEY']
)}}
-- we get last run timestamp if exists
{% if is_incremental() %}
    {% set results = run_query("SELECT MAX(UPDATED_AT_UTC) as max_updated_at FROM " ~ this) %}
    {% set max_updated_at = results.columns[0].values()[0] if results.columns[0].values() else none %}
{% endif %}

-- we retrieve seasons
with final_season as (
    SELECT
        season.SEASON_KEY,
        season.SEASON_ID,
        season.SEASON_SPORT,
        season.SEASON_COUNTRY,
        season.SEASON_NAME,
        season.SEASON_DIVISION,
        season.SEASON_TEAMCHOICE_DEADLINE
    FROM
        {{ref('curated_season')}} season
    {% if is_incremental() and max_updated_at is not none %}
    WHERE season.UPDATED_AT_UTC > '{{ max_updated_at }}'
    {% endif %}
)
SELECT
    final_season.SEASON_KEY,
    final_season.SEASON_ID,
    final_season.SEASON_SPORT,
    final_season.SEASON_COUNTRY,
    final_season.SEASON_NAME,
    final_season.SEASON_DIVISION,
    final_season.SEASON_TEAMCHOICE_DEADLINE,
    {{updated_at_fields()}}
FROM
    final_season
LEFT JOIN
    {{ ref('local_time')}} lt 
    ON lt.COUNTRY = final_season.SEASON_COUNTRY