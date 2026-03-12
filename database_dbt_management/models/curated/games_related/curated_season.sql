/* 
    The purpose of this table is to get all seasons we focus on
    Inputs:
        landing_season: we get all distinct seasons we are focusing on during the run
    Primary Key:
        SEASON_KEY based on SEASON_ID
    Filter:
        landing_season is already filtered on seasons which should be processed
    Materialization:
        incremental to avoid removing old seasons
*/
{{config(
    tags=['init_compet'],
    materialized="incremental",
    unique_key = ['SEASON_KEY']
)}}

-- we get seasons from landing_season
with season_with_key as (
    SELECT DISTINCT
        MD5('SEASON' || '^^' || season.SEASON_ID) AS SEASON_KEY,
        season.SEASON_ID,
        season.SEASON_SPORT,
        season.SEASON_COUNTRY,
        season.SEASON_NAME,
        season.SEASON_DIVISION,
        season.SEASON_TEAMCHOICE_DEADLINE
    FROM
        {{source("LAND",'SEASON')}} season
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
    season_with_key final_season
LEFT JOIN
    {{ ref('local_time')}} lt 
    ON lt.COUNTRY = final_season.SEASON_COUNTRY