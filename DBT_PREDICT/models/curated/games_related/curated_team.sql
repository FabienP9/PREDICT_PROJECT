/* 
    The purpose of this table is to extract all teams participating in seasons we focus on.
    Inputs:
        landing_game: we retrieve teams from all games we focus on during the run
    Joins:
        curated_season: foreign key
    Primary Key:
        TEAM_KEY based on SEASON_ID and TEAM_NAME
    Foreign key:
        SEASON_KEY: 1-n relationship with season. A team corresponds to a season
    Filter:
        landing_game is already filtered on teams which need to be processed
    Materialization:
        incremental to avoid removing old teams already in
*/

{{config(
    tags=['init_compet'],
    materialized="incremental",
    unique_key = ['TEAM_KEY']
)}}
--we get teams from landing_game - either home or away
with team_home as (
    SELECT DISTINCT
        game.TEAM_HOME AS TEAM_NAME,
        game.SEASON_ID
    FROM {{source("LAND","GAME")}} game
),
team_away as (
    SELECT DISTINCT
        game.TEAM_AWAY AS TEAM_NAME,
        game.SEASON_ID
    FROM {{source("LAND","GAME")}} game
),
-- we union both - with deduplication
team as (
    SELECT
        team_home.TEAM_NAME,
        team_home.SEASON_ID
    FROM
        team_home
    UNION
    SELECT
        team_away.TEAM_NAME,
        team_away.SEASON_ID
    FROM
        team_away    
),
-- we calculate keys
team_with_key as (
    SELECT
        MD5('TEAM' || '^^' || team.SEASON_ID || '^^' || team.TEAM_NAME) AS TEAM_KEY,
        season.SEASON_KEY,
        team.TEAM_NAME
    FROM
        team
    LEFT JOIN
        {{ref('curated_season')}} season
        ON team.SEASON_ID = season.SEASON_ID
)
SELECT
    final_team.TEAM_KEY,
    final_team.SEASON_KEY,
    final_team.TEAM_NAME,
    {{updated_at_fields()}}
FROM 
    team_with_key final_team
    {{updated_at_table_join_season('final_team')}}