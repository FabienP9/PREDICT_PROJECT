/* 
    The purpose of this table is to get all games we need to process
    Inputs:
        adjusted_game (ephemeral): the extracted game via landing_game we want to process
    Joins:
        curated_competition: foreign key
        curated_gameday: foreign key
        curated_team: foreign key
        landing_game_modification: to be able to change the game number if required by the manual table, for message id
    Primary Key:
        GAME_KEY based on COMPETITION_SOURCE and GAME_SOURCE_ID
    Secondary key:
        SEASON_KEY: 1-n relationship with season
        COMPETITION_KEY: 1-n relationship with competition
        GAMEDAY_KEY: 1-n relationship with gameday
        TEAM_HOME_KEY / TEAM_AWAY_KEY: 1-n relationship with team
    Filter:
        adjusted_game is already filtered with games to process
    Materialization:
        incremental to avoid removing old games already in
*/
{{config(
    tags=['init_compet'],
    materialized = "incremental",
    unique_key = 'GAME_KEY',
)}}
-- we get games extracted
with adjusted_game as (
    SELECT
        game.SEASON_KEY,
        game.COMPETITION_KEY,
        game.TEAM_HOME_KEY,
        game.TEAM_AWAY_KEY,
        game.GAMEDAY_KEY,
        game.GAME_KEY,
        game.GAME_SOURCE_ID,
        game.GAMEDAY_MESSAGE,
        game.SCORE_HOME,
        game.SCORE_AWAY,
        game.SCORE_HOME - game.SCORE_AWAY AS RESULT,
        CASE 
            WHEN game.SCORE_HOME + game.SCORE_AWAY = 0
            THEN 0
            ELSE 1
        END AS IS_PLAYED,
        game.DATE_GAME_LOCAL,
        game.TIME_GAME_LOCAL,
        game.DATE_GAME_UTC,
        game.TIME_GAME_UTC
    FROM
        {{ref('adjusted_game')}} game
),
--We calculate the game number, which will be the game id for message: 
--either we take it from landing_game_modification if exists, or on the sorted game_source_id
ranked_games AS (
    SELECT
        game_adj.GAME_KEY,
        COALESCE(modif.GAME_FORUM_ID,ROW_NUMBER() OVER (
            PARTITION BY game_adj.GAMEDAY_KEY 
            ORDER BY game_adj.GAME_SOURCE_ID ASC)) AS GAME_MESSAGE_SHORT
    FROM 
        adjusted_game game_adj
    LEFT JOIN
        {{ref('curated_season')}} season
        ON game_adj.SEASON_KEY = season.SEASON_KEY
    LEFT JOIN 
        {{source("LAND",'GAME_MODIF')}} modif 
        ON modif.GAME_SOURCE_ID = game_adj.GAME_SOURCE_ID
        AND modif.SEASON_ID = season.SEASON_ID
    
),
--We retrieve games: if the game is played already there is no time, we get it from the current version (this)
final_game as (
    SELECT
        adj_game.SEASON_KEY,
        adj_game.COMPETITION_KEY,
        adj_game.GAMEDAY_KEY,
        adj_game.GAME_KEY,
        adj_game.GAME_SOURCE_ID,
        ranked_games.GAME_MESSAGE_SHORT,
        -- the unique id of a game per season, for message identification
        adj_game.GAMEDAY_MESSAGE || '.' || LPAD(ranked_games.GAME_MESSAGE_SHORT, 2, '0') AS GAME_MESSAGE,
        adj_game.DATE_GAME_LOCAL,
        adj_game.DATE_GAME_UTC,
        adj_game.TIME_GAME_LOCAL,
        adj_game.TIME_GAME_UTC,
        adj_game.TEAM_HOME_KEY,
        adj_game.TEAM_AWAY_KEY,
        adj_game.SCORE_HOME,
        adj_game.SCORE_AWAY,
        adj_game.RESULT,
        adj_game.IS_PLAYED
    FROM
        adjusted_game adj_game
    LEFT JOIN
        ranked_games 
        ON ranked_games.GAME_KEY = adj_game.GAME_KEY
)
SELECT
    final_game.GAME_KEY,
    final_game.SEASON_KEY,
    final_game.COMPETITION_KEY,
    final_game.GAMEDAY_KEY,
    final_game.TEAM_HOME_KEY,
    final_game.TEAM_AWAY_KEY,
    final_game.GAME_SOURCE_ID,
    final_game.GAME_MESSAGE_SHORT,
    final_game.GAME_MESSAGE,
    final_game.DATE_GAME_LOCAL,
    final_game.TIME_GAME_LOCAL,
    final_game.DATE_GAME_UTC,
    final_game.TIME_GAME_UTC,
    final_game.SCORE_HOME,
    final_game.SCORE_AWAY,
    final_game.RESULT,
    final_game.IS_PLAYED,
    {{updated_at_fields()}}
FROM
    final_game
    {{updated_at_table_join_season('final_game')}}