/* 
    The purpose of this table is to get all games we focus on and change their gameday if needed
    Inputs:
        landing_game: the extracted game
    Joins:
        landing_gameday_modification: to know which gameday need to be renamed or grouped
        curated_season: foreign_key
        curated_competition: foreign_key
        curated_team: foreign_key
    Filter:
        landing_games already filtered on games which should be processed
    Materialization:
        ephemeral - it will be used by curated_game and curated_gameday
*/
{{config(
    materialized='ephemeral'
)}}
-- we get games extracted to calculate different gamedays, with their possible modif from landing_gameday_modification
with adjusted_game as (
    SELECT
        game.COMPETITION_SOURCE,
        game.COMPETITION_ID,
        game.SEASON_ID,
        game.GAME_SOURCE_ID,
        COALESCE(modif.GAMEDAY_MODIFIED,game.GAMEDAY) AS GAMEDAY,
        game.TEAM_HOME,
        game.TEAM_AWAY,
        game.SCORE_HOME,
        game.SCORE_AWAY,
        game.DATE_GAME_UTC,
        game.TIME_GAME_UTC,
        game.DATE_GAME_LOCAL,
        game.TIME_GAME_LOCAL
    FROM
        {{source("LAND",'GAME')}} game
    LEFT JOIN
        {{source("LAND",'GAMEDAY_MODIF')}} modif
        ON game.SEASON_ID = modif.SEASON_ID
        AND game.COMPETITION_ID = modif.COMPETITION_ID
        AND LOWER(REPLACE(game.GAMEDAY, ' ', '')) = LOWER(REPLACE(modif.GAMEDAY, ' ', '')) 
),
-- we add keys
game_with_key as (
    SELECT
        season.SEASON_KEY,
        season.SEASON_COUNTRY,
        compet.COMPETITION_KEY,
        teamhome.TEAM_KEY as TEAM_HOME_KEY,
        teamaway.TEAM_KEY as TEAM_AWAY_KEY,
        MD5('GAMEDAY' || '^^' || game_adj.SEASON_ID || '^^' || game_adj.GAMEDAY) AS GAMEDAY_KEY,
        MD5('GAME' || '^^' || game_adj.COMPETITION_SOURCE || '^^' || game_adj.GAME_SOURCE_ID) AS GAME_KEY,
        game_adj.GAME_SOURCE_ID,
        game_adj.GAMEDAY,
        game_adj.SCORE_HOME,
        game_adj.SCORE_AWAY,
        game_adj.DATE_GAME_UTC,
        game_adj.TIME_GAME_UTC,
        game_adj.DATE_GAME_LOCAL,
        game_adj.TIME_GAME_LOCAL
    FROM
        adjusted_game game_adj
    LEFT JOIN
        {{ref('curated_season')}} season
        ON game_adj.SEASON_ID = season.SEASON_ID
    LEFT JOIN
        {{ref('curated_competition')}} compet 
        ON season.SEASON_KEY = compet.SEASON_KEY
        AND game_adj.COMPETITION_ID = compet.COMPETITION_ID
    LEFT JOIN
        {{ref('curated_team')}} teamhome
        ON season.SEASON_KEY = teamhome.SEASON_KEY
        AND game_adj.TEAM_HOME = teamhome.TEAM_NAME
    LEFT JOIN
        {{ref('curated_team')}} teamaway
        ON season.SEASON_KEY = teamaway.SEASON_KEY
        AND game_adj.TEAM_AWAY = teamaway.TEAM_NAME
),
-- We get a short version of GAMEDAY for message formatting: 
-- ex: "28ème journée" becomes "28ÈJ" / "Quart de finale Match 1" becomes "QDFM1"
gameday_message as (
    WITH RECURSIVE char_cte (input_str, result_str, prev_char, pos) AS (
        -- Initial step: Start with the first character
        SELECT input_str,
               SUBSTR(input_str, 1, 1) AS result_str,
               SUBSTR(input_str, 1, 1) AS prev_char,
               2 AS pos
        FROM (SELECT DISTINCT GAMEDAY AS input_str FROM game_with_key)    
        UNION ALL
        -- Recursive step: Process one character at a time
        SELECT input_str,
               CASE 
                   WHEN REGEXP_LIKE(SUBSTR(input_str, pos, 1), '[0-9]') 
                        OR prev_char IN (' ', '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9') 
                   THEN result_str || SUBSTR(input_str, pos, 1)
                   ELSE result_str
               END,
               SUBSTR(input_str, pos, 1),
               pos + 1
        FROM char_cte
        WHERE pos <= LENGTH(input_str)
    ) 
    SELECT input_str AS GAMEDAY, UPPER(REPLACE(result_str,' ','')) AS GAMEDAY_MESSAGE 
    FROM char_cte
    WHERE pos = LENGTH(input_str) + 1
),
-- we add the result as gameday_message
game_with_message as (
    SELECT    
        game_key.SEASON_KEY,
        game_key.SEASON_COUNTRY,
        game_key.COMPETITION_KEY,
        game_key.TEAM_HOME_KEY,
        game_key.TEAM_AWAY_KEY,
        game_key.GAMEDAY_KEY,
        game_key.GAME_KEY,
        game_key.GAME_SOURCE_ID,
        game_key.GAMEDAY,
        gameday.GAMEDAY_MESSAGE,
        game_key.SCORE_HOME,
        game_key.SCORE_AWAY,
        game_key.DATE_GAME_UTC,
        game_key.TIME_GAME_UTC,
        game_key.DATE_GAME_LOCAL,
        game_key.TIME_GAME_LOCAL
    FROM 
        game_with_key game_key
    JOIN
        gameday_message gameday
        ON game_key.GAMEDAY = gameday.GAMEDAY   
    LEFT JOIN
        {{ ref('local_time')}} lt 
        ON game_key.SEASON_COUNTRY = lt.COUNTRY
)
SELECT * FROM game_with_message