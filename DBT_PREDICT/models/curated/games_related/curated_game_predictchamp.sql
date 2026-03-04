/* 
    The purpose of this table is to get all games:
    - from curated_game for competition which are the same for prediction championship
    - from landing_predictchamp_game_to_add for other competition
    Inputs:
        curated_game
        landing_predictchamp_game_to_add
    Joins:
        landing_gameday_modification: to know which gameday need to be renamed or grouped
        curated_competition: foreign key
        curated_gameday: foreign key
        curated_team: foreign key
    Primary Key:
        GAME_KEY: 
        - same than curated_game for game coming from there
        - or on PREDICTCHAMP_GAME_ID when coming from landing_predictchamp_game_to_add
    Secondary key:
        SEASON_KEY: 1-n relationship with season
        COMPETITION_KEY: 1-n relationship with competition
        GAMEDAY_KEY: 1-n relationship with gameday
        TEAM_HOME_KEY / TEAM_AWAY_KEY: 1-n relationship with team
    Filter:
        Inputs are filtered with game to process
        curated_competition: filter on IS_SAME_FOR_PREDICTCHAMP
    Materialization:
        incremental to avoid removing old games already in
*/
{{config(
    tags=['init_compet'],
    materialized = "incremental",
    unique_key = 'GAME_KEY'
)}}

-- we get input data
-- join game with competition to get IS_SAME_FOR_PREDICTCHAMP: 
-- if 1, we get games of the compet, if 0 we don't
with game as (
    SELECT
        game.SEASON_KEY,
        game.COMPETITION_KEY,
        game.GAMEDAY_KEY,
        game.GAME_KEY,
        game.GAME_MESSAGE_SHORT,
        game.GAME_MESSAGE,
        game.GAME_SOURCE_ID,
        NULL AS GAME_PC_ID,
        game.TEAM_HOME_KEY,
        game.TEAM_AWAY_KEY,
        1 AS HAS_HOME_ADV,
        compet.IS_FOR_RANK
    FROM
        {{ref('curated_game')}} game
    JOIN
        {{ref('curated_competition')}} compet
        ON compet.COMPETITION_KEY = game.COMPETITION_KEY
        AND compet.IS_SAME_FOR_PREDICTCHAMP = 1 
),
-- we get game to add from landing_predictchamp_game_to_add when it's not same, with their possible modif from landing_gameday_modification
pc_game as (
    SELECT
        gp.SEASON_ID,
        COALESCE(modif.GAMEDAY_MODIFIED,gp.GAMEDAY) AS GAMEDAY,
        gp.TEAM_HOME,
        gp.TEAM_AWAY,
        ROW_NUMBER() OVER (
            PARTITION BY COALESCE(modif.GAMEDAY_MODIFIED,gp.GAMEDAY) ORDER BY gp.PREDICTCHAMP_GAME_ID ASC) 
        AS GAME_MESSAGE_SHORT,
        MD5('PREDICTCHAMP_GAME' || '^^' || gp.PREDICTCHAMP_GAME_ID) AS GAME_KEY,
        gp.PREDICTCHAMP_GAME_ID AS GAME_PC_ID,
        gp.HAS_HOME_ADV
    FROM
        {{source('LAND','GAME_PREDICTCHAMP')}} gp
    LEFT JOIN
        {{source('LAND','GAMEDAY_MODIF')}} modif
        ON gp.SEASON_ID = modif.SEASON_ID
        AND LOWER(REPLACE(gp.GAMEDAY, ' ', '')) = LOWER(REPLACE(modif.GAMEDAY, ' ', ''))
),
-- we add keys to calculated games
-- for that,we apply the JAROWINKLER function to get the closest team in the list of possible home and away teams
pc_game_with_key as (
    SELECT
        season.SEASON_KEY,
        gameday.COMPETITION_KEY,
        gameday.GAMEDAY_KEY,
        pcg.GAME_KEY,
        pcg.GAME_MESSAGE_SHORT,
        gameday.GAMEDAY_MESSAGE || '.' || LPAD(pcg.GAME_MESSAGE_SHORT, 2, '0') AS GAME_MESSAGE,
        NULL AS GAME_SOURCE_ID,
        pcg.GAME_PC_ID,
        teamhome.TEAM_KEY AS TEAM_HOME_KEY,
        teamaway.TEAM_KEY AS TEAM_AWAY_KEY,
        pcg.HAS_HOME_ADV,
        compet.IS_FOR_RANK,
        JAROWINKLER_SIMILARITY(lower(trim(pcg.TEAM_HOME)), lower(trim(teamhome.TEAM_NAME))) AS TEAM_HOME_SCORE_JARO,
        JAROWINKLER_SIMILARITY(lower(trim(pcg.TEAM_AWAY)), lower(trim(teamaway.TEAM_NAME))) AS TEAM_AWAY_SCORE_JARO,
    FROM
        pc_game pcg
    LEFT JOIN
        {{ref('curated_season')}} season
        ON pcg.SEASON_ID = season.SEASON_ID
    LEFT JOIN
        {{ref('curated_gameday')}} gameday
        ON season.SEASON_KEY = gameday.SEASON_KEY
        AND pcg.GAMEDAY = gameday.GAMEDAY    
    LEFT JOIN
        {{ref("curated_team")}} teamhome
        ON teamhome.SEASON_KEY = season.SEASON_KEY
    LEFT JOIN
        {{ref("curated_team")}} teamaway
        ON teamaway.SEASON_KEY = season.SEASON_KEY
    LEFT JOIN
        {{ref('curated_competition')}} compet
        ON compet.COMPETITION_KEY = gameday.COMPETITION_KEY
    QUALIFY    
        -- we get the closest home and away teams
        RANK() OVER (
                PARTITION BY pcg.GAME_PC_ID
                ORDER BY TEAM_HOME_SCORE_JARO DESC, teamhome.TEAM_KEY
            )
        = 1
        AND
        RANK() OVER (
                PARTITION BY pcg.GAME_PC_ID
                ORDER BY TEAM_AWAY_SCORE_JARO DESC, teamaway.TEAM_KEY
            )
        = 1
),
-- we union both scope of games
final_game as (
    SELECT
        SEASON_KEY,
        COMPETITION_KEY,
        GAMEDAY_KEY,
        GAME_KEY,
        GAME_MESSAGE_SHORT,
        GAME_MESSAGE,
        GAME_SOURCE_ID,
        GAME_PC_ID,
        TEAM_HOME_KEY,
        TEAM_AWAY_KEY,
        HAS_HOME_ADV,
        IS_FOR_RANK
    FROM
        game
    UNION ALL
    SELECT
        SEASON_KEY,
        COMPETITION_KEY,
        GAMEDAY_KEY,
        GAME_KEY,
        GAME_MESSAGE_SHORT,
        GAME_MESSAGE,
        GAME_SOURCE_ID,
        GAME_PC_ID,
        TEAM_HOME_KEY,
        TEAM_AWAY_KEY,
        HAS_HOME_ADV,
        IS_FOR_RANK
    FROM
        pc_game_with_key
)
SELECT
    final_game.GAME_KEY,
    final_game.SEASON_KEY,
    final_game.COMPETITION_KEY,
    final_game.GAMEDAY_KEY,
    final_game.GAME_MESSAGE_SHORT,
    final_game.GAME_MESSAGE,
    final_game.GAME_SOURCE_ID,
    final_game.GAME_PC_ID,
    final_game.TEAM_HOME_KEY,
    final_game.TEAM_AWAY_KEY,
    final_game.HAS_HOME_ADV,
    final_game.IS_FOR_RANK,
    {{updated_at_fields()}}
FROM
    final_game
    {{updated_at_table_join_season('final_game')}}