/*  The purpose of this table is to calculate the results of the prediction championship games
    Inputs:
        curated_game_predictchamp: the curated games for the prediction championship
    Joins:
        consumpted_gameday: we filter on the gameday with action
        consumpted_user_gameday: to get the team chosen by user
    Primary Key:
        GAME_KEY from curated
    Foreign Key:
        SEASON_KEY: from curated
        GAMEDAY_KEY: from curated
        COMPETITION_KEY: from curated
        GAME_KEY: from curated
        TEAM_HOME_KEY: from curated
        TEAM_AWAY_KEY: from curated
    Filter:
        Only games related to the gameday with action
    Materialization:
        incremental to avoid removing old calculations */
{{config(
    materialized="incremental",
    unique_key=['GAME_KEY']
)}}

-- we extract game of the gameday with action
with game as (
    SELECT
        game.SEASON_KEY,
        game.COMPETITION_KEY,
        game.GAMEDAY_KEY,
        game.GAME_KEY,
        game.GAME_MESSAGE_SHORT,
        game.GAME_MESSAGE,
        game.GAME_SOURCE_ID,
        game.GAME_PC_ID,
        game.TEAM_HOME_KEY,
        game.TEAM_AWAY_KEY,
        game.HAS_HOME_ADV,
        game.IS_FOR_RANK,
        gameday.IS_TO_DELETE
    FROM
        {{ref('curated_game_predictchamp')}}  game
    JOIN
        {{ref('consumpted_gameday')}} gameday
        ON game.GAMEDAY_KEY = gameday.GAMEDAY_KEY
        AND gameday.IS_TO_CALCULATE + gameday.IS_TO_DELETE > 0
),
--we extract sum of team points for gameday with action
team_points as (
    SELECT
        ug.TEAM_CHOICE_KEY AS TEAM_KEY,
        gameday.GAMEDAY_KEY,
        SUM(ug.POINTS) AS TEAM_POINTS 
    FROM
        {{ref('consumpted_user_gameday')}}  ug
    JOIN
        {{ref('consumpted_gameday')}} gameday
        ON ug.GAMEDAY_KEY = gameday.GAMEDAY_KEY
        AND gameday.IS_TO_CALCULATE + gameday.IS_TO_DELETE > 0
    GROUP BY
        ug.TEAM_CHOICE_KEY,
        gameday.GAMEDAY_KEY
),
final as (
    SELECT
        game.SEASON_KEY,
        game.COMPETITION_KEY,
        game.GAMEDAY_KEY,
        game.GAME_KEY,
        game.GAME_MESSAGE_SHORT,
        game.GAME_MESSAGE,
        game.GAME_SOURCE_ID,
        game.GAME_PC_ID,
        game.TEAM_HOME_KEY,
        game.TEAM_AWAY_KEY,
        game.HAS_HOME_ADV,
        game.IS_FOR_RANK,
        CASE
            WHEN game.IS_TO_DELETE = 1 THEN 0
            ELSE COALESCE(team_points_home.TEAM_POINTS,0)
        END AS POINTS_BASE,
        CAST(game.HAS_HOME_ADV * 0.2 * POINTS_BASE AS INT) AS POINTS_BONUS,
        POINTS_BASE + POINTS_BONUS AS POINTS_HOME,
        CASE
            WHEN game.IS_TO_DELETE = 1 THEN 0
            ELSE COALESCE(team_points_away.TEAM_POINTS,0)
        END AS POINTS_AWAY,
        CASE
            -- 0: no winner (deleted or not calculated) - 1: home team winner - 2: away team winner
            WHEN game.IS_TO_DELETE = 1 THEN 0
            WHEN POINTS_HOME >= POINTS_AWAY THEN 1
            ELSE 2
        END AS WINNER
    FROM
        game
    LEFT JOIN
        team_points team_points_home 
        ON team_points_home.TEAM_KEY = game.TEAM_HOME_KEY
        AND team_points_home.GAMEDAY_KEY = game.GAMEDAY_KEY
    LEFT JOIN
        team_points team_points_away 
        ON team_points_away.TEAM_KEY = game.TEAM_AWAY_KEY
        AND team_points_away.GAMEDAY_KEY = game.GAMEDAY_KEY
)
SELECT
    final.SEASON_KEY,
    final.COMPETITION_KEY,
    final.GAMEDAY_KEY,
    final.GAME_KEY,
    final.GAME_MESSAGE_SHORT,
    final.GAME_MESSAGE,
    final.GAME_SOURCE_ID,
    final.GAME_PC_ID,
    final.TEAM_HOME_KEY,
    final.TEAM_AWAY_KEY,
    final.HAS_HOME_ADV,
    final.IS_FOR_RANK,
    final.POINTS_BASE,
    final.POINTS_BONUS,
    final.POINTS_HOME,
    final.POINTS_AWAY,
    final.WINNER,
    {{updated_at_fields()}}
FROM
    final
    {{updated_at_table_join_season('final')}}