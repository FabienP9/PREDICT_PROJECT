/*  The purpose of this view is to retrieve the calculation of the prediction championship games detailed per user
It is used by Python program for output_calculation message */

{{config(
    materialized="view"
)}}

with team as (
        SELECT
            gp.GAME_KEY,
            gp.GAMEDAY_KEY,
            gp.SEASON_KEY,
            team_home.TEAM_KEY,
            team_home.TEAM_NAME
        FROM
            {{ref('consumpted_game_predictchamp')}} gp
        JOIN
            {{ref('consumpted_team')}} team_home
            ON team_home.TEAM_KEY = gp.TEAM_HOME_KEY
    UNION ALL 
        SELECT
            gp.GAME_KEY,
            gp.GAMEDAY_KEY,
            gp.SEASON_KEY,
            team_away.TEAM_KEY,
            team_away.TEAM_NAME
        FROM
            {{ref('consumpted_game_predictchamp')}} gp
        JOIN
            {{ref('consumpted_team')}} team_away
            ON team_away.TEAM_KEY = gp.TEAM_AWAY_KEY    

)
SELECT
    team.GAME_KEY,
    team.TEAM_NAME,
    user.USER_NAME,
    ug.POINTS,
    ROW_NUMBER() OVER(
        PARTITION BY team.GAME_KEY, team.TEAM_NAME
        ORDER BY ug.POINTS DESC
    ) AS RANK_USER_TEAM,
    gameday.GAMEDAY,
    season.SEASON_ID
FROM
    team
JOIN
    {{ref('consumpted_user_gameday')}} ug
    ON ug.GAMEDAY_KEY = team.GAMEDAY_KEY
    AND ug.TEAM_CHOICE_KEY = team.TEAM_KEY
JOIN
    {{ref('consumpted_user')}} user
    ON user.USER_KEY = ug.USER_KEY
JOIN
    {{ref('consumpted_gameday')}} gameday
    ON team.GAMEDAY_KEY = gameday.GAMEDAY_KEY
JOIN
    {{ref('consumpted_season')}} season
    ON team.SEASON_KEY = season.SEASON_KEY