/* 
    The purpose of this view is to display user with their scores  per gameday
    It will be used by the Python program to display the score of users for a specific gameday and their rankings
*/
{{config(
    materialized="view"
)}}

SELECT
    user.USER_NAME,
    gameday.GAMEDAY,
    ug.POINTS AS GAMEDAY_POINTS,
    ug.NB_PREDICTION AS NB_PREDICTION_GAMEDAY,
    TO_VARCHAR(gameday.END_DATE_LOCAL, 'YYYY-MM') AS END_YEARMONTH_LOCAL,
    team.TEAM_NAME,
    CASE
        WHEN gp_home.WINNER = 1 THEN 1
        WHEN gp_away.WINNER = 2 THEN 1
        ELSE 0
    END AS WIN,
    CASE
        WHEN gp_home.WINNER = 2 THEN 1
        WHEN gp_away.WINNER = 1 THEN 1
        ELSE 0
    END AS LOSS,
    compet.COMPETITION_LABEL,
    season.SEASON_ID
FROM
     {{ref('consumpted_user')}} user
JOIN
    {{ref('consumpted_user_gameday')}} ug
    ON ug.USER_KEY = user.USER_KEY
JOIN
    {{ref('consumpted_gameday')}} gameday
    ON gameday.GAMEDAY_KEY = ug.GAMEDAY_KEY
JOIN
    {{ref('consumpted_season')}} season
    ON user.SEASON_KEY = season.SEASON_KEY
JOIN
    {{ref('consumpted_competition')}} compet
    ON compet.COMPETITION_KEY = gameday.COMPETITION_KEY
LEFT JOIN
    {{ref('consumpted_team')}} team
    ON team.TEAM_KEY = ug.TEAM_CHOICE_KEY
LEFT JOIN
    {{ref('consumpted_game_predictchamp')}} gp_home
    ON gp_home.GAMEDAY_KEY = gameday.GAMEDAY_KEY
    AND team.TEAM_KEY = gp_home.TEAM_HOME_KEY
LEFT JOIN
    {{ref('consumpted_game_predictchamp')}} gp_away
    ON gp_away.GAMEDAY_KEY = gameday.GAMEDAY_KEY
    AND team.TEAM_KEY = gp_away.TEAM_AWAY_KEY