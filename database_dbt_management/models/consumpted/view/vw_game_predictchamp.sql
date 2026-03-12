/*  The purpose of this view is to display the results of the prediction championship games.
It is used by Python program for output_calculation message */

{{config(
    materialized="view"
)}}

SELECT
    gp.GAME_KEY,
    gp.GAME_MESSAGE_SHORT,
    team_home.TEAM_NAME AS TEAM_HOME_NAME,
    team_away.TEAM_NAME AS TEAM_AWAY_NAME,
    gp.IS_FOR_RANK,
    gp.HAS_HOME_ADV,
    gp.POINTS_BONUS,
    gp.POINTS_HOME,
    gp.POINTS_AWAY,
    gp.WINNER,
    gameday.GAMEDAY,
    season.SEASON_ID
FROM
    {{ref('consumpted_game_predictchamp')}} gp
JOIN
    {{ref('consumpted_team')}} team_home
    ON team_home.TEAM_KEY = gp.TEAM_HOME_KEY
JOIN
    {{ref('consumpted_team')}} team_away
    ON team_away.TEAM_KEY = gp.TEAM_AWAY_KEY
JOIN
    {{ref('consumpted_gameday')}} gameday
    ON gp.GAMEDAY_KEY = gameday.GAMEDAY_KEY
JOIN
    {{ref('consumpted_season')}} season
    ON gp.SEASON_KEY = season.SEASON_KEY