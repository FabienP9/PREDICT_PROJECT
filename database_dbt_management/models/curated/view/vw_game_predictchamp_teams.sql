/*  The purpose of this view is to display games of the predict championship with their teams, for checking */

{{config(
    materialized="view"
)}}

SELECT
    season.SEASON_ID,
    gameday.GAMEDAY,
    th.TEAM_NAME AS TEAM_HOME,
    ta.TEAM_NAME AS TEAM_AWAY
FROM {{ref('curated_game_predictchamp')}} gp
LEFT JOIN {{ref('curated_season')}} season ON season.SEASON_KEY = gp.SEASON_KEY
LEFT JOIN {{ref('curated_gameday')}} gameday ON gameday.GAMEDAY_KEY = gp.GAMEDAY_KEY
LEFT JOIN {{ref('curated_team')}} th ON th.TEAM_KEY = gp.TEAM_HOME_KEY
LEFT JOIN {{ref('curated_team')}} ta ON ta.TEAM_KEY = gp.TEAM_AWAY_KEY
ORDER BY
    season.SEASON_ID,
    gameday.GAMEDAY