/* 
    The purpose of this view is to display games on it.
    It will be used by the Python program to calculate the scope of games to update, and for output
    Unique key:
        COMPETITION_SOURCE, GAME_SOURCE_ID
*/
{{config(
    materialized="view"
)}}

SELECT
    season.SEASON_ID,
    competition.COMPETITION_ID,
    competition.COMPETITION_SOURCE,
    gameday.GAMEDAY_MESSAGE,
    gameday.GAMEDAY,
    gameday.BEGIN_DATE_UTC AS GAMEDAY_BEGIN_DATE_UTC,
    gameday.END_DATE_UTC AS GAMEDAY_END_DATE_UTC,
    game.GAME_SOURCE_ID,
    game.GAME_MESSAGE,
    game.GAME_MESSAGE_SHORT,
    game.DATE_GAME_UTC,
    game.DATE_GAME_LOCAL,
    game.TIME_GAME_LOCAL,
    team_home.TEAM_NAME AS TEAM_HOME_NAME,
    team_away.TEAM_NAME AS TEAM_AWAY_NAME,
    game.SCORE_HOME,
    game.SCORE_AWAY,
    game.RESULT
FROM
    {{ref('consumpted_game')}} game
JOIN
    {{ref('consumpted_season')}} season
    ON game.SEASON_KEY = season.SEASON_KEY
JOIN
    {{ref('consumpted_competition')}} competition
    ON game.COMPETITION_KEY = competition.COMPETITION_KEY
JOIN
    {{ref('consumpted_gameday')}} gameday
    ON game.GAMEDAY_KEY = gameday.GAMEDAY_KEY
JOIN
    {{ref('consumpted_team')}} team_home
    ON team_home.TEAM_KEY = game.TEAM_HOME_KEY
JOIN
    {{ref('consumpted_team')}} team_away
    ON team_away.TEAM_KEY = game.TEAM_AWAY_KEY
