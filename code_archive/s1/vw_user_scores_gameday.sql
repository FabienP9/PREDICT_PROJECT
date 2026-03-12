/* 
    The purpose of this view is to display user with their scores  per gameday
    It will be used by the Python program to display the score of users for a specific gameday and their rankings
*/
{{config(
    materialized="view"
)}}

SELECT
    user.USER_NAME,
    ug.TOTAL_SCORE AS GAMEDAY_SCORE,  
    ug.POINTS AS GAMEDAY_POINTS,
    gameday.GAMEDAY,
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