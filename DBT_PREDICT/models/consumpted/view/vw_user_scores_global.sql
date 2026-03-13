/* 
    The purpose of this view is to display user with their total scores  per season
    It will be used by the Python program to display the score of users and their rankings
*/
{{config(
    materialized="view"
)}}

SELECT
    user.USER_NAME,
    user.TOTAL_POINTS,
    user.NB_GAMEDAY_PREDICT,
    user.NB_GAMEDAY_FIRST,
    user.NB_PREDICTION AS NB_TOTAL_PREDICT,
    season.SEASON_ID
FROM
    {{ref('consumpted_user')}} user
JOIN
    {{ref('consumpted_season')}} season
    ON user.SEASON_KEY = season.SEASON_KEY