/* 
    The purpose of this view is to display correction on predictions due to mispelling writing by user.
    It will be used by the Python program to retrieve in the output_calculation message the list of correction
*/
{{config(
    materialized="view"
)}}

SELECT
    season.SEASON_ID,
    gameday.GAMEDAY,
    user.USER_NAME,
    pg.PREDICT_ID
FROM
    {{ref('consumpted_predict_game')}} pg
JOIN
    {{ref('consumpted_user')}} user            
    ON pg.USER_KEY = user.USER_KEY
JOIN
    {{ref('consumpted_gameday')}} gameday
    ON gameday.GAMEDAY_KEY = pg.GAMEDAY_KEY
JOIN
    {{ref('consumpted_season')}} season
    ON season.SEASON_KEY = pg.SEASON_KEY 
WHERE    
    pg.IS_PREDICTION_IN_DEFECT = 1