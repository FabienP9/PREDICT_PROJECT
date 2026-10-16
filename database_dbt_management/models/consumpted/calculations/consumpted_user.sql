/*  The purpose of this table is calculate score, features and points for users
    Only for the season of gameday with action
    Inputs:
        curated_user: the curated user
    Joins:
        consumpted_season: foreign key
        consumpted_gameday: to get gameday with action
        consumpted_user_gameday: foreign key
    Primary Key:
        USER_KEY from curated
    Foreign Key:
        SEASON_KEY: from curated
    Filter:
        Only user related with season on which the gameday is the action (IS_TO_DELETE or IS_TO_CALCULATE)
    Materialization:
        incremental to avoid removing old calculations */

{{config(
    materialized="incremental",
    unique_key=['USER_KEY']
)}}
-- we get only user with season for which gameday is on action
with user as (
    SELECT
        user.USER_KEY,
        user.USER_NAME,
        user.SEASON_KEY,
        gameday.GAMEDAY_KEY
    FROM
        {{ref('curated_user')}} user
    JOIN
        {{ref('consumpted_season')}} season
        ON user.SEASON_KEY = season.SEASON_KEY
    JOIN
        {{ref('consumpted_gameday')}} gameday
        ON season.SEASON_KEY = gameday.SEASON_KEY
        AND gameday.IS_TO_CALCULATE + gameday.IS_TO_DELETE > 0
),
-- we merge with consumpted_user_gameday to get features
user_gameday as (
    SELECT
       user.USER_KEY,
       user.USER_NAME,
       user.SEASON_KEY,
       ug.GAMEDAY_KEY,
       ug.POINTS,
       ug.NB_PREDICTION,
       -- we calculate the rank of each user to calculate the number of time a user ranked first (next run)
       RANK() OVER (
            PARTITION BY
                ug.GAMEDAY_KEY
            ORDER BY
                ug.POINTS DESC
       ) AS RANK_USER_GAMEDAY
    FROM
        user
    JOIN
        {{ref('consumpted_user_gameday')}} ug
    ON
        user.SEASON_KEY = ug.SEASON_KEY
        and user.USER_KEY = ug.USER_KEY
),
-- we calculate the total points, the number of predicted gamedays and predictions, 
-- and get the number of time the user came first
figures as (
    SELECT
        ug2.USER_KEY,
        ug2.USER_NAME,
        ug2.SEASON_KEY,
        SUM(ug2.POINTS) AS TOTAL_POINTS,
        SUM(ug2.NB_PREDICTION) AS NB_PREDICTION,
        COUNT(DISTINCT ug2.GAMEDAY_KEY) AS NB_GAMEDAY_PREDICT,
        SUM(CASE 
                WHEN ug2.RANK_USER_GAMEDAY = 1 THEN 1
                ELSE 0 END)
        AS NB_GAMEDAY_FIRST
    FROM user_gameday ug2
    GROUP BY
        ug2.USER_KEY,
        ug2.USER_NAME,
        ug2.SEASON_KEY
),
-- we get the number of predictions total in the season
nb_predictions_total as (
    SELECT
        gameday.SEASON_KEY,
        SUM(gameday.NB_PREDICTION) AS NB_PREDICTION_TOTAL
    FROM
        {{ref('consumpted_gameday')}} gameday
    WHERE
        gameday.IS_CALCULATED = 1
    GROUP BY
        gameday.SEASON_KEY
),
final_user as (
    SELECT
        figures.USER_KEY,
        figures.USER_NAME,
        figures.SEASON_KEY,
        figures.TOTAL_POINTS,
        figures.NB_PREDICTION,
        figures.NB_GAMEDAY_PREDICT,
        figures.NB_GAMEDAY_FIRST,
        CASE 
            WHEN figures.NB_PREDICTION/nb_predictions_total.NB_PREDICTION_TOTAL > 0.75 THEN 1
            ELSE 0
        END AS IS_ELIGIBLE_AVERAGE_RANKING,
        CASE 
            WHEN figures.NB_PREDICTION/nb_predictions_total.NB_PREDICTION_TOTAL > 0.15 THEN 1
            ELSE 0
        END AS IS_ELIGIBLE_PREDICTCHAMP        
    FROM
        figures
    JOIN
        nb_predictions_total
        ON nb_predictions_total.SEASON_KEY = figures.SEASON_KEY
)
SELECT
    final_user.USER_KEY,
    final_user.USER_NAME,
    final_user.SEASON_KEY,
    final_user.TOTAL_POINTS,
    final_user.NB_PREDICTION,
    final_user.NB_GAMEDAY_PREDICT,
    final_user.NB_GAMEDAY_FIRST,
    final_user.IS_ELIGIBLE_AVERAGE_RANKING,
    final_user.IS_ELIGIBLE_PREDICTCHAMP,
    {{updated_at_fields()}}
FROM 
    final_user
    {{updated_at_table_join_season('final_user')}}