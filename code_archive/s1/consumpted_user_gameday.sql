/*  The purpose of this table is to match user per gamedays who made predictions:
    - calculate the total score and points in the gameday
    - get the chosen team for the prediction championship
    Only for gamedays with action
    Inputs:
        curated_predict_game: the curated prediction of games
    Joins:
        curated_user: foreign key
        curated_season: foreign key
        curated_gameday: foreign key
        curated_choice_team: foreign key
    Primary Key:
        USER_GAMEDAY_KEY based on season id, user_name and gameday
    Foreign Key:
        MESSAGE_KEY: from curated
        TOPIC_KEY: from curated
        USER_KEY: from curated
        SEASON_KEY: from curated
        GAME_KEY: from curated
    Filter:
        Only gameday with action
    Materialization:
        incremental to avoid removing old calculations */

{{config(
    materialized="incremental",
    unique_key=['USER_GAMEDAY_KEY']
)}}
-- we merge user with prediction and gamedays with action
with user_game as (
    SELECT 
        MD5('USER_GAMEDAY' || '^^' || user.USER_NAME || '^^' || season.SEASON_ID || 
             '^^' || gameday.GAMEDAY) AS USER_GAMEDAY_KEY,
        predict.USER_KEY,
        gameday.GAMEDAY_KEY,
        gameday.SEASON_KEY,
        predict.SCORE AS PREDICTION_SCORE,
        gameday.NB_GAME,
        predict.IS_PREDICTION_IN_DEFECT,
        gameday.END_DATE_UTC AS GAMEDAY_END_DATE_UTC,
        gameday.END_TIME_UTC AS GAMEDAY_END_TIME_UTC
    FROM
        {{ref('consumpted_predict_game')}} predict
    JOIN
        {{ref('curated_user')}} user
        ON predict.USER_KEY = user.USER_KEY
    JOIN
        {{ref('consumpted_gameday')}} gameday
        ON predict.GAMEDAY_KEY = gameday.GAMEDAY_KEY
        AND gameday.IS_TO_CALCULATE + gameday.IS_TO_DELETE > 0
    JOIN
        {{ref('consumpted_season')}} season
        ON gameday.SEASON_KEY = season.SEASON_KEY
),
-- we group to have only one row per user and gameday
grouped as (
    SELECT
        ug.USER_GAMEDAY_KEY,
        ug.USER_KEY,
        ug.GAMEDAY_KEY,
        ug.SEASON_KEY,
        ug.NB_GAME,
        SUM(ug.PREDICTION_SCORE) AS CALCULATED_SCORE,
        -- we get the origin of prediction: if at least one prediction of the GAMEDAY comes in defect
        -- then they all are in defect in order regarding to the calculatation of AUTOMATIC_SCORE (next cte)
        MAX(IS_PREDICTION_IN_DEFECT) AS ARE_PREDICTIONS_IN_DEFECT,
        ug.GAMEDAY_END_DATE_UTC,
        ug.GAMEDAY_END_TIME_UTC
    FROM 
        user_game ug
    GROUP BY
        ug.USER_GAMEDAY_KEY,
        ug.USER_KEY,
        ug.GAMEDAY_KEY,
        ug.SEASON_KEY,
        ug.NB_GAME,
        ug.GAMEDAY_END_DATE_UTC,
        ug.GAMEDAY_END_TIME_UTC
),
-- we calculate total score
score as (
    SELECT
        grouped.USER_GAMEDAY_KEY,
        grouped.USER_KEY,
        grouped.GAMEDAY_KEY,
        grouped.SEASON_KEY,
        grouped.NB_GAME,
        grouped.CALCULATED_SCORE,
        grouped.ARE_PREDICTIONS_IN_DEFECT,
        CASE
            WHEN grouped.ARE_PREDICTIONS_IN_DEFECT = 0 THEN 2
            ELSE 0
        END AS AUTOMATIC_SCORE,
        GREATEST(0,2*(10-grouped.NB_GAME)) AS PARTICIPATION_SCORE,
        AUTOMATIC_SCORE + PARTICIPATION_SCORE + CALCULATED_SCORE AS TOTAL_SCORE,
        -- triangular number formula based on TOTAL_SCORE
        CAST((TOTAL_SCORE+1)*(TOTAL_SCORE)/2 AS INT) AS POINTS,
        grouped.GAMEDAY_END_DATE_UTC,
        grouped.GAMEDAY_END_TIME_UTC
    FROM
        grouped
),
-- we add the chosen team (if any) for the user for this gameday (he has to choose it before the end of the gameday)
score_with_chosenteam as (
    SELECT
        score.USER_GAMEDAY_KEY,
        score.USER_KEY,
        score.GAMEDAY_KEY,
        score.SEASON_KEY,
        score.NB_GAME,
        score.ARE_PREDICTIONS_IN_DEFECT,
        score.CALCULATED_SCORE,
        score.AUTOMATIC_SCORE,
        score.PARTICIPATION_SCORE,
        score.TOTAL_SCORE,
        score.POINTS,
        teamc.TEAM_KEY AS TEAM_CHOICE_KEY
    FROM
        score
    LEFT JOIN
        {{ref('curated_choice_team')}} teamc
        ON score.SEASON_KEY = teamc.SEASON_KEY
        AND score.USER_KEY = teamc.USER_KEY
        AND teamc.IS_ONTIME = 1
        AND teamc.EDITION_TIME_UTC <= TO_TIMESTAMP(CONCAT(TO_CHAR(score.GAMEDAY_END_DATE_UTC, 'YYYY-MM-DD'), ' ', TO_CHAR(score.GAMEDAY_END_TIME_UTC, 'HH24:MI:SS'))) 
        -- we calculate the rank by descending time to select the last one chronologically
        -- if no match with a team is found, it will still have a rank = 1 with null team
        QUALIFY 
            ROW_NUMBER() OVER (
                PARTITION BY
                    score.SEASON_KEY,
                    score.USER_KEY,
                    score.GAMEDAY_KEY
                ORDER BY
                    teamc.EDITION_TIME_UTC DESC    
            ) = 1
)
SELECT
    final.USER_GAMEDAY_KEY,
    final.USER_KEY,
    final.GAMEDAY_KEY,
    final.SEASON_KEY,
    final.NB_GAME,
    final.ARE_PREDICTIONS_IN_DEFECT,
    final.CALCULATED_SCORE,
    final.AUTOMATIC_SCORE,
    final.PARTICIPATION_SCORE,
    final.TOTAL_SCORE,
    final.POINTS,
    final.TEAM_CHOICE_KEY,
    {{updated_at_fields()}}  
FROM
    score_with_chosenteam final
    {{updated_at_table_join_season('final')}}