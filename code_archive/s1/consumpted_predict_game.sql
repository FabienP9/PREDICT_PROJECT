/*  The purpose of this table is to calculate scores on each prediction for gameday with action
    Inputs:
        curated_predict_game: the curated prediction of games
    Joins
        consumpted_game: foreign key
        consumpted_gameday: get gameday with action
        curated_choice_bonus: the curated choice of bonus game
    Primary Key:
        PREDICT_GAME_KEY
    Foreign Key:
        MESSAGE_KEY: from curated
        TOPIC_KEY: from curated
        USER_KEY: from curated
        SEASON_KEY: from curated
        GAME_KEY: from curated
        GAMEDAY_KEY: from curated
    Filter:
        Only games prediction related to the gameday with action where prediction is last ontime
    Materialization:
        incremental to avoid removing old prediction already calculated */

{{config(
    materialized="incremental",
    unique_key=['PREDICT_GAME_KEY']
)}}
-- we get prediction only for gameday with action from consumpted_gameday and we calculate if it is a bonus game
-- only for last_ontime predictions
with prediction as (
    SELECT
        predict.PREDICT_GAME_KEY,
        predict.SEASON_KEY,
        predict.USER_KEY,
        predict.FORUM_KEY,
        predict.GAME_KEY,
        game.RESULT AS GAME_RESULT,
        predict.PREDICT_ID,
        predict.PREDICT,
        predict.IS_PREDICTION_FROM_MESSAGE AS IS_PREDICTION_FROM_MESSAGE_GAME,
        predict.IS_PROGRAM_REFINEMENT AS IS_PROGRAM_REFINEMENT_GAME,
        CASE
            WHEN bonus.GAME_KEY IS NULL then 0
            ELSE 1
        END AS IS_BONUS_GAME,
        bonus.IS_PREDICTION_FROM_MESSAGE AS IS_PREDICTION_FROM_MESSAGE_BONUS,
        bonus.IS_PROGRAM_REFINEMENT AS IS_PROGRAM_REFINEMENT_BONUS,
        gameday.GAMEDAY_KEY,
        gameday.IS_TO_CALCULATE,
        gameday.IS_TO_DELETE
    FROM 
        {{ref('curated_predict_game')}} predict
    JOIN
        {{ref('consumpted_game')}} game
        ON predict.GAME_KEY = game.GAME_KEY
    JOIN
        {{ref('consumpted_gameday')}} gameday
        ON game.GAMEDAY_KEY = gameday.GAMEDAY_KEY
        AND gameday.IS_TO_CALCULATE + gameday.IS_TO_DELETE > 0
    LEFT JOIN
        {{ref('curated_choice_bonus')}} bonus
        ON predict.GAME_KEY = bonus.GAME_KEY
        AND predict.USER_KEY = bonus.USER_KEY
        AND bonus.IS_LAST_ONTIME = 1
    WHERE
        predict.IS_LAST_ONTIME = 1
),
-- we then calculate score per game prediction with action
calculation as (
    SELECT 
        predict2.PREDICT_GAME_KEY,
        predict2.SEASON_KEY,
        predict2.USER_KEY,
        predict2.FORUM_KEY,
        predict2.GAMEDAY_KEY,
        predict2.GAME_KEY,
        predict2.GAME_RESULT,
        predict2.PREDICT_ID,
        predict2.PREDICT,
        predict2.IS_PREDICTION_FROM_MESSAGE_GAME,
        predict2.IS_PROGRAM_REFINEMENT_GAME,
        predict2.IS_BONUS_GAME,
        predict2.IS_PREDICTION_FROM_MESSAGE_BONUS,
        predict2.IS_PROGRAM_REFINEMENT_BONUS,
        predict2.IS_TO_CALCULATE,
        predict2.IS_TO_DELETE,
        CASE
            -- for deletion we revert back score to 0
            WHEN predict2.IS_TO_DELETE = 1 THEN 0
            -- if the game result and prediction are not same sign, the user didn't found the good winner and win 0 points
            WHEN predict2.GAME_RESULT * COALESCE(predict2.PREDICT,0) <= 0 THEN 0
            -- if he found the good winner the score is 3 if IS_BONUS = 0 (not bonus game) else 9
            WHEN predict2.GAME_RESULT * COALESCE(predict2.PREDICT,0) > 0 THEN (2*predict2.IS_BONUS_GAME +1)*3
        END AS SCORE_WIN,
        CASE
            -- for deletion we revert back score to 0
            WHEN predict2.IS_TO_DELETE = 1 THEN 0
            -- if the absolute difference between result and prediction is 5 or less, award shift score, else no
            WHEN ABS(predict2.GAME_RESULT - predict2.PREDICT) > 5 THEN 0
            -- if it is ok the score is 1 if IS_BONUS = 0 (not bonus game) else 3
            WHEN ABS(predict2.GAME_RESULT - predict2.PREDICT) <= 5 THEN 2*predict2.IS_BONUS_GAME +1
        END AS SCORE_SHIFT,
        SCORE_WIN + SCORE_SHIFT AS SCORE
    FROM prediction predict2
),
-- we retrieve scores, and get if the prediction with its bonus choice (if exists) is in defect or not. 
-- Either the prediction or the bonus have to be in defect for the gloabl result to be in defect
final_calculation as (
    SELECT
        calcul.PREDICT_GAME_KEY,
        calcul.SEASON_KEY,
        calcul.USER_KEY,
        calcul.FORUM_KEY,
        calcul.GAMEDAY_KEY,
        calcul.GAME_KEY,
        calcul.GAME_RESULT,
        calcul.PREDICT_ID,
        calcul.PREDICT,
        calcul.IS_BONUS_GAME,
        CASE
            WHEN IS_PREDICTION_FROM_MESSAGE_GAME = 0 AND calcul.IS_PROGRAM_REFINEMENT_GAME = 0 THEN 1
            WHEN IS_PREDICTION_FROM_MESSAGE_BONUS = 0 AND calcul.IS_PROGRAM_REFINEMENT_BONUS = 0 THEN 1
            ELSE 0
        END AS IS_PREDICTION_IN_DEFECT,
        calcul.SCORE_WIN,
        calcul.SCORE_SHIFT,
        calcul.SCORE
    FROM
        calculation calcul
)
SELECT
    final_calcul.PREDICT_GAME_KEY,
    final_calcul.SEASON_KEY,
    final_calcul.USER_KEY,
    final_calcul.GAMEDAY_KEY,
    final_calcul.GAME_KEY,
    final_calcul.GAME_RESULT,
    final_calcul.PREDICT_ID,
    final_calcul.PREDICT,
    final_calcul.IS_BONUS_GAME,
    final_calcul.IS_PREDICTION_IN_DEFECT,
    final_calcul.SCORE_WIN,
    final_calcul.SCORE_SHIFT,
    final_calcul.SCORE,
    {{updated_at_fields()}}
FROM 
    final_calculation final_calcul
    {{updated_at_table_join_forum('final_calcul')}}