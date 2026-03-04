/* 
    The purpose of this table is to extract all choices on bonus games from forums users
    Inputs:
        curated_predictraw: stored predictions, containing bonus choices
    Joins:
        curated_gameday: foreign key
        curated_game: foreign key
    Primary Key:
        CHOICE_BONUS_KEY = PREDICT_KEY from curated_predictraw_check
    Foreign key:
        GAME_KEY: 1-n relationship with game
        GAMEDAY_KEY: 1-n relationship with gameday
    Filter:
        curated_predictraw is  filtered on newly updated predictions
        We filter them on bonus choice 
    Materialization:
        Incremental to avoid deleting old choices

*/

{{config(
    materialized = "incremental",
    unique_key = 'CHOICE_BONUS_KEY',
)}}

{% if is_incremental() %}
    {% set results = run_query("SELECT MAX(UPDATED_AT_UTC) as max_updated_at FROM " ~ this) %}
    {% set max_updated_at = results.columns[0].values()[0] if results.columns[0].values() else none %}
{% endif %}

-- we extract newly updated raw predictions matching with a bonus PREDICT ID: xxxxx.BN
with predictions as (
    SELECT
        prediction.PREDICT_KEY AS CHOICE_BONUS_KEY,
        prediction.MESSAGE_EDITION_KEY,
        prediction.SEASON_KEY,
        prediction.USER_KEY,
        prediction.FORUM_KEY,
        prediction.EDITION_TIME_LOCAL,
        prediction.EDITION_TIME_UTC,
        prediction.PREDICT_ID,
        prediction.PREDICT_RAW,
        prediction.IS_PREDICTION_FROM_MESSAGE,
        prediction.IS_PROGRAM_REFINEMENT
    FROM
        {{ref('curated_predictraw_check')}} prediction
    WHERE
        SPLIT_PART(prediction.PREDICT_ID, '.', 2) = 'BN'
    {% if is_incremental() and max_updated_at is not none %}
        AND prediction.UPDATED_AT_UTC > '{{ max_updated_at }}'
    {% endif %}
),
-- we merge with gameday to extract on which gameday is the bonus choice, refining the prediction first
predictions_with_gameday as (
    SELECT
        prediction2.CHOICE_BONUS_KEY,
        prediction2.MESSAGE_EDITION_KEY,
        prediction2.SEASON_KEY,
        prediction2.USER_KEY,
        prediction2.FORUM_KEY,
        prediction2.EDITION_TIME_LOCAL,
        prediction2.EDITION_TIME_UTC,
        prediction2.PREDICT_ID,
        prediction2.PREDICT_RAW,
        prediction2.IS_PREDICTION_FROM_MESSAGE,
        prediction2.IS_PROGRAM_REFINEMENT,
        gameday.GAMEDAY_KEY,
        --we transform PREDICT_RAW into PREDICT for matching with a game
        TRIM(prediction2.PREDICT_RAW) AS PREDICT_RAW2,
        SPLIT_PART(prediction2.PREDICT_ID, '.', 1) AS GAMEDAY_PART,
        CASE
            -- if #xxx.yy# -> xxx.yy
            WHEN PREDICT_RAW2 LIKE '%.%' THEN REGEXP_REPLACE(PREDICT_RAW2, '#', '')
            -- if xxxyy -> xxx.yy
            WHEN REGEXP_LIKE(PREDICT_RAW2, GAMEDAY_PART || '[0-9]{1,2}$') THEN 
                GAMEDAY_PART || '.' || LPAD(REPLACE(PREDICT_RAW2, GAMEDAY_PART, ''), 2, '0')
            -- if y -> xxx.yy
            ELSE GAMEDAY_PART || '.' || LPAD(PREDICT_RAW2, 2, '0')
        END AS PREDICT
    FROM
        predictions prediction2
    -- by joining on the id, we get the gameday on which apply the bonus
    JOIN
        {{ref('curated_gameday')}} gameday
        ON SPLIT_PART(prediction2.PREDICT_ID, '.', 1) = gameday.GAMEDAY_MESSAGE
        AND prediction2.SEASON_KEY = gameday.SEASON_KEY
),
-- we merge with game to extract on which game is the choice, and if it is before the game
predictions_with_game as (
    SELECT
        pg.CHOICE_BONUS_KEY,
        pg.MESSAGE_EDITION_KEY,
        pg.SEASON_KEY,
        pg.USER_KEY,
        pg.FORUM_KEY,
        pg.GAMEDAY_KEY,
        game.GAME_KEY,
        pg.EDITION_TIME_LOCAL,
        pg.EDITION_TIME_UTC,
        pg.PREDICT_ID,
        pg.PREDICT_RAW,
        pg.PREDICT,
        pg.IS_PREDICTION_FROM_MESSAGE,
        pg.IS_PROGRAM_REFINEMENT,
        CASE 
                WHEN pg.EDITION_TIME_UTC <= TO_TIMESTAMP(CONCAT(TO_CHAR(game.DATE_GAME_UTC, 'YYYY-MM-DD'), ' ', TO_CHAR(game.TIME_GAME_UTC, 'HH24:MI:SS'))) 
                THEN 1
            ELSE 0
            END AS IS_ONTIME,
        -- we add a rank to get only the last ontime game prediction (next cte)
        ROW_NUMBER() OVER(
            PARTITION BY
                pg.USER_KEY,
                pg.GAMEDAY_KEY
            ORDER BY
                IS_ONTIME DESC,
                pg.EDITION_TIME_UTC DESC
        ) AS RANK_BONUS_CHOICE
    FROM
        predictions_with_gameday pg
    JOIN
        {{ref('curated_game')}} game
        ON pg.PREDICT = game.GAME_MESSAGE
        AND pg.SEASON_KEY = game.SEASON_KEY
),
-- we retrieve bonus
-- IS_LAST_ONTIME = 1 only for the latest choice before game time per user
final_bonus as (
    SELECT
        predict.CHOICE_BONUS_KEY,
        predict.MESSAGE_EDITION_KEY,
        predict.SEASON_KEY,
        predict.USER_KEY,
        predict.FORUM_KEY,
        predict.GAMEDAY_KEY,
        predict.GAME_KEY,
        predict.EDITION_TIME_LOCAL,
        predict.EDITION_TIME_UTC,
        predict.PREDICT_ID,
        predict.PREDICT_RAW,
        predict.PREDICT,
        predict.IS_PREDICTION_FROM_MESSAGE,
        predict.IS_PROGRAM_REFINEMENT,
        CASE
            WHEN RANK_BONUS_CHOICE = 1 AND IS_ONTIME = 1 THEN 1
            ELSE 0
        END AS IS_LAST_ONTIME
    FROM
        predictions_with_game predict
)
--if curated_predict_bonus_predict already exists with IS_LAST_ONTIME = 1 we need to change it if we've found a new one
{% if is_incremental() %}
,this as (
    SELECT
        this.CHOICE_BONUS_KEY,
        this.MESSAGE_EDITION_KEY,
        this.SEASON_KEY,
        this.USER_KEY,
        this.FORUM_KEY,
        this.GAMEDAY_KEY,
        this.GAME_KEY,
        this.EDITION_TIME_LOCAL,
        this.EDITION_TIME_UTC,
        this.PREDICT_ID,
        this.PREDICT_RAW,
        this.PREDICT,
        this.IS_PREDICTION_FROM_MESSAGE,
        this.IS_PROGRAM_REFINEMENT,
        0 AS IS_LAST_ONTIME
    FROM 
        final_bonus
    JOIN 
        {{this}} this
        ON final_bonus.GAMEDAY_KEY = this.GAMEDAY_KEY
        AND final_bonus.USER_KEY = this.USER_KEY
        AND final_bonus.CHOICE_BONUS_KEY != this.CHOICE_BONUS_KEY
        AND this.IS_LAST_ONTIME = 1
        AND final_bonus.IS_LAST_ONTIME = 1
),
-- we finally retrieve calculated choice with this
final_bonus2 as (
    SELECT * FROM final_bonus
    UNION
    SELECT * FROM this
)
{% endif %}
SELECT 
    final.CHOICE_BONUS_KEY,
    final.MESSAGE_EDITION_KEY,
    final.SEASON_KEY,
    final.USER_KEY,
    final.FORUM_KEY,
    final.GAMEDAY_KEY,
    final.GAME_KEY,
    final.EDITION_TIME_LOCAL,
    final.EDITION_TIME_UTC,
    final.PREDICT_ID,
    final.PREDICT_RAW,
    final.PREDICT,
    final.IS_PREDICTION_FROM_MESSAGE,
    final.IS_PROGRAM_REFINEMENT,
    final.IS_LAST_ONTIME,
    {{updated_at_fields()}}
FROM 
    {% if is_incremental() %} final_bonus2 final
    {% else %} final_bonus final
    {% endif %}
    {{updated_at_table_join_forum('final')}}