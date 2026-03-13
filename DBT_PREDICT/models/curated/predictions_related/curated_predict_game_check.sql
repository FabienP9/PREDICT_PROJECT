/* 
    The purpose of this table is to extract all predictions on games from forums, possibly corrected    
    Inputs:
        curated_predictraw: stored raw predictions already corrected
    Joins:
        curated_game: foreign key
    Primary Key:
        PREDICT_GAME_KEY (same than PREDICT_KEY from curated_predictraw)
    Foreign key:
        GAME_KEY: 1-n relationship with game
    Filter:
        we get only curated_predictraw which are newly updated
    Materialization:
        Incremental to avoid deleting old predictions
*/

{{config(
    materialized = "incremental",
    unique_key = 'PREDICT_GAME_KEY',
)}}

{% if is_incremental() %}
    {% set results = run_query("SELECT MAX(UPDATED_AT_UTC) as max_updated_at FROM " ~ this) %}
    {% set max_updated_at = results.columns[0].values()[0] if results.columns[0].values() else none %}
{% endif %}

-- we extract raw predictions newly updated
with predictions as (
    SELECT
        prediction.PREDICT_KEY AS PREDICT_GAME_KEY,
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
        {% if is_incremental() and max_updated_at is not none %}
        WHERE prediction.UPDATED_AT_UTC > '{{ max_updated_at }}'
        {% endif %}
),
-- we merge with game to extract only game predictions, and refine predict_raw to have an integer
-- if impossible to have an integer we delete the prediction
predictions_with_game as (
    SELECT
        predict.PREDICT_GAME_KEY,
        predict.MESSAGE_EDITION_KEY,
        predict.SEASON_KEY,
        predict.USER_KEY,
        predict.FORUM_KEY,
        predict.EDITION_TIME_LOCAL,
        predict.EDITION_TIME_UTC,
        predict.PREDICT_ID,
        game.GAME_KEY,
        predict.PREDICT_RAW,
        REPLACE(predict.PREDICT_RAW, ' ', '') AS PREDICT_RAW2,
        TRY_CAST(CASE
            -- raw predictions like "--6" or "-+6" have been seen and need to be handled as "-6"
            WHEN PREDICT_RAW2 LIKE '+-%' OR PREDICT_RAW2 LIKE '--%' OR PREDICT_RAW2 LIKE '++%' OR PREDICT_RAW2 LIKE '-+%' THEN LEFT(PREDICT_RAW2, 1) || SUBSTRING(PREDICT_RAW2, 3)
            ELSE PREDICT_RAW2
        END AS NUMBER) AS PREDICT,
        predict.IS_PREDICTION_FROM_MESSAGE,
        predict.IS_PROGRAM_REFINEMENT,
        CASE 
            WHEN predict.EDITION_TIME_UTC <= TO_TIMESTAMP(CONCAT(TO_CHAR(game.DATE_GAME_UTC, 'YYYY-MM-DD'), ' ', TO_CHAR(game.TIME_GAME_UTC, 'HH24:MI:SS'))) 
                THEN 1
            ELSE 0
        END AS IS_ONTIME,
        -- we add a rank to get the last ontime game prediction (next cte)
        ROW_NUMBER() OVER(
            PARTITION BY
                predict.USER_KEY,
                game.GAME_KEY
            ORDER BY
                IS_ONTIME DESC,
                predict.EDITION_TIME_UTC DESC
        ) AS RANK_GAME_PREDICTION
    FROM
        predictions predict
    JOIN
        {{ref('curated_game')}} game
        ON predict.SEASON_KEY = game.SEASON_KEY
        AND predict.PREDICT_ID = game.GAME_MESSAGE
    WHERE 
        -- if TRY_CAST didn't make an integer: it returns NULL, we filter prediction like this
        PREDICT IS NOT NULL
),
-- we retrieve games prediction
-- IS_LAST_ONTIME = 1 only for the latest prediction before game time per user
final_predict_game as (
    SELECT
        pg.PREDICT_GAME_KEY,
        pg.MESSAGE_EDITION_KEY,
        pg.SEASON_KEY,
        pg.USER_KEY,
        pg.FORUM_KEY,
        pg.EDITION_TIME_LOCAL,
        pg.EDITION_TIME_UTC,
        pg.GAME_KEY,
        pg.PREDICT_ID,
        pg.PREDICT_RAW,
        pg.PREDICT,
        pg.IS_PREDICTION_FROM_MESSAGE,
        pg.IS_PROGRAM_REFINEMENT,
        CASE
            WHEN RANK_GAME_PREDICTION = 1 AND IS_ONTIME = 1 THEN 1
            ELSE 0
        END AS IS_LAST_ONTIME
    FROM
        predictions_with_game pg
)
-- if curated_predict_game_check already exists with IS_LAST_ONTIME = 1 
-- we need to switch to 0, considering the new last
{% if is_incremental() %}
,this as (
    SELECT
        this.PREDICT_GAME_KEY,
        this.MESSAGE_EDITION_KEY,
        this.SEASON_KEY,
        this.USER_KEY,
        this.FORUM_KEY,
        this.EDITION_TIME_LOCAL,
        this.EDITION_TIME_UTC,
        this.GAME_KEY,
        this.PREDICT_ID,
        this.PREDICT_RAW,
        this.PREDICT,
        this.IS_PREDICTION_FROM_MESSAGE,
        this.IS_PROGRAM_REFINEMENT,
        0 AS IS_LAST_ONTIME
    FROM 
        final_predict_game
    JOIN 
        {{this}} this
        ON final_predict_game.USER_KEY = this.USER_KEY
        AND final_predict_game.GAME_KEY = this.GAME_KEY
        AND final_predict_game.PREDICT_GAME_KEY != this.PREDICT_GAME_KEY
        AND this.IS_LAST_ONTIME = 1
        AND final_predict_game.IS_LAST_ONTIME = 1
),
-- we finally retreive prediction calculated and those from this
final_predict_game2 as (
    SELECT * FROM final_predict_game
    UNION
    SELECT * FROM this
)
{% endif %}
SELECT 
    final.PREDICT_GAME_KEY,
    final.MESSAGE_EDITION_KEY,
    final.SEASON_KEY,
    final.USER_KEY,
    final.FORUM_KEY,
    final.EDITION_TIME_LOCAL,
    final.EDITION_TIME_UTC,
    final.GAME_KEY,
    final.PREDICT_ID,
    final.PREDICT_RAW,
    final.PREDICT,
    final.IS_PREDICTION_FROM_MESSAGE,
    final.IS_PROGRAM_REFINEMENT,
    final.IS_LAST_ONTIME,
    {{updated_at_fields()}}
FROM 
    {% if is_incremental() %} final_predict_game2 final
    {% else %} final_predict_game final
    {% endif %}
    {{updated_at_table_join_forum('final')}}