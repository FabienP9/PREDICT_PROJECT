/* 
    The purpose of this table is to extract all predictions from message, in a raw manner
    The refinement is done in curated_message_bonus_check / curated_message_game_check / curated_message_team_check    
    Inputs:
        landing_message_check: stored messages
    Joins:
        curated_message_check: get active version of messages
        landing_message_correction: corrections of predictions
        landing_message_quotes_to_keep: to get messages with quotes we need to keep
    Primary key:
        PREDICT_KEY based on FORUM_SOURCE, MESSAGE_FORUM_ID, EDITION_TIME_UTC and PREDICT_ID
    Filter:
        curated_message_check is filtered on edition time > the last processed
        landing_message_correction is already filtered on messages processed
        landing_message_quotes_to_add is already filtered on messages we need to process
    Materialization:
        Incremental to avoid deleting old predictions
*/
{{config(
    materialized = "incremental",
    unique_key = 'PREDICT_KEY',
)}}

{% if is_incremental() %}
    {% set results = run_query("SELECT MAX(EDITION_TIME_UTC) as max_updated_at FROM " ~ this) %}
    {% set max_updated_at = results.columns[0].values()[0] if results.columns[0].values() else none %}
{% endif %}

-- we get correction of prediction
with correction as (
    SELECT
        correction.FORUM_SOURCE,
        correction.MESSAGE_FORUM_ID,
        correction.PREDICT_ID,
        correction.PREDICT,
        correction.IS_PROGRAM_REFINEMENT
    FROM
        {{source('LAND','PREDICT_CORRECTION')}} correction
),
-- we get messages quotes to keep
message_quotes_to_keep as (
    SELECT
        quotes.FORUM_SOURCE,
        quotes.MESSAGE_FORUM_ID
    from
        {{source("LAND", "MESSAGE_QUOTE_TO_KEEP")}} quotes
),
-- we extract all prediction from new or old corrected messages from curated_message_check
-- for that we first dequote message which need it
-- the form of prediction is "#xxx# yyyy ==> zzzz ;""
-- we use distinct because the granularity is the message, not the prediction
message_scope as (
    SELECT DISTINCT
        message.MESSAGE_EDITION_KEY,
        message.SEASON_KEY,
        message.USER_KEY,
        forum.FORUM_KEY,
        forum.FORUM_SOURCE,
        message.MESSAGE_FORUM_ID,
        message.EDITION_TIME_LOCAL,
        message.EDITION_TIME_UTC,
        CASE
            WHEN quotes2.MESSAGE_FORUM_ID IS NULL
                -- Remove quoted content within <blockquote> tags, for quotes not into keep 
                THEN REGEXP_REPLACE(message.MESSAGE_CONTENT,'<blockquote>.*?</blockquote>','')
            ELSE
                message.MESSAGE_CONTENT
        END AS MESSAGE_TRANSFORMED,
        REGEXP_SUBSTR_ALL(MESSAGE_TRANSFORMED, '#[^#;]+#[^=>]+==>[^;<>]+') AS ARRAY_PREDICTIONS
    FROM
        {{ref('curated_message_check')}} message
    LEFT JOIN
        {{ref('curated_forum')}} forum
        ON forum.FORUM_KEY = message.FORUM_KEY
    LEFT JOIN
        correction correction2
        ON correction2.MESSAGE_FORUM_ID = message.MESSAGE_FORUM_ID
        AND correction2.FORUM_SOURCE = forum.FORUM_SOURCE
    LEFT JOIN
        message_quotes_to_keep quotes2
        ON forum.FORUM_SOURCE = quotes2.FORUM_SOURCE
        AND message.MESSAGE_FORUM_ID = quotes2.MESSAGE_FORUM_ID
    WHERE
        message.IS_MESSAGE_TO_PROCESS = 1
    {% if is_incremental() and max_updated_at is not none %}
        AND (
            -- new one
            message.DBT_VALID_FROM > '{{ max_updated_at }}'
            -- or corrected one
            OR correction2.MESSAGE_FORUM_ID IS NOT NULL
            -- or where quotes is to add
            OR quotes2.MESSAGE_FORUM_ID IS NOT NULL)
    {% endif %}
),
-- we split each prediction to get "#xxxx#" as id and the raw prediction after "===>"
-- we keep message which have no predictions, for a next cte match with correction file
prediction as (
    -- message with predictions
    SELECT
        base.MESSAGE_EDITION_KEY,
        base.SEASON_KEY,
        base.USER_KEY,
        base.FORUM_KEY,
        base.FORUM_SOURCE,
        base.MESSAGE_FORUM_ID,
        base.EDITION_TIME_LOCAL,
        base.EDITION_TIME_UTC,
        base.MESSAGE_TRANSFORMED,
        base.ARRAY_PREDICTIONS,
        F.VALUE AS PREDICTION,
        SUBSTR(PREDICTION, REGEXP_INSTR(PREDICTION,'#',1,1)+1,REGEXP_INSTR(PREDICTION,'#',1,2)-2) AS PREDICT_ID,
        SUBSTR(PREDICTION, REGEXP_INSTR(PREDICTION,'==>',1,1)+3) AS PREDICT_RAW,
        1 AS IS_PREDICTION_FROM_MESSAGE
    FROM
        message_scope base,
        LATERAL FLATTEN(input => ARRAY_PREDICTIONS) F
UNION ALL
    -- message without predictions
    SELECT
        base.MESSAGE_EDITION_KEY,
        base.SEASON_KEY,
        base.USER_KEY,
        base.FORUM_KEY,
        base.FORUM_SOURCE,
        base.MESSAGE_FORUM_ID,
        base.EDITION_TIME_LOCAL,
        base.EDITION_TIME_UTC,
        base.MESSAGE_TRANSFORMED,
        NULL AS ARRAY_PREDICTIONS,
        NULL AS PREDICTION,
        NULL AS PREDICT_ID,
        NULL AS PREDICT_RAW,
        1 AS IS_PREDICTION_FROM_MESSAGE
    FROM
        message_scope base
    WHERE
        ARRAY_SIZE(ARRAY_PREDICTIONS) = 0
),
-- we add corrections tables where message_source_id match
prediction_with_correction as(
    -- predictions from the previous cte
    SELECT 
        prediction.MESSAGE_EDITION_KEY,
        prediction.SEASON_KEY,
        prediction.USER_KEY,
        prediction.FORUM_KEY,
        prediction.FORUM_SOURCE,
        prediction.MESSAGE_FORUM_ID,
        prediction.EDITION_TIME_LOCAL,
        prediction.EDITION_TIME_UTC,
        prediction.MESSAGE_TRANSFORMED,
        prediction.ARRAY_PREDICTIONS,
        prediction.PREDICTION,
        prediction.PREDICT_ID,
        prediction.PREDICT_RAW,
        prediction.IS_PREDICTION_FROM_MESSAGE,
        0 AS IS_PROGRAM_REFINEMENT
    FROM 
        prediction
    UNION ALL
    -- correction on predictions from previous cte
    SELECT DISTINCT
        prediction.MESSAGE_EDITION_KEY,
        prediction.SEASON_KEY,
        prediction.USER_KEY,
        prediction.FORUM_KEY,
        prediction.FORUM_SOURCE,
        prediction.MESSAGE_FORUM_ID,
        prediction.EDITION_TIME_LOCAL,
        prediction.EDITION_TIME_UTC,
        prediction.MESSAGE_TRANSFORMED,
        prediction.ARRAY_PREDICTIONS,
        NULL AS PREDICTION,
        correction3.PREDICT_ID,
        correction3.PREDICT AS PREDICT_RAW,
        0 AS IS_PREDICTION_FROM_MESSAGE,
        correction3.IS_PROGRAM_REFINEMENT
    FROM 
        prediction  
    JOIN
        correction correction3
        ON correction3.MESSAGE_FORUM_ID = prediction.MESSAGE_FORUM_ID
        AND correction3.FORUM_SOURCE = prediction.FORUM_SOURCE
),
-- we can't have several prediction concerning the same PREDICT_ID on same message_edition
-- if there is we take the one corrected if exists, else the minimum value of predict raw
prediction_ranked as (
    SELECT
        pc.MESSAGE_EDITION_KEY,
        pc.SEASON_KEY,
        pc.USER_KEY,
        pc.FORUM_KEY,
        pc.FORUM_SOURCE,
        pc.MESSAGE_FORUM_ID,
        pc.EDITION_TIME_LOCAL,
        pc.EDITION_TIME_UTC,
        pc.MESSAGE_TRANSFORMED,
        pc.ARRAY_PREDICTIONS,
        pc.PREDICTION,
        pc.PREDICT_ID,
        pc.PREDICT_RAW,
        pc.IS_PREDICTION_FROM_MESSAGE,
        pc.IS_PROGRAM_REFINEMENT
    FROM
        prediction_with_correction pc
    WHERE
        -- we remove message without predictions now we have already corrected them
        pc.PREDICT_ID IS NOT NULL
    -- we deduplicate predictions
    QUALIFY
        ROW_NUMBER() OVER (
            PARTITION BY 
                pc.MESSAGE_EDITION_KEY,
                pc.PREDICT_ID
            ORDER BY
                pc.IS_PREDICTION_FROM_MESSAGE ASC, -- if corrected (0), it will be taken first
                pc.IS_PROGRAM_REFINEMENT DESC, -- if there is a refinement (1), we take it first
                pc.PREDICT_RAW ASC) -- PREDICT_RAW min first
        = 1 
),
-- we retrieve the raw predictions with the PREDICT_KEY
final_prediction as (
    SELECT
        MD5('PREDICTION' || '^^' || pr.FORUM_SOURCE || '^^' || 
            pr.MESSAGE_FORUM_ID || '^^' || pr.EDITION_TIME_UTC || '^^' ||
            pr.PREDICT_ID) AS PREDICT_KEY,
        pr.SEASON_KEY,
        pr.USER_KEY,
        pr.FORUM_KEY,
        pr.MESSAGE_EDITION_KEY,
        pr.EDITION_TIME_LOCAL,
        pr.EDITION_TIME_UTC,
        pr.MESSAGE_TRANSFORMED,
        pr.ARRAY_PREDICTIONS,
        pr.PREDICTION,
        pr.PREDICT_ID,
        pr.PREDICT_RAW,
        pr.IS_PREDICTION_FROM_MESSAGE,
        pr.IS_PROGRAM_REFINEMENT
    FROM
        prediction_ranked pr
)
SELECT
    final_prediction.PREDICT_KEY,
    final_prediction.MESSAGE_EDITION_KEY,
    final_prediction.SEASON_KEY,
    final_prediction.USER_KEY,
    final_prediction.FORUM_KEY,
    final_prediction.EDITION_TIME_LOCAL,
    final_prediction.EDITION_TIME_UTC,
    final_prediction.MESSAGE_TRANSFORMED,
    final_prediction.ARRAY_PREDICTIONS,
    final_prediction.PREDICTION,
    final_prediction.PREDICT_ID,
    final_prediction.PREDICT_RAW,
    final_prediction.IS_PREDICTION_FROM_MESSAGE,
    final_prediction.IS_PROGRAM_REFINEMENT,
    {{updated_at_fields()}}
FROM
    final_prediction
    {{updated_at_table_join_forum('final_prediction')}}
