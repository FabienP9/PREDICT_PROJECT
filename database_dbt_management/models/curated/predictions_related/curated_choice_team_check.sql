/* 
    The purpose of this table is to extract all team choices from forums users  
    Inputs:
        curated_predictgross: stored predictions
    Joins:
        curated_season: foreign key
        curated_team: foreign key
    Primary Key:
        CHOICE_TEAM_KEY based on FORUM_SOURCE and MESSAGE_FORUM_ID and EDITION_TIME
    Foreign key:
        TEAM_KEY: 1-n relationship with the team the user choose
    Filter:
        curated_predictraw is already filtered on predictions we need to process
        curated_season: filter on prediction made before the deadline
    Materialization:
        Incremental to avoid deleting old choices
*/

{{config(
    materialized = "incremental",
    unique_key = 'CHOICE_TEAM_KEY',
)}}

{% if is_incremental() %}
    {% set results = run_query("SELECT MAX(UPDATED_AT_UTC) as max_updated_at FROM " ~ this) %}
    {% set max_updated_at = results.columns[0].values()[0] if results.columns[0].values() else none %}
{% endif %}

-- we extract predictions newly updated with PREDICT_ID = 'SEASON_TM'
with predictions as (
    SELECT
        prediction.PREDICT_KEY AS CHOICE_TEAM_KEY,
        prediction.MESSAGE_EDITION_KEY,
        prediction.SEASON_KEY,
        prediction.USER_KEY,
        prediction.FORUM_KEY,
        prediction.EDITION_TIME_LOCAL,
        prediction.EDITION_TIME_UTC,
        prediction.PREDICT_ID,
        prediction.PREDICT_RAW,
        prediction.IS_PREDICTION_FROM_MESSAGE,
        prediction.IS_PROGRAM_REFINEMENT,
        CASE
            WHEN prediction.EDITION_TIME_LOCAL <= season.SEASON_TEAMCHOICE_DEADLINE THEN 1
            ELSE 0
        END AS IS_ONTIME
    FROM
        {{ref('curated_predictraw_check')}} prediction
    JOIN
        {{ref('curated_season')}} season
        ON prediction.SEASON_KEY = season.SEASON_KEY
        AND prediction.PREDICT_ID = 'SEASON.TM'
        {% if is_incremental() and max_updated_at is not none %}
            AND prediction.UPDATED_AT_UTC > '{{ max_updated_at }}'
        {% endif %}
),
-- we merge with all season's teams to extract the closest with manual team user's choice
-- for that,we apply the JAROWINKLER function to get the closest team in the list of possible teams
predictions_with_team as (
    SELECT
        prediction2.CHOICE_TEAM_KEY,
        prediction2.MESSAGE_EDITION_KEY,
        prediction2.SEASON_KEY,
        prediction2.USER_KEY,
        prediction2.FORUM_KEY,
        prediction2.EDITION_TIME_LOCAL,
        prediction2.EDITION_TIME_UTC,
        prediction2.PREDICT_ID,
        prediction2.PREDICT_RAW,
        team.TEAM_NAME AS PREDICT,
        prediction2.IS_PREDICTION_FROM_MESSAGE,
        prediction2.IS_PROGRAM_REFINEMENT,
        prediction2.IS_ONTIME,
        JAROWINKLER_SIMILARITY(lower(trim(prediction2.PREDICT_RAW)), lower(trim(team.TEAM_NAME))) AS TEAM_SCORE_JARO,
        team.TEAM_KEY
    FROM
        predictions prediction2
    JOIN
        {{ref('curated_team')}} team
        ON prediction2.SEASON_KEY = team.SEASON_KEY
    WHERE
        prediction2.PREDICT_RAW IS NOT NULL
        AND TRIM(prediction2.PREDICT_RAW) != ''
    QUALIFY    
        -- we get the closest team
        ROW_NUMBER() OVER (
                PARTITION BY prediction2.CHOICE_TEAM_KEY
                ORDER BY TEAM_SCORE_JARO DESC, team.TEAM_KEY
            )
        = 1
)
SELECT 
    final.CHOICE_TEAM_KEY,
    final.MESSAGE_EDITION_KEY,
    final.SEASON_KEY,
    final.USER_KEY,
    final.FORUM_KEY,
    final.TEAM_KEY,
    final.EDITION_TIME_LOCAL,
    final.EDITION_TIME_UTC,
    final.PREDICT_ID,
    final.PREDICT_RAW,
    final.PREDICT,
    final.IS_PREDICTION_FROM_MESSAGE,
    final.IS_PROGRAM_REFINEMENT,
    final.IS_ONTIME,
    {{updated_at_fields()}}
FROM 
    predictions_with_team final
    {{updated_at_table_join_forum('final')}}