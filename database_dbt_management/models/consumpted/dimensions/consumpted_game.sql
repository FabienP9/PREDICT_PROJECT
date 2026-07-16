/* 
    The purpose of this table is to retrieve games which changed in curated
    with their prediction odds and SCORE_WIN value calculated
    Inputs:
        curated_game: the curated gamedays
    Primary Key:
        GAME_KEY from curated
    Foreign key:
        SEASON_KEY: from curated
        COMPETITION_KEY: from curated
        GAMEDAY_KEY: from curated
        TEAM_HOME_KEY / TEAM_AWAY_KEY: from curated
    Filter:
        Only new games or games which changed since last run
    Materialization:
        incremental to avoid removing old gamedays already in
*/

{{config(
    tags=['init_compet'],
    materialized="incremental",
    unique_key=['GAME_KEY']
)}}
-- we get last run timestamp if exists
{% if is_incremental() %}
    {% set results = run_query("SELECT MAX(UPDATED_AT_UTC) as max_updated_at FROM " ~ this) %}
    {% set max_updated_at = results.columns[0].values()[0] if results.columns[0].values() else none %}
{% endif %}
-- we retrieve games
with game_extraction as (
    SELECT
        game.GAME_KEY,
        game.SEASON_KEY,
        game.COMPETITION_KEY,
        game.GAMEDAY_KEY,
        game.GAME_SOURCE_ID,
        game.GAME_MESSAGE_SHORT,
        game.GAME_MESSAGE,
        game.DATE_GAME_LOCAL,
        game.TIME_GAME_LOCAL,
        game.DATE_GAME_UTC,
        game.TIME_GAME_UTC,
        game.TEAM_HOME_KEY,
        game.TEAM_AWAY_KEY,
        game.SCORE_HOME,
        game.SCORE_AWAY,
        game.RESULT,
        game.IS_PLAYED
    FROM
        {{ref('curated_game')}} game
        {% if is_incremental() and max_updated_at is not none %}
    WHERE UPDATED_AT_UTC > '{{ max_updated_at }}'
        {% endif %}
),
odds as (
    SELECT
        predict_game.GAME_KEY,
        COALESCE(SUM(
            CASE
                WHEN predict_game.PREDICT > 0 THEN 1
                ELSE 0
            END
        ),0) AS NB_PREDICTOR_WINNER_HOME,
        COALESCE(SUM(
            CASE
                WHEN predict_game.PREDICT < 0 THEN 1
                ELSE 0
            END
        ),0) AS NB_PREDICTOR_WINNER_AWAY,
        CAST(COALESCE(NB_PREDICTOR_WINNER_HOME*100/ NULLIF(NB_PREDICTOR_WINNER_HOME + NB_PREDICTOR_WINNER_AWAY,0),NULL) AS INT) AS PERC_PREDICTOR_WINNER_HOME,
        CAST(COALESCE(NB_PREDICTOR_WINNER_AWAY*100/ NULLIF(NB_PREDICTOR_WINNER_HOME + NB_PREDICTOR_WINNER_AWAY,0),NULL) AS INT) AS PERC_PREDICTOR_WINNER_AWAY,
    FROM
        {{ref('curated_predict_game')}} predict_game
    JOIN 
        game_extraction
        ON game_extraction.GAME_KEY = predict_game.GAME_KEY
    GROUP BY
        predict_game.GAME_KEY
)
SELECT
    game_extraction.GAME_KEY,
    game_extraction.SEASON_KEY,
    game_extraction.COMPETITION_KEY,
    game_extraction.GAMEDAY_KEY,
    game_extraction.GAME_SOURCE_ID,
    game_extraction.GAME_MESSAGE_SHORT,
    game_extraction.GAME_MESSAGE,
    game_extraction.DATE_GAME_LOCAL,
    game_extraction.TIME_GAME_LOCAL,
    game_extraction.DATE_GAME_UTC,
    game_extraction.TIME_GAME_UTC,
    game_extraction.TEAM_HOME_KEY,
    game_extraction.TEAM_AWAY_KEY,
    game_extraction.SCORE_HOME,
    game_extraction.SCORE_AWAY,
    game_extraction.RESULT,
    game_extraction.IS_PLAYED,
    odds.NB_PREDICTOR_WINNER_HOME,
    odds.NB_PREDICTOR_WINNER_AWAY,
    odds.PERC_PREDICTOR_WINNER_HOME,
    odds.PERC_PREDICTOR_WINNER_AWAY,
    15 + CAST(CASE
        WHEN game_extraction.RESULT > 0 AND odds.PERC_PREDICTOR_WINNER_HOME < 40 THEN 40-odds.PERC_PREDICTOR_WINNER_HOME
        WHEN game_extraction.RESULT < 0 AND odds.PERC_PREDICTOR_WINNER_AWAY < 40 THEN 40-odds.PERC_PREDICTOR_WINNER_AWAY
        ELSE 0
    END AS NUMBER(2)) AS SCORE_WIN_VALUE,
    {{updated_at_fields()}}
FROM
    game_extraction
JOIN
    odds
    ON odds.GAME_KEY = game_extraction.GAME_KEY
    {{updated_at_table_join_season('game_extraction')}}