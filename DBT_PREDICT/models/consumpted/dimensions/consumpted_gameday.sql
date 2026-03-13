/* 
    The purpose of this table is to retrieve gamedays which changed in curated
    Inputs:
        curated_gameday: the curated gamedays
    Joins:
        curated_game: to have the number of game related to the gameday
    Primary Key:
        GAMEDAY_KEY from curated
    Foreign key:
        SEASON_KEY: from curated
        COMPETITION_KEY: from curated
    Filter:
        Only new gamedays or gamedays whose status flags (calculate, delete, recalculate) require action since last run
    Materialization:
        incremental to avoid removing old gamedays already in
*/

{{config(
    tags=['init_compet'],
    materialized="incremental",
    unique_key=['GAMEDAY_KEY']
)}}
-- we get last run timestamp if exists
{% if is_incremental() %}
    {% set results = run_query("SELECT MAX(UPDATED_AT_UTC) as max_updated_at FROM " ~ this) %}
    {% set max_updated_at = results.columns[0].values()[0] if results.columns[0].values() else none %}
{% endif %}

-- we extract gamedays from curated and calculate which output gameday we need
with gameday as (
    SELECT
        gameday.GAMEDAY_KEY, 
        gameday.SEASON_KEY,
        gameday.COMPETITION_KEY,
        gameday.GAMEDAY,
        gameday.GAMEDAY_MESSAGE,
        gameday.IS_PLAYED,
        gameday.IS_PARTIALLY_PLAYED,
        gameday.BEGIN_DATE_LOCAL,
        gameday.BEGIN_TIME_LOCAL,
        gameday.END_DATE_LOCAL,
        gameday.END_TIME_LOCAL,
        gameday.BEGIN_DATE_UTC,
        gameday.BEGIN_TIME_UTC,
        gameday.END_DATE_UTC,
        gameday.END_TIME_UTC,
        gameday.IS_TO_INIT,
        gameday.IS_TO_CALCULATE,
        gameday.IS_TO_DELETE,
        gameday.IS_TO_RECALCULATE,
        gameday.IS_TO_INIT AS IS_OUTPUT_INIT,
        CASE
            WHEN gameday.IS_TO_CALCULATE + gameday.IS_TO_DELETE + gameday.IS_TO_RECALCULATE > 0 THEN 1
            ELSE 0
        END AS IS_OUTPUT_CALCULATION
    FROM
        {{ref('curated_gameday')}} gameday
    {% if is_incremental() and max_updated_at is not none %}
    WHERE UPDATED_AT_UTC > '{{ max_updated_at }}'
    {% endif %}
),
-- we get the number of games
gameday_nbgame as (
    SELECT
        gameday2.GAMEDAY_KEY,
        COALESCE(COUNT(game.GAMEDAY_KEY),0) AS NB_GAME
    FROM
        gameday gameday2
    LEFT JOIN
        {{ref('curated_game')}} game
        ON game.GAMEDAY_KEY = gameday2.GAMEDAY_KEY
    GROUP BY
        gameday2.GAMEDAY_KEY
),
-- we retrieve gamedays and calculate if gamedays really need to be calculated, or deleted
final_gameday as (
    SELECT
        gameday3.GAMEDAY_KEY, 
        gameday3.SEASON_KEY,
        gameday3.COMPETITION_KEY,
        gameday3.GAMEDAY,
        gameday3.GAMEDAY_MESSAGE,
        gameday_nbgame.NB_GAME,
        gameday3.IS_PLAYED,
        gameday3.IS_PARTIALLY_PLAYED,
        gameday3.BEGIN_DATE_LOCAL,
        gameday3.BEGIN_TIME_LOCAL,
        gameday3.END_DATE_LOCAL,
        gameday3.END_TIME_LOCAL,
        gameday3.BEGIN_DATE_UTC,
        gameday3.BEGIN_TIME_UTC,
        gameday3.END_DATE_UTC,
        gameday3.END_TIME_UTC,
        gameday3.IS_OUTPUT_INIT,
        gameday3.IS_OUTPUT_CALCULATION,
        {% if is_incremental() %}
            -- if the gameday is already calculated, no need to calculate except if we want to recalculate
            CASE
                -- if played and we want to recalculate, we will calculate
                WHEN gameday3.IS_PLAYED = 1 AND gameday3.IS_TO_RECALCULATE = 1 THEN 1
                -- if played and we want to calculate and gameday doesn't exist yet in table, then we will calculate it
                WHEN gameday3.IS_PLAYED = 1 AND gameday3.IS_TO_CALCULATE = 1 AND this.GAMEDAY_KEY IS NULL THEN 1
                -- if played and we want to calculate and it is not already calculated, then we will calculate it
                WHEN gameday3.IS_PLAYED = 1 AND gameday3.IS_TO_CALCULATE = 1 AND this.IS_CALCULATED = 0 THEN 1
                -- else we don't calculate it
                ELSE 0
            END AS IS_TO_CALCULATE,
            -- if the gameday is not calculated, no need to delete it
            CASE
                WHEN gameday3.IS_TO_DELETE = 1 AND this.IS_CALCULATED = 1 THEN 1
                ELSE 0
            END AS IS_TO_DELETE,
            -- we then update IS_CALCULATED - which will help us know for next runs if it need to be calculated again
            CASE
                -- if we want to delete it then it won't be calculated anymore
                WHEN gameday3.IS_TO_DELETE = 1 AND this.IS_CALCULATED = 1 THEN 0
                -- if we want to recalculate, then it will be calculated
                WHEN gameday3.IS_PLAYED = 1 AND gameday3.IS_TO_RECALCULATE = 1 THEN 1
                -- if we want to calculate and it is not yet, then it will be calculated
                WHEN gameday3.IS_PLAYED = 1 AND gameday3.IS_TO_CALCULATE = 1 AND this.GAMEDAY_KEY IS NULL THEN 1
                WHEN gameday3.IS_PLAYED = 1 AND gameday3.IS_TO_CALCULATE = 1 AND this.IS_CALCULATED = 0 THEN 1
                WHEN this.GAMEDAY_KEY IS NULL THEN 0
                ELSE this.IS_CALCULATED
            END AS IS_CALCULATED,
        {% else %}
            gameday3.IS_TO_CALCULATE,
            0 AS IS_TO_DELETE,
            gameday3.IS_TO_CALCULATE AS IS_CALCULATED,
        {% endif %}
    FROM
        gameday gameday3
    LEFT JOIN 
        gameday_nbgame 
        ON gameday3.GAMEDAY_KEY = gameday_nbgame.GAMEDAY_KEY
    {% if is_incremental()%}
    LEFT JOIN {{this}} this ON this.GAMEDAY_KEY = gameday3.GAMEDAY_KEY
    {% endif %}
)
SELECT
    final_gameday.GAMEDAY_KEY,
    final_gameday.SEASON_KEY,
    final_gameday.COMPETITION_KEY,
    final_gameday.GAMEDAY,
    final_gameday.GAMEDAY_MESSAGE,
    final_gameday.NB_GAME,
    final_gameday.IS_PLAYED,
    final_gameday.IS_PARTIALLY_PLAYED,
    final_gameday.BEGIN_DATE_LOCAL,
    final_gameday.BEGIN_TIME_LOCAL,
    final_gameday.END_DATE_LOCAL,
    final_gameday.END_TIME_LOCAL,
    final_gameday.BEGIN_DATE_UTC,
    final_gameday.BEGIN_TIME_UTC,
    final_gameday.END_DATE_UTC,
    final_gameday.END_TIME_UTC,
    final_gameday.IS_TO_CALCULATE,
    final_gameday.IS_TO_DELETE,
    final_gameday.IS_CALCULATED,
    final_gameday.IS_OUTPUT_INIT,
    final_gameday.IS_OUTPUT_CALCULATION,
    {{updated_at_fields()}}
FROM
    final_gameday
    {{updated_at_table_join_season('final_gameday')}}