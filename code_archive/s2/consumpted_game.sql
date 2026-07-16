/* 
    The purpose of this table is to retrieve games which changed in curated
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
with final_game as (
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
)
SELECT
    final_game.GAME_KEY,
    final_game.SEASON_KEY,
    final_game.COMPETITION_KEY,
    final_game.GAMEDAY_KEY,
    final_game.GAME_SOURCE_ID,
    final_game.GAME_MESSAGE_SHORT,
    final_game.GAME_MESSAGE,
    final_game.DATE_GAME_LOCAL,
    final_game.TIME_GAME_LOCAL,
    final_game.DATE_GAME_UTC,
    final_game.TIME_GAME_UTC,
    final_game.TEAM_HOME_KEY,
    final_game.TEAM_AWAY_KEY,
    final_game.SCORE_HOME,
    final_game.SCORE_AWAY,
    final_game.RESULT,
    final_game.IS_PLAYED,
    {{updated_at_fields()}}
FROM
    final_game
    {{updated_at_table_join_season('final_game')}}