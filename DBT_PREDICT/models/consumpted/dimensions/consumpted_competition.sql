/* 
    The purpose of this table is to retrieve competitions which changed
    Inputs:
        curated_competition: we get competitions from there
    Primary key:
        COMPETITION_KEY: from curated
    Foreign key:
        SEASON_KEY: 1-n relationship with season
    Filter:
        Only new competitions or competitions which changed since last run
    Materialization:
        Incremental to avoid deleting old competition
*/
{{config(
    tags=['init_compet'],
    materialized="incremental",
    unique_key = ['COMPETITION_KEY']
)}}
-- we get last run timestamp if exists
{% if is_incremental() %}
    {% set results = run_query("SELECT MAX(UPDATED_AT_UTC) as max_updated_at FROM " ~ this) %}
    {% set max_updated_at = results.columns[0].values()[0] if results.columns[0].values() else none %}
{% endif %}

-- we retrieve competition
with final_compet as (
    SELECT
        compet.COMPETITION_KEY,
        compet.SEASON_KEY,
        compet.COMPETITION_ID,
        compet.COMPETITION_SOURCE,
        compet.COMPETITION_LABEL,
        compet.IS_SAME_FOR_PREDICTCHAMP,
        compet.IS_FOR_RANK
    FROM 
        {{ref('curated_competition')}} compet
    {% if is_incremental() and max_updated_at is not none %}
    WHERE UPDATED_AT_UTC > '{{ max_updated_at }}'
    {% endif %}
)
SELECT
    final_compet.COMPETITION_KEY,
    final_compet.SEASON_KEY,
    final_compet.COMPETITION_ID,
    final_compet.COMPETITION_SOURCE,
    final_compet.COMPETITION_LABEL,
    final_compet.IS_SAME_FOR_PREDICTCHAMP,
    final_compet.IS_FOR_RANK,
    {{updated_at_fields()}}
FROM 
    final_compet
    {{updated_at_table_join_season('final_compet')}}