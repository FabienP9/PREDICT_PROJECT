/* 
    The purpose of this table is to retrieve forum which changed in curated
    Inputs:
        curated_forum: the curated forum
    Primary Key:
        FORUM_KEY from curated
    Filter:
        Only new forums or forums which changed since last run
    Materialization:
        incremental to avoid removing old forums already in
*/

{{config(
    tags=['init_compet'],
    materialized="incremental",
    unique_key=['FORUM_KEY']
)}}
-- we get last run timestamp if exists
{% if is_incremental() %}
    {% set results = run_query("SELECT MAX(UPDATED_AT_UTC) as max_updated_at FROM " ~ this) %}
    {% set max_updated_at = results.columns[0].values()[0] if results.columns[0].values() else none %}
{% endif %}
-- we retrieve forums
with final_forum as (
SELECT
    forum.FORUM_KEY,
    forum.FORUM_SOURCE,
    forum.FORUM_COUNTRY
FROM
    {{ref('curated_forum')}} forum
    {% if is_incremental() and max_updated_at is not none %}
    WHERE forum.UPDATED_AT_UTC > '{{ max_updated_at }}'
    {% endif %}
)
SELECT
    final_forum.FORUM_KEY,
    final_forum.FORUM_SOURCE,
    final_forum.FORUM_COUNTRY,
    {{updated_at_fields()}}
FROM
    final_forum
LEFT JOIN
    {{ ref('local_time')}} lt 
    ON lt.COUNTRY = final_forum.FORUM_COUNTRY