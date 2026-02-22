/* 
    The purpose of this table is to retrieve topic which changed in curated
    Inputs:
        curated_topic: the curated topic
    Primary Key:
        TOPIC_KEY
    Foreign Key:
        FORUM_KEY: from curated
        SEASON_KEY: from curated
    Filter:
        Only new topics or topics which changed since last run
    Materialization:
        incremental to avoid removing old topics already in
*/

{{config(
    tags=['init_compet'],
    materialized="incremental",
    unique_key=['TOPIC_KEY']
)}}
-- we get last run timestamp if exists
{% if is_incremental() %}
    {% set results = run_query("SELECT MAX(UPDATED_AT_UTC) as max_updated_at FROM " ~ this) %}
    {% set max_updated_at = results.columns[0].values()[0] if results.columns[0].values() else none %}
{% endif %}
--we retrieve topics
with final_topic as (
SELECT
    topic.TOPIC_KEY,
    topic.FORUM_KEY,
    topic.SEASON_KEY,
    topic.FORUM_SOURCE,
    topic.TOPIC_NUMBER,
    topic.IS_FOR_PREDICT,
    topic.IS_FOR_RESULT,
    topic.MESSAGE_NUMBER_TO_EDIT   
FROM
    {{ref('curated_topic')}} topic
    {% if is_incremental() and max_updated_at is not none %}
    WHERE topic.UPDATED_AT_UTC > '{{ max_updated_at }}'
    {% endif %}
)
SELECT
    final_topic.TOPIC_KEY,
    final_topic.FORUM_KEY,
    final_topic.SEASON_KEY,
    final_topic.FORUM_SOURCE,
    final_topic.TOPIC_NUMBER,
    final_topic.IS_FOR_PREDICT,
    final_topic.IS_FOR_RESULT,
    final_topic.MESSAGE_NUMBER_TO_EDIT,
    {{updated_at_fields()}}
FROM
    final_topic
    {{updated_at_table_join_forum('final_topic')}}