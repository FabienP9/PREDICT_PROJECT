/* 
    The purpose of this table is to extract all topic on which we display or retrieve prediction messages, per season
    Inputs:
        landing_topic: from where we retrieve topics
    Joins:
        curated_forum: foreign key
    Primary Key:
        TOPIC_KEY based on FORUM_SOURCE and TOPIC_NUMBER
    Foreign key:
        FORUM_KEY: 1-n relationship with forum
        SEASON_KEY: 1-n relationship with season
    Filter:
        landing_topic is already filtered with topics we need to proceed
    Materialization:
        incremental to avoid removing old topics already in
*/

{{config(
    tags=['init_compet'],
    materialized="incremental",
    unique_key = ['TOPIC_KEY']
)}}

-- We retrieve topics
with final_topic as (
    SELECT 
        MD5('TOPIC' || '^^' || topic.FORUM_SOURCE || '^^' || topic.TOPIC_NUMBER) AS TOPIC_KEY,
        forum.FORUM_KEY,
        season.SEASON_KEY,
        topic.FORUM_SOURCE,
        topic.TOPIC_NUMBER,
        topic.IS_FOR_PREDICT,
        topic.IS_FOR_RESULT,
        topic.MESSAGE_NUMBER_TO_EDIT
    FROM {{source("LAND","TOPIC")}} topic
    LEFT JOIN {{ref('curated_season')}} season
        ON topic.SEASON_ID = season.SEASON_ID
    LEFT JOIN {{ref('curated_forum')}} forum
        ON topic.FORUM_SOURCE = forum.FORUM_SOURCE
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