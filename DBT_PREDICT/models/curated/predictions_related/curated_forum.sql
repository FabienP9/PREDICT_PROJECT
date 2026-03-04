/* 
    The purpose of this table is to extract all forum on which we display or retrieve prediction messages
    Inputs:
        landing_topic: we retrieve distinct forum from topics
    Primary Key:
        FORUM_KEY based on FORUM_SOURCE
    Filter:
        landing_topic is already filtered with topics we need to proceed
    Materialization:
        incremental to avoid removing old forums already in
*/
{{config(
    tags=['init_compet'],
    materialized="incremental",
    unique_key = ['FORUM_KEY']
)}}
-- we select distinct forums: there can be several topics on a same forum on landing_topic
with forum as (
    SELECT DISTINCT
        MD5('FORUM' || '^^' || topic.FORUM_SOURCE) AS FORUM_KEY,
        topic.FORUM_SOURCE,
        topic.FORUM_COUNTRY 
    FROM
        {{source("LAND",'TOPIC')}} topic
)
SELECT
    forum.FORUM_KEY,
    forum.FORUM_SOURCE,
    forum.FORUM_COUNTRY,
    {{updated_at_fields()}}
FROM 
    forum
LEFT JOIN
    {{ ref('local_time')}} lt 
    ON lt.COUNTRY = forum.FORUM_COUNTRY