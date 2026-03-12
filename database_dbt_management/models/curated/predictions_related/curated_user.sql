/* 
    The purpose of this table is to extract all users per season who wrote message on forums    
    Inputs:
        landing_message_check: stored messages
    Joins:
        curated_season: foreign key
        curated_topic: to get curated_season
    Primary Key:
        USER_KEY based on USER and SEASON_ID
    Foreign key:
        SEASON_KEY: 1-n relationship with season - a user is defined for a season
    Filter:
        landing_message_check is already filtered with messages/users we need to proceed
    Materialization:
        incremental to avoid removing old user already in
*/

{{config(
    tags=['main'],
    materialized="incremental",
    unique_key = ['USER_KEY']
)}}
--we get distinct users from message
with message as (
    SELECT DISTINCT
        message_check.USER,
        message_check.FORUM_SOURCE,
        message_check.TOPIC_NUMBER
    FROM {{source("LAND","MESSAGE_CHECK")}} message_check
),
-- we retrieve distinct users per season
final_user as (
    SELECT DISTINCT
        MD5('USER' || '^^' || message.USER || '^^' || season.SEASON_ID) AS USER_KEY,
        message.USER AS USER_NAME,
        season.SEASON_KEY
    FROM
        message
    LEFT JOIN
        {{ref('curated_topic')}} topic
        ON message.FORUM_SOURCE = topic.FORUM_SOURCE
        AND message.TOPIC_NUMBER = topic.TOPIC_NUMBER
    LEFT JOIN
        {{ref('curated_season')}} season
        ON topic.SEASON_KEY = season.SEASON_KEY
)
SELECT
    final_user.USER_KEY,
    final_user.USER_NAME,
    final_user.SEASON_KEY,
    {{updated_at_fields()}}
FROM 
    final_user
    {{updated_at_table_join_season('final_user')}}