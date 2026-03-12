/* 
    The purpose of this table is to extract all messages from forums with their history.
    Indeed, they can be edited by the user.    
    Inputs:
        landing_message_check: stored messages
    Joins:
        curated_forum: Foreign key
        curated_topic: Foreign key
        curated_user: Foreign key
    Primary Key:
        For the snapshot definition: MESSAGE_KEY based on FORUM_SOURCE, and MESSAGE_FORUM_ID
        For a row: MESSAGE_EDITION_KEY based on FORUM_SOURCE, and MESSAGE_FORUM_ID and EDITION_TIME_UTC
    Foreign key:
        FORUM_KEY: 1-n relationship with forum
        TOPIC_KEY: 1-n relationship with topic
        SEASON_KEY: 1-n relationship with season
        USER_KEY: 1-n relationship with user
    Filter:
        landing_message_check is already filtered on messages we need to process
    Materialization:
        Snapshot to get the history of messages - based on the changing EDITION_TIME_UTC
*/

{%snapshot curated_message_check %}

{{config(
    tags=['main'],
    schema = 'CURATED',
    unique_key = 'MESSAGE_KEY',
    strategy = 'timestamp',
    updated_at = 'EDITION_TIME_UTC'
)}}
-- we extract messages from landing_message_check
with message as (
    SELECT
        message_check.FORUM_SOURCE,
        message_check.TOPIC_NUMBER,
        message_check.USER,
        message_check.MESSAGE_FORUM_ID,
        message_check.CREATION_TIME_LOCAL,
        -- if not edited yet, we take the creation time as edition time
        COALESCE(message_check.EDITION_TIME_LOCAL, message_check.CREATION_TIME_LOCAL) AS EDITION_TIME_LOCAL,
        message_check.MESSAGE_CONTENT
    FROM
        {{source('LAND','MESSAGE_CHECK')}} message_check
),
-- we get keys and convert message datetime to utc equivalent and define flags
message_with_key as (
    SELECT
        MD5 ('MESSAGE' || '^^' || message.FORUM_SOURCE || '^^' || message.MESSAGE_FORUM_ID ) AS MESSAGE_KEY,
        topic.FORUM_KEY,
        topic.TOPIC_KEY,
        topic.SEASON_KEY,
        user.USER_KEY,
        message.FORUM_SOURCE,
        message.MESSAGE_FORUM_ID,
        message.CREATION_TIME_LOCAL,
        message.EDITION_TIME_LOCAL,
        convert_timezone(lt.TIMEZONE, 'UTC',message.CREATION_TIME_LOCAL) AS CREATION_TIME_UTC,
        convert_timezone(lt.TIMEZONE, 'UTC',message.EDITION_TIME_LOCAL) AS EDITION_TIME_UTC,
        message.MESSAGE_CONTENT,
        topic.IS_FOR_PREDICT,
        topic.IS_FOR_RESULT,
        CASE 
            WHEN topic.IS_FOR_PREDICT = 1 AND STARTSWITH(message.MESSAGE_CONTENT,'+++++') THEN 1
            ELSE 0
        END AS IS_PROGRAM_INIT_MESSAGE,
        CASE 
            WHEN topic.IS_FOR_RESULT = 1 AND STARTSWITH(message.MESSAGE_CONTENT,'+++++') THEN 1
            ELSE 0
        END AS IS_PROGRAM_CALCUL_MESSAGE,
        CASE
            WHEN STARTSWITH(message.MESSAGE_CONTENT,'*****') THEN 1
            ELSE 0
        END AS IS_MESSAGE_TO_AVOID,
        CASE
            WHEN IS_PROGRAM_INIT_MESSAGE + IS_PROGRAM_CALCUL_MESSAGE + IS_MESSAGE_TO_AVOID > 0 THEN 0
            ELSE 1
        END AS IS_MESSAGE_TO_PROCESS
    FROM
        message
    LEFT JOIN
        {{ref('curated_topic')}} topic
        ON message.FORUM_SOURCE = topic.FORUM_SOURCE
        AND message.TOPIC_NUMBER = topic.TOPIC_NUMBER
    LEFT JOIN
        {{ref('curated_user')}} user
        ON message.USER = user.USER_NAME
        AND user.SEASON_KEY = topic.SEASON_KEY
    LEFT JOIN
        {{ref('curated_forum')}} forum
        ON forum.FORUM_KEY = topic.FORUM_KEY
    LEFT JOIN
        {{ ref('local_time')}} lt 
        ON forum.FORUM_COUNTRY = lt.COUNTRY
)
-- we retrieve messages
SELECT
    final_message.MESSAGE_KEY,
    MD5 ('MESSAGE_EDITION' || '^^' || final_message.FORUM_SOURCE || '^^' || 
        final_message.MESSAGE_FORUM_ID || '^^' || final_message.EDITION_TIME_UTC ) 
    AS MESSAGE_EDITION_KEY,
    final_message.FORUM_KEY,
    final_message.TOPIC_KEY,
    final_message.SEASON_KEY,
    final_message.USER_KEY,
    final_message.MESSAGE_FORUM_ID,
    final_message.CREATION_TIME_LOCAL,
    final_message.EDITION_TIME_LOCAL,
    final_message.CREATION_TIME_UTC,
    final_message.EDITION_TIME_UTC,
    final_message.MESSAGE_CONTENT,
    final_message.IS_PROGRAM_INIT_MESSAGE,
    final_message.IS_PROGRAM_CALCUL_MESSAGE,
    final_message.IS_MESSAGE_TO_AVOID,
    final_message.IS_MESSAGE_TO_PROCESS
FROM
    message_with_key final_message

{% endsnapshot %}