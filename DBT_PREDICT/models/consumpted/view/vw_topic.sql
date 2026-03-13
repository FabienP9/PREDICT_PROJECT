/* 
    The purpose of this view is to display topics on a view.
    It will be used by the Python program to calculate the scope of topics from which to update messages
    Unique key:
        FORUM_SOURCE / TOPIC_NUMBER
*/
{{config(
    materialized="view"
)}}

SELECT
    season.SEASON_ID,
    forum.FORUM_SOURCE,
    forum.FORUM_COUNTRY,
	lt.TIMEZONE AS FORUM_TIMEZONE,
	topic.TOPIC_NUMBER,
    topic.IS_FOR_PREDICT,
    topic.IS_FOR_RESULT,
    topic.MESSAGE_NUMBER_TO_EDIT
FROM
    {{ref('consumpted_topic')}} topic
JOIN
    {{ref('consumpted_forum')}} forum
    ON topic.FORUM_KEY = forum.FORUM_KEY
JOIN
    {{ref('consumpted_season')}} season
    ON season.SEASON_KEY = topic.SEASON_KEY  
JOIN
    {{ ref('local_time')}} lt 
    ON lt.COUNTRY = forum.FORUM_COUNTRY