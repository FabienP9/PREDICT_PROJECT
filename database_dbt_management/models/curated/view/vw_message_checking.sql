/* 
    The purpose of this view is to display predictions from messages coming from "check" tables
    It displays them in a user-friendly format to check if prediction are well extracted from forums
    If it is ok the user can validate as an input of Python program in order to run calculations
    '1_GAME': Game prediction
    '2_BONUS': Bonus choices
    '3_TEAM': Team selection
*/
{{config(
    materialized="view"
)}}
-- we select all messages even if no predictions
with message_info as (
    SELECT
        season.SEASON_ID,
        topic.FORUM_SOURCE,
        CAST(topic.TOPIC_NUMBER AS STRING) AS TOPIC_NUMBER,
        CAST(message.MESSAGE_FORUM_ID AS STRING) AS MESSAGE_ID,
        user.USER_NAME,
        message.EDITION_TIME_UTC,
        message.EDITION_TIME_LOCAL AS EDITTIME,
        '0_INFO' AS TYPE_KEY,
        '' AS KEY,
        '' AS VALUE,
        '' AS COMES_FROM
    FROM
        {{ref('curated_message_check')}} message
    JOIN
        {{ref('curated_topic')}} topic
        ON topic.TOPIC_KEY = message.TOPIC_KEY
    JOIN
        {{ref('curated_user')}} user
        ON user.USER_KEY = message.USER_KEY
    JOIN
        {{ref('curated_season')}} season
        ON season.SEASON_KEY = message.SEASON_KEY
    WHERE 
        message.IS_MESSAGE_TO_PROCESS = 1
),
-- we select predictions on games
game_predict as (
    SELECT
        season.SEASON_ID,
        topic.FORUM_SOURCE,
        CAST(topic.TOPIC_NUMBER AS STRING) AS TOPIC_NUMBER,
        CAST(message.MESSAGE_FORUM_ID AS STRING) AS MESSAGE_ID,
        user.USER_NAME,
        message.EDITION_TIME_UTC,
        message.EDITION_TIME_LOCAL AS EDITTIME,
        '1_GAME' AS TYPE_KEY,
        GAME.GAME_MESSAGE AS KEY,
        CAST(pg.PREDICT AS STRING) AS VALUE,
        CASE
            WHEN pg.IS_PREDICTION_FROM_MESSAGE = 1 THEN 'MESSAGE'
            WHEN pg.IS_PROGRAM_REFINEMENT = 1 THEN 'CORRECTION_PROGRAM'
            ELSE 'CORRECTION'
        END AS COMES_FROM
        FROM
            {{ref('curated_message_check')}} message
        JOIN
            {{ref('curated_topic')}} topic
            ON topic.TOPIC_KEY = message.TOPIC_KEY
        JOIN
            {{ref('curated_user')}} user
            ON user.USER_KEY = message.USER_KEY
        JOIN
            {{ref('curated_predict_game_check')}} pg
            ON message.MESSAGE_EDITION_KEY = pg.MESSAGE_EDITION_KEY
        JOIN
            {{ref('curated_game')}} game
            ON game.GAME_KEY = pg.GAME_KEY
        JOIN
            {{ref('curated_season')}} season
            ON pg.SEASON_KEY = season.SEASON_KEY
        WHERE pg.IS_LAST_ONTIME = 1
),
-- we select bonus
game_bonus as (
    SELECT
        season.SEASON_ID,
        topic.FORUM_SOURCE,
        CAST(topic.TOPIC_NUMBER AS STRING) AS TOPIC_NUMBER,
        CAST(message.MESSAGE_FORUM_ID AS STRING) AS MESSAGE_ID,
        user.USER_NAME,
        message.EDITION_TIME_UTC,
        message.EDITION_TIME_LOCAL AS EDITTIME,
        '2_BONUS' AS TYPE_KEY,
        gameday.GAMEDAY_MESSAGE AS KEY,
        game.GAME_MESSAGE AS VALUE,
        CASE
            WHEN bonus.IS_PREDICTION_FROM_MESSAGE = 1 THEN 'MESSAGE'
            WHEN bonus.IS_PROGRAM_REFINEMENT = 1 THEN 'CORRECTION_PROGRAM'
            ELSE 'CORRECTION'
        END AS COMES_FROM
    FROM
        {{ref('curated_message_check')}} message
    JOIN
        {{ref('curated_topic')}} topic
        ON topic.TOPIC_KEY = message.TOPIC_KEY
    JOIN
        {{ref('curated_user')}} user
        ON user.USER_KEY = message.USER_KEY
    JOIN
        {{ref('curated_choice_bonus_check')}} bonus
        ON bonus.MESSAGE_EDITION_KEY = message.MESSAGE_EDITION_KEY
    JOIN
        {{ref('curated_game')}} game
        ON game.GAME_KEY = bonus.GAME_KEY
    JOIN
        {{ref('curated_gameday')}} gameday
        ON gameday.GAMEDAY_KEY = game.GAMEDAY_KEY
    JOIN
        {{ref('curated_season')}} season
        ON bonus.SEASON_KEY = season.SEASON_KEY
    WHERE bonus.IS_LAST_ONTIME = 1
),
-- we select team choices
team_choice as (
    SELECT
        season.SEASON_ID,
        topic.FORUM_SOURCE,
        CAST(topic.TOPIC_NUMBER AS STRING) AS TOPIC_NUMBER,
        CAST(message.MESSAGE_FORUM_ID AS STRING) AS MESSAGE_ID,
        user.USER_NAME,
        message.EDITION_TIME_UTC,
        message.EDITION_TIME_LOCAL AS EDITTIME,
        '3_TEAM' AS TYPE_KEY,
        'SEASON' AS KEY,
        team.TEAM_NAME AS VALUE,
        CASE
            WHEN teamc.IS_PREDICTION_FROM_MESSAGE = 1 THEN 'MESSAGE'
            WHEN teamc.IS_PROGRAM_REFINEMENT = 1 THEN 'CORRECTION_PROGRAM'
            ELSE 'CORRECTION'
        END AS COMES_FROM
    FROM
        {{ref('curated_message_check')}} message
    JOIN
        {{ref('curated_topic')}} topic
        ON topic.TOPIC_KEY = message.TOPIC_KEY
    JOIN
        {{ref('curated_user')}} user
        ON user.USER_KEY = message.USER_KEY
    JOIN
        {{ref('curated_choice_team_check')}} teamc
        ON teamc.MESSAGE_EDITION_KEY = message.MESSAGE_EDITION_KEY
    JOIN
        {{ref('curated_team')}} team
        ON team.TEAM_KEY = teamc.TEAM_KEY
    JOIN
        {{ref('curated_season')}} season
        ON teamc.SEASON_KEY = season.SEASON_KEY
    WHERE teamc.IS_ONTIME = 1
),
final as (
    SELECT * FROM message_info
    UNION ALL SELECT * FROM game_predict
    UNION ALL SELECT * FROM game_bonus
    UNION ALL SELECT * FROM team_choice
)
-- we retrieve everything in a user-friendly display
SELECT 
    FORUM_SOURCE,
    TOPIC_NUMBER,
    MESSAGE_ID,
    USER_NAME,
    EDITTIME,
    TYPE_KEY,
    KEY,
    VALUE,
    COMES_FROM,
    SEASON_ID,
    EDITION_TIME_UTC,
FROM
    final
ORDER BY 
    FORUM_SOURCE,
    TOPIC_NUMBER, 
    MESSAGE_ID, 
    EDITTIME, 
    TYPE_KEY, 
    KEY