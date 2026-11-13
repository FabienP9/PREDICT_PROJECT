/* 
    The purpose of this view is to display the calendar in real time, to get coming run actions
    The chosen time window is current date - 1month (if action are delayed because of bugs) 
    -> current date + 1month
*/
{{config(
    materialized="view"
)}}
with gamedays as (
    SELECT
        gameday.SEASON_KEY,
        gameday.COMPETITION_KEY,
        gameday.COMPETITION_SOURCE_ID,
        gameday.GAMEDAY,
        gameday.BEGIN_DATE_UTC AS GAMEDAY_BEGIN_DATE_UTC,
        TO_TIMESTAMP(gameday.BEGIN_DATE_UTC || ' ' || gameday.BEGIN_TIME_UTC) AS GAMEDAY_BEGIN_DATETIME_UTC, 
        gameday.END_DATE_UTC AS GAMEDAY_END_DATE_UTC,
        TO_TIMESTAMP(gameday.END_DATE_UTC || ' ' || gameday.END_TIME_UTC) AS GAMEDAY_END_DATETIME_UTC
    FROM
        {{ref('consumpted_gameday')}} gameday

),
games as (
    SELECT DISTINCT
        game.SEASON_KEY,
        game.COMPETITION_KEY,
        gameday.COMPETITION_SOURCE_ID,
        gameday.GAMEDAY,
        game.DATE_GAME_UTC AS GAME_DATE_UTC,
        game.DATE_GAME_LOCAL AS GAME_DATE_LOCAL,
        TO_TIMESTAMP(game.DATE_GAME_UTC || ' ' || game.TIME_GAME_UTC) AS GAME_DATETIME_UTC,
        CASE
            WHEN game.DATE_GAME_UTC = gameday.END_DATE_UTC AND game.TIME_GAME_UTC = gameday.END_TIME_UTC THEN 1
            ELSE 0
        END AS IS_LAST_IN_GAMEDAY_TIME,
        CASE
            WHEN game.DATE_GAME_UTC = gameday.END_DATE_UTC THEN 1
            ELSE 0
        END AS IS_LAST_IN_GAMEDAY_DATE        
    FROM
        {{ref('consumpted_game')}} game
    JOIN
        {{ref('consumpted_gameday')}} gameday
        ON game.GAMEDAY_KEY = gameday.GAMEDAY_KEY
),
-- focusing on CHECK task: at every gametime
check_task as (
    SELECT
        'CHECK' AS TASK_RUN,
        games.SEASON_KEY,
        games.COMPETITION_KEY,
        games.COMPETITION_SOURCE_ID,
        games.GAMEDAY,
        games.GAME_DATETIME_UTC AS TS_TASK_UTC,
        NULL AS TS_TASK_LOCAL,
        0 AS IS_TO_INIT,
        0 AS IS_TO_CALCULATE,
        0 AS IS_TO_CALCULATE_PARTIAL,
        0 AS IS_TO_DELETE,
        0 AS IS_TO_RECALCULATE,
        'CHECK' AS MESSAGE_ACTION,
        'AVOID' AS GAME_ACTION
    FROM
        games
),
-- focus on updategames task: several times before games
updategames_task as(
    {{updategames_gettime('DAY',0,'00:00:00.000')}}
    UNION ALL {{updategames_gettime('DAY',-1,'00:30:00.000')}}
    UNION ALL {{updategames_gettime('DAY',-3,'01:00:00.000')}}
    UNION ALL {{updategames_gettime('DAY',-6,'01:30:00.000')}}
    UNION ALL {{updategames_gettime('DAY',-10,'02:00:00.000')}}
    UNION ALL {{updategames_gettime('DAY',-15,'02:30:00.000')}}
    UNION ALL {{updategames_gettime('DAY',-21,'03:00:00.000')}}
    UNION ALL {{updategames_gettime('DAY',-28,'03:30:00.000')}}
    UNION ALL {{updategames_gettime('DAY',-36,'04:00:00.000')}}
    UNION ALL {{updategames_gettime('DAY',-45,'04:30:00.000')}}
    UNION ALL {{updategames_gettime('DAY',-55,'05:00:00.000')}}
    UNION ALL {{updategames_gettime('DAY',-66,'05:30:00.000')}}
    UNION ALL {{updategames_gettime('DAY',-78,'06:00:00.000')}}
    UNION ALL {{updategames_gettime('DAY',-91,'06:30:00.000')}}
    UNION ALL {{updategames_gettime('DAY',-105,'07:00:00.000')}}
    UNION ALL {{updategames_gettime('DAY',-120,'07:30:00.000')}}
    UNION ALL {{updategames_gettime('DAY',-136,'08:00:00.000')}}
    UNION ALL {{updategames_gettime('DAY',-153,'08:30:00.000')}}
    UNION ALL {{updategames_gettime('DAY',-171,'09:00:00.000')}}
    UNION ALL {{updategames_gettime('DAY',-190,'09:30:00.000')}}
),-- focus on calculate task: 105 minutes after gameday end date
calculate_task as (
    SELECT
        'CALCULATE' AS TASK_RUN,
        gamedays.SEASON_KEY,
        gamedays.COMPETITION_KEY,
        gamedays.COMPETITION_SOURCE_ID,
        gamedays.GAMEDAY,
        DATEADD(MINUTE, 105, gamedays.GAMEDAY_END_DATETIME_UTC) AS TS_TASK_UTC,
        NULL AS TS_TASK_LOCAL,
        0 AS IS_TO_INIT,
        1 AS IS_TO_CALCULATE,
        0 AS IS_TO_CALCULATE_PARTIAL,
        0 AS IS_TO_DELETE,
        0 AS IS_TO_RECALCULATE,
        'RUN' AS MESSAGE_ACTION,
        'RUN' AS GAME_ACTION
    FROM
        gamedays
),
-- focus on calculate partial task
-- 105 minutes after gametime
-- and at 23:30 every gameday
-- except for the last one of the gameday locally
calculate_partial_time as (
    SELECT
        'CALCULATE_PARTIAL' AS TASK_RUN,
        games.SEASON_KEY,
        games.COMPETITION_KEY,
        games.COMPETITION_SOURCE_ID,
        games.GAMEDAY,
        DATEADD(MINUTE, 105, games.GAME_DATETIME_UTC) AS TS_TASK_UTC,
        NULL AS TS_TASK_LOCAL,
        0 AS IS_TO_INIT,
        0 AS IS_TO_CALCULATE,
        1 AS IS_TO_CALCULATE_PARTIAL,
        0 AS IS_TO_DELETE,
        0 AS IS_TO_RECALCULATE,
        'RUN' AS MESSAGE_ACTION,
        'RUN' AS GAME_ACTION
    FROM
        games
    WHERE
        games.IS_LAST_IN_GAMEDAY_TIME = 0
),
calculate_partial_date as (
    SELECT DISTINCT
        'CALCULATE_PARTIAL' AS TASK_RUN,
        games.SEASON_KEY,
        games.COMPETITION_KEY,
        games.COMPETITION_SOURCE_ID,
        games.GAMEDAY,
        NULL AS TS_TASK_UTC,
        TO_TIMESTAMP(games.GAME_DATE_LOCAL || ' 23:30:00.000') AS TS_TASK_LOCAL,
        0 AS IS_TO_INIT,
        0 AS IS_TO_CALCULATE,
        1 AS IS_TO_CALCULATE_PARTIAL,
        0 AS IS_TO_DELETE,
        0 AS IS_TO_RECALCULATE,
        'RUN' AS MESSAGE_ACTION,
        'RUN' AS GAME_ACTION
    FROM
        games
    WHERE
        games.IS_LAST_IN_GAMEDAY_DATE = 0
),
calculate_partial_task as (
    SELECT * FROM calculate_partial_time
    UNION ALL
    SELECT * FROM calculate_partial_date
),
-- focus on init task
-- if exists a previous gameday GP in the season, beginning is max 10 days before the beginning of G
--    we init G 15 mns after the beginning of GP if this is less than 10 days before the first game of G
--    else we init 10 days before the first game of G
-- if several gameday G1 and G2 begin same day:
--    we group them so that they share the same previous gameday GP
--    the init time of the following gameday GF will be 15 mns after the min beginning between G1 and G2
init_gameday_group AS (
    SELECT
        gamedays.SEASON_KEY,
        gamedays.GAMEDAY_BEGIN_DATE_UTC,
        MIN(gamedays.GAMEDAY_BEGIN_DATETIME_UTC) AS GROUP_GAMEDAY_BEGIN_DATETIME_UTC
    FROM gamedays
    GROUP BY 
    gamedays.SEASON_KEY,
    gamedays.GAMEDAY_BEGIN_DATE_UTC
),
init_group_with_previous AS (
    SELECT
        init_gameday_group.SEASON_KEY,
        init_gameday_group.GAMEDAY_BEGIN_DATE_UTC,
        init_gameday_group.GROUP_GAMEDAY_BEGIN_DATETIME_UTC,
        LAG(init_gameday_group.GROUP_GAMEDAY_BEGIN_DATETIME_UTC)
            OVER (
                PARTITION BY init_gameday_group.SEASON_KEY 
                ORDER BY init_gameday_group.GROUP_GAMEDAY_BEGIN_DATETIME_UTC)
        AS PREV_GROUP_GAMEDAY_BEGIN_DATETIME_UTC
    FROM init_gameday_group
),
init_calculation_per_group AS (
    SELECT
        init_group_with_previous.SEASON_KEY,
        init_group_with_previous.GAMEDAY_BEGIN_DATE_UTC,
        CASE
            WHEN 
                init_group_with_previous.PREV_GROUP_GAMEDAY_BEGIN_DATETIME_UTC IS NOT NULL
                AND init_group_with_previous.PREV_GROUP_GAMEDAY_BEGIN_DATETIME_UTC >= DATEADD(DAY, -10, init_group_with_previous.GROUP_GAMEDAY_BEGIN_DATETIME_UTC)
            THEN DATEADD(MINUTE, 15, init_group_with_previous.PREV_GROUP_GAMEDAY_BEGIN_DATETIME_UTC)
            ELSE DATEADD(DAY, -10, init_group_with_previous.GROUP_GAMEDAY_BEGIN_DATETIME_UTC)
        END AS TS_TASK_UTC
    FROM init_group_with_previous
),
init_task as (
    SELECT
        'INIT' AS TASK_RUN,
        gamedays.SEASON_KEY,
        gamedays.COMPETITION_KEY,
        gamedays.COMPETITION_SOURCE_ID,
        gamedays.GAMEDAY,
        init_calculation_per_group.TS_TASK_UTC,
        NULL AS TS_TASK_LOCAL,
        1 AS IS_TO_INIT,
        0 AS IS_TO_CALCULATE,
        0 AS IS_TO_CALCULATE_PARTIAL,
        0 AS IS_TO_DELETE,
        0 AS IS_TO_RECALCULATE,
        'AVOID' AS MESSAGE_ACTION,
        'RUN' AS GAME_ACTION
    FROM 
        gamedays
    JOIN 
        init_calculation_per_group
    ON 
        gamedays.SEASON_KEY = init_calculation_per_group.SEASON_KEY
        AND gamedays.GAMEDAY_BEGIN_DATE_UTC = init_calculation_per_group.GAMEDAY_BEGIN_DATE_UTC
),
-- we union everything and filter on the time range
cte_union as (
    SELECT * FROM (
        SELECT * FROM check_task
        UNION ALL SELECT * FROM updategames_task
        UNION ALL SELECT * FROM calculate_task
        UNION ALL SELECT * FROM calculate_partial_task
        UNION ALL SELECT * FROM init_task
    )
    WHERE 
        DATEADD(MONTH,-1,current_date) <= COALESCE(TS_TASK_UTC,TS_TASK_LOCAL)
        AND DATEADD(MONTH,1,current_date) >= COALESCE(TS_TASK_UTC,TS_TASK_LOCAL)
),
-- we retrieve the calendar with the calculation of local time
calendar as (
    SELECT DISTINCT
        cte.TASK_RUN,
        season.SEASON_ID,
        season.SEASON_SPORT,
        season.SEASON_COUNTRY,
        season.SEASON_NAME,
        season.SEASON_DIVISION,
        compet.COMPETITION_ID,
        cte.COMPETITION_SOURCE_ID,
        cte.GAMEDAY,
        CASE
            WHEN cte.TS_TASK_UTC IS NULL THEN convert_timezone(lt.TIMEZONE, 'UTC', cte.TS_TASK_LOCAL)
            ELSE cte.TS_TASK_UTC
        END AS TS_TASK_UTC, 
        CASE
            WHEN cte.TS_TASK_LOCAL IS NULL THEN convert_timezone('UTC', lt.TIMEZONE, cte.TS_TASK_UTC)
            ELSE cte.TS_TASK_LOCAL
        END AS TS_TASK_LOCAL,
        cte.IS_TO_INIT,
        cte.IS_TO_CALCULATE,
        cte.IS_TO_CALCULATE_PARTIAL,
        cte.IS_TO_DELETE,
        cte.IS_TO_RECALCULATE,
        cte.MESSAGE_ACTION,
        cte.GAME_ACTION
    FROM
        cte_union cte
    JOIN
        {{ref('consumpted_season')}} season
        ON cte.SEASON_KEY = season.SEASON_KEY
    JOIN
        {{ref('consumpted_competition')}} compet
        ON cte.COMPETITION_KEY = compet.COMPETITION_KEY
    LEFT JOIN
        {{ ref('local_time')}} lt 
        ON lt.COUNTRY = season.SEASON_COUNTRY
)
SELECT
    calendar.TASK_RUN,
    calendar.SEASON_ID,
    calendar.SEASON_SPORT,
    calendar.SEASON_COUNTRY,
    calendar.SEASON_NAME,
    calendar.SEASON_DIVISION,
    calendar.COMPETITION_ID,
    calendar.COMPETITION_SOURCE_ID,
    calendar.GAMEDAY,
    calendar.TS_TASK_UTC, 
    calendar.TS_TASK_LOCAL,
    calendar.IS_TO_INIT,
    calendar.IS_TO_CALCULATE,
    calendar.IS_TO_CALCULATE_PARTIAL,
    calendar.IS_TO_DELETE,
    calendar.IS_TO_RECALCULATE,
    calendar.MESSAGE_ACTION,
    calendar.GAME_ACTION
FROM
    calendar
ORDER BY TS_TASK_UTC