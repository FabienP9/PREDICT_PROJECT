/* 
    The purpose of this view is to display the calendar in real time, to get coming run actions
    The chosen time window is current date - 1week (if action are delayed because of bugs) 
    -> current date + 1month
*/
{{config(
    materialized="view"
)}}
-- we get the time when reading messages must be done
-- it is at every game start (only once even if there is several games starting same time)
with read_message as (
    SELECT DISTINCT
        'CHECK' AS TASK_RUN,
        game_readm.GAMEDAY_KEY,
        TO_TIMESTAMP(game_readm.DATE_GAME_UTC || ' ' || game_readm.TIME_GAME_UTC) AS TS_TASK_UTC,
        0 AS IS_TO_INIT,
        0 AS IS_TO_CALCULATE,
        0 AS IS_TO_DELETE,
        0 AS IS_TO_RECALCULATE,
        'CHECK' AS MESSAGE_ACTION,
        'AVOID' AS GAME_ACTION
    FROM 
        {{ref('consumpted_game')}} game_readm
    WHERE
        game_readm.TIME_GAME_UTC IS NOT NULL
),
-- we get the time when we should init the gameday
-- if exists a previous gameday in the season we init 15 mns after the beginning of previous gameday.
-- else we init one week before the beginning of the gameday
-- as some gameday begin same day, we focus on the min time of them, to define the init time the following gameday
init_gameday_begin AS (
    SELECT
        gameday.GAMEDAY_KEY,
        gameday.SEASON_KEY,
        TO_TIMESTAMP(gameday.BEGIN_DATE_UTC || ' ' || gameday.BEGIN_TIME_UTC) AS BEGIN_TS_UTC,
        gameday.BEGIN_DATE_UTC
    FROM {{ref('consumpted_gameday')}} gameday
    WHERE gameday.BEGIN_TIME_UTC IS NOT NULL
),
init_begin_min_per_day AS (
    SELECT
        init_gameday_begin.SEASON_KEY,
        init_gameday_begin.BEGIN_DATE_UTC,
        MIN(init_gameday_begin.BEGIN_TS_UTC) AS FIRST_TS_UTC_OF_DAY
    FROM init_gameday_begin
    GROUP BY init_gameday_begin.SEASON_KEY, init_gameday_begin.BEGIN_DATE_UTC
),
init_calculation_per_day AS (
    SELECT
        init_begin_min_per_day.SEASON_KEY,
        init_begin_min_per_day.BEGIN_DATE_UTC,
        CASE
            WHEN LAG(init_begin_min_per_day.FIRST_TS_UTC_OF_DAY)
                 OVER (PARTITION BY init_begin_min_per_day.SEASON_KEY 
                    ORDER BY init_begin_min_per_day.BEGIN_DATE_UTC) IS NULL
            THEN DATEADD(DAY, -7, init_begin_min_per_day.FIRST_TS_UTC_OF_DAY)
            ELSE DATEADD(MINUTE,15,LAG(init_begin_min_per_day.FIRST_TS_UTC_OF_DAY)
                OVER (PARTITION BY init_begin_min_per_day.SEASON_KEY 
                    ORDER BY init_begin_min_per_day.BEGIN_DATE_UTC)
            )
        END AS TS_TASK_UTC
    FROM init_begin_min_per_day
),
init_gameday as (
    SELECT
        'INIT' AS TASK_RUN,
        init_gameday_begin.GAMEDAY_KEY,
        init_calculation_per_day.TS_TASK_UTC,
        1 AS IS_TO_INIT,
        0 AS IS_TO_CALCULATE,
        0 AS IS_TO_DELETE,
        0 AS IS_TO_RECALCULATE,
        'AVOID' AS MESSAGE_ACTION,
        'RUN' AS GAME_ACTION
    FROM 
        init_gameday_begin
    JOIN 
        init_calculation_per_day
    ON 
        init_gameday_begin.SEASON_KEY = init_calculation_per_day.SEASON_KEY
        AND init_gameday_begin.BEGIN_DATE_UTC = init_calculation_per_day.BEGIN_DATE_UTC
),
-- we get the time when we should calculate the result of gameday
-- 2h after the begin of the last game of the gameday
calculate_gameday as (
    SELECT
        'CALCULATE' AS TASK_RUN,
        gameday_calculate.GAMEDAY_KEY,
        DATEADD(HOUR, 2, TO_TIMESTAMP(gameday_calculate.END_DATE_UTC || ' ' || gameday_calculate.END_TIME_UTC)) AS TS_TASK_UTC,
        0 AS IS_TO_INIT,
        1 AS IS_TO_CALCULATE,
        0 AS IS_TO_DELETE,
        0 AS IS_TO_RECALCULATE,
        'RUN' AS MESSAGE_ACTION,
        'RUN' AS GAME_ACTION
        FROM
            {{ref('consumpted_gameday')}} gameday_calculate     
        WHERE
            gameday_calculate.END_TIME_UTC IS NOT NULL
),
-- the timetable of games can change, so we need to read them regularly to be sure we have the updated timetable
-- 10 weeks before / 6 weeks before / 1 month before / 3 weeks before / and 10 days before the expected beginning
-- we read them around 8 AM UTC each time
read_gameday as (
    {{calendar_read_gameday('WEEK',-10,'08:01:00.000')}}
    UNION ALL {{calendar_read_gameday('WEEK',-6,'08:02:00.000')}}
    UNION ALL {{calendar_read_gameday('MONTH',-1,'08:03:00.000')}}
    UNION ALL {{calendar_read_gameday('WEEK',-3,'08:04:00.000')}}
    UNION ALL {{calendar_read_gameday('DAY',-10,'08:05:00.000')}}
),
read_gameday_info as (
    SELECT
        'UPDATEGAMES' AS TASK_RUN,
        gameday_readg.GAMEDAY_KEY,
        gameday_readg.TS_TASK_UTC,
        0 AS IS_TO_INIT,
        0 AS IS_TO_CALCULATE,
        0 AS IS_TO_DELETE,
        0 AS IS_TO_RECALCULATE,
        'AVOID' AS MESSAGE_ACTION,
        'RUN' AS GAME_ACTION
    FROM
        read_gameday gameday_readg
),
-- we union everything and filter on the time range
cte_union as (
    SELECT * FROM (
        SELECT * FROM read_message
        UNION ALL SELECT * FROM init_gameday
        UNION ALL SELECT * FROM calculate_gameday
        UNION ALL SELECT * FROM read_gameday_info
    )
    WHERE 
        DATEADD(WEEK,-1,current_date) <= TS_TASK_UTC
        AND DATEADD(MONTH,1,current_date) >= TS_TASK_UTC
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
        gameday.GAMEDAY,
        cte.TS_TASK_UTC, 
        convert_timezone('UTC', lt.TIMEZONE, cte.TS_TASK_UTC) as TS_TASK_LOCAL,
        cte.IS_TO_INIT,
        cte.IS_TO_CALCULATE,
        cte.IS_TO_DELETE,
        cte.IS_TO_RECALCULATE,
        cte.MESSAGE_ACTION,
        cte.GAME_ACTION
    FROM
        cte_union cte
    JOIN
        {{ref('consumpted_gameday')}} gameday
        ON gameday.GAMEDAY_KEY = cte.GAMEDAY_KEY
    JOIN
        {{ref('consumpted_season')}} season
        ON gameday.SEASON_KEY = season.SEASON_KEY
    JOIN
        {{ref('consumpted_competition')}} compet
        ON gameday.COMPETITION_KEY = compet.COMPETITION_KEY
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
    calendar.GAMEDAY,
    calendar.TS_TASK_UTC, 
    calendar.TS_TASK_LOCAL,
    calendar.IS_TO_INIT,
    calendar.IS_TO_CALCULATE,
    calendar.IS_TO_DELETE,
    calendar.IS_TO_RECALCULATE,
    calendar.MESSAGE_ACTION,
    calendar.GAME_ACTION
FROM
    calendar
ORDER BY TS_TASK_UTC