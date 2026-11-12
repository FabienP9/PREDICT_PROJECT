/* The purpose of this module is to define macros for the vw_calendar view repetitive code to read game */
{% macro updategames_gettime(datepart, value_shift,hourtime_utc)%}

    /*
        The purpose of this macro is to parametrize a cte for the view_calendar which is reused several times
        It calculates a shift in time for a game with DATEADD SQL function to calculate time for updating games
        Args:
            datepart: the type of DATEADD (DAY,WEEK,MONTH,YEAR)
            value_shift: the value we are shifting in date
            hourtime_utc: the utc time during the day we want to get
    */
    SELECT 
        'UPDATEGAMES' AS TASK_RUN,
        games.SEASON_KEY,
        games.COMPETITION_KEY,
        games.COMPETITION_SOURCE_ID,
        games.GAMEDAY,
        TO_TIMESTAMP(TO_DATE(DATEADD({{datepart}}, {{value_shift}}, games.GAME_DATE_UTC)) || ' ' || '{{hourtime_utc}}') AS TS_TASK_UTC,
        NULL AS TS_TASK_LOCAL,
        0 AS IS_TO_INIT,
        0 AS IS_TO_CALCULATE,
        0 AS IS_TO_CALCULATE_PARTIAL,
        0 AS IS_TO_DELETE,
        0 AS IS_TO_RECALCULATE,
        'AVOID' AS MESSAGE_ACTION,
        'RUN' AS GAME_ACTION
    FROM
        games

{% endmacro %}