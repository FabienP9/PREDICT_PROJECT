/* The purpose of this module is to define macros for the vw_calendar view repetitive code to read gamedays */
{% macro calendar_read_gameday(datepart, value_shift,hourtime_utc)%}

    /*
        The purpose of this macro is to parametrize a cte for the view_calendar which is reused several times
        It calculates a shift in time for a gameday with DATEADD SQL function
        Args:
            datepart: the type of DATEADD (DAY,WEEK,MONTH,YEAR)
            value_shift: the value we are shifting in date
            hourtime_utc: the utc time during the day we want to get
    */
    SELECT 
        gameday_read.GAMEDAY_KEY,
        TO_TIMESTAMP(TO_DATE(DATEADD({{datepart}}, {{value_shift}}, gameday_read.BEGIN_DATE_UTC)) || ' ' || '{{hourtime_utc}}') AS TS_TASK_UTC
    FROM
        {{ref('consumpted_gameday')}} gameday_read
    WHERE
        gameday_read.BEGIN_TIME_UTC IS NOT NULL

{% endmacro %}