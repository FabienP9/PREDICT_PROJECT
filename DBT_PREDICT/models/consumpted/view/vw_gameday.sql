/* 
    The purpose of this view is to display gamedays
    It is used for defining message scope of the Python program and to calculate output at the end of it.
*/
{{config(
    materialized="view"
)}}
with gameday as (
    SELECT
        gameday1.GAMEDAY,
        gameday1.NB_GAME+2 AS NB_PREDICTION, /* one bonus game per gameday = 3 prediction */
        gameday1.BEGIN_DATE_LOCAL,
        gameday1.BEGIN_TIME_LOCAL,
        gameday1.END_DATE_UTC,
        gameday1.END_TIME_UTC,
        gameday1.GAMEDAY_MESSAGE,
        gameday1.IS_CALCULATED,
        compet.COMPETITION_LABEL,
        season.SEASON_ID,
        season.SEASON_DIVISION,
        CASE
            WHEN season.SEASON_TEAMCHOICE_DEADLINE >= gameday1.BEGIN_DATE_LOCAL THEN 1
            ELSE 0
        END AS USER_CAN_CHOOSE_TEAM_FOR_PREDICTCHAMP,
        TO_VARCHAR(gameday1.END_DATE_LOCAL, 'MM') AS END_MONTH_LOCAL,
        TO_VARCHAR(gameday1.END_DATE_LOCAL, 'YYYY-MM') AS END_YEARMONTH_LOCAL,
        TO_VARCHAR(LEAD(gameday1.END_DATE_LOCAL) OVER (PARTITION BY gameday1.COMPETITION_KEY ORDER BY END_DATE_LOCAL), 'YYYY-MM') AS NEXT_END_YEARMONTH_LOCAL_COMPETITION,
        TO_VARCHAR(LEAD(gameday1.END_DATE_LOCAL) OVER (PARTITION BY gameday1.SEASON_KEY ORDER BY END_DATE_LOCAL), 'YYYY-MM') AS NEXT_END_YEARMONTH_LOCAL_SEASON
    FROM
        {{ref('consumpted_gameday')}} gameday1
    LEFT JOIN
        {{ref('consumpted_competition')}} compet
        ON compet.COMPETITION_KEY = gameday1.COMPETITION_KEY
    LEFT JOIN
        {{ref('consumpted_season')}} season
        ON gameday1.SEASON_KEY = season.SEASON_KEY    
)
SELECT
    gameday.GAMEDAY,
    gameday.NB_PREDICTION,
    gameday.BEGIN_DATE_LOCAL,
    gameday.BEGIN_TIME_LOCAL,
    gameday.END_DATE_UTC,
    gameday.END_TIME_UTC,
    gameday.GAMEDAY_MESSAGE,
    gameday.IS_CALCULATED,
    gameday.SEASON_ID,
    gameday.SEASON_DIVISION,
    gameday.USER_CAN_CHOOSE_TEAM_FOR_PREDICTCHAMP,
    gameday.END_MONTH_LOCAL,
    gameday.END_YEARMONTH_LOCAL,
    gameday.COMPETITION_LABEL,
    CASE
        WHEN NEXT_END_YEARMONTH_LOCAL_COMPETITION IS NULL THEN 1
        ELSE 0
    END AS DISPLAY_COMPET_MVP_RANKING,
    CASE
        WHEN NEXT_END_YEARMONTH_LOCAL_SEASON IS NULL THEN 1
        WHEN NEXT_END_YEARMONTH_LOCAL_SEASON != END_YEARMONTH_LOCAL THEN 1
        ELSE 0
    END AS DISPLAY_MONTH_MVP_RANKING    
FROM
    gameday