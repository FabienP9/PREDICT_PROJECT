'''The purpose of this file is to gather all sql queries used to generate output message'''

#constant
DATABASE = "#DATABASE#"

#Query to get gameday details for inited gameday message
VW_GAMEDAY_QUERY = f'''
    SELECT
        GAMEDAY,
        IS_CALCULATED,
        BEGIN_DATE_LOCAL,
        BEGIN_TIME_LOCAL,
        GAMEDAY_MESSAGE,
        SEASON_DIVISION,
        SEASON_ID,
        USER_CAN_CHOOSE_TEAM_FOR_PREDICTCHAMP,
        CONCAT('WEEKDAY_', DAYOFWEEK(BEGIN_DATE_LOCAL)) AS BEGIN_DATE_WEEKDAY,
        CONCAT('MONTH_', END_MONTH_LOCAL) AS END_MONTH_LOCAL,
        END_YEARMONTH_LOCAL,
        COMPETITION_LABEL,
        IS_SAME_FOR_PREDICTCHAMP,
        DISPLAY_COMPET_MVP_RANKING,
        DISPLAY_MONTH_MVP_RANKING
    FROM
        {DATABASE}.CONSUMPTED.VW_GAMEDAY   
    WHERE 
        SEASON_ID = %s
        AND GAMEDAY = %s
'''

#Query to get topics where to post the inited gameday message
VW_TOPIC_INIT_QUERY = f'''
    SELECT
        FORUM_SOURCE,
        FORUM_COUNTRY,
        FORUM_TIMEZONE,
        TOPIC_NUMBER,
        MESSAGE_NUMBER_TO_EDIT,
        IS_FOR_PREDICT,
        IS_FOR_RESULT
    FROM
        {DATABASE}.CONSUMPTED.VW_TOPIC
    WHERE
        IS_FOR_PREDICT = 1
        AND SEASON_ID = %s;
'''

#Query to get topics where to post the calculated gameday message
VW_TOPIC_CALCULATE_QUERY = f'''
    SELECT
        FORUM_SOURCE,
        FORUM_COUNTRY,
        FORUM_TIMEZONE,
        TOPIC_NUMBER,
        MESSAGE_NUMBER_TO_EDIT,
        IS_FOR_PREDICT,
        IS_FOR_RESULT
    FROM
        {DATABASE}.CONSUMPTED.VW_TOPIC
    WHERE
        IS_FOR_RESULT = 1
        AND SEASON_ID = %s;
'''

#Query to get topics where to list games
VW_GAME_QUERY = f'''
    SELECT
        GAMEDAY,
        GAMEDAY_MESSAGE,
        GAME_MESSAGE,
        GAME_MESSAGE_SHORT,
        TEAM_HOME_NAME,
        TEAM_AWAY_NAME,
        SCORE_HOME,
        SCORE_AWAY,
        RESULT,
        DATE_GAME_LOCAL,
        TIME_GAME_LOCAL
    FROM
        {DATABASE}.CONSUMPTED.VW_GAME
    WHERE
        SEASON_ID = %s
        AND GAMEDAY = %s
    ORDER BY GAME_MESSAGE;
'''

#Query to get remaining games for already started gameday (ie. opened) at a specific date
VW_GAME_OPENED_QUERY = f'''
        SELECT
            GAMEDAY_MESSAGE,
            GAMEDAY,
            GAME_MESSAGE,
            TEAM_HOME_NAME,
            TEAM_AWAY_NAME,
            DATE_GAME_LOCAL,
            TIME_GAME_LOCAL
        FROM 
            {DATABASE}.CONSUMPTED.VW_GAME
        WHERE
            SEASON_ID = %s
            AND GAMEDAY != %s
            AND GAMEDAY_BEGIN_DATE_UTC <= TO_DATE(%s,'YYYY-MM-DD')
            AND GAMEDAY_END_DATE_UTC > TO_DATE(%s,'YYYY-MM-DD')
            AND DATE_GAME_UTC > TO_DATE(%s,'YYYY-MM-DD')
        ORDER BY 
            GAME_MESSAGE;
    '''

#Query to get the calendar of next opening gamedays
VW_GAMEDAY_NEXTOPENING_QUERY = f'''
    SELECT 
        cal.GAMEDAY, 
        TO_DATE(cal.TS_TASK_LOCAL) AS DATE_TASK_LOCAL, 
        TO_TIME(cal.TS_TASK_LOCAL) AS TIME_TASK_LOCAL,  
        gameday.BEGIN_DATE_LOCAL, 
        gameday.BEGIN_TIME_LOCAL, 
        gameday.END_DATE_LOCAL,
        gameday.END_TIME_LOCAL
    FROM 
        {DATABASE}.CONSUMPTED.VW_CALENDAR cal
    JOIN 
        {DATABASE}.CONSUMPTED.VW_GAMEDAY gameday
        ON gameday.SEASON_ID = cal.SEASON_ID
        AND gameday.GAMEDAY = cal.GAMEDAY
    WHERE 
        cal.TASK_RUN = 'INIT'
        AND cal.TS_TASK_UTC >= TO_DATE(%s,'YYYY-MM-DD')
        AND cal.SEASON_ID = %s
    ORDER BY
        TS_TASK_LOCAL;
'''

#Query to get prediction and result per game and user
VW_PREDICT_GAME_QUERY = f'''
    SELECT
        *
    FROM
        {DATABASE}.CONSUMPTED.VW_PREDICT_GAME
    WHERE
        SEASON_ID = %s
        AND GAMEDAY = %s
    ORDER BY
        lower(USER_NAME)
    '''

#Query to get global (per season) result per user
VW_USER_SCORES_GLOBAL_QUERY = f'''
    SELECT
        USER_NAME,
        TOTAL_POINTS,
        CAST(CASE
            WHEN NB_GAMEDAY_PREDICT = 0 THEN 0
            ELSE TOTAL_POINTS / NB_TOTAL_PREDICT
        END AS DECIMAL(10,2)) AS AVERAGE_POINTS,
        NB_GAMEDAY_PREDICT,
        NB_GAMEDAY_FIRST,
        NB_TOTAL_PREDICT,
        RANK() OVER (ORDER BY TOTAL_POINTS DESC) AS RANK
    FROM
        {DATABASE}.CONSUMPTED.VW_USER_SCORES_GLOBAL
    WHERE
        SEASON_ID = %s
    '''

#Query to get result per gameday per user
VW_USER_SCORES_GAMEDAY_QUERY = f'''
    SELECT
        USER_NAME,
        GAMEDAY_POINTS,
        NB_PREDICTION_GAMEDAY,
        CAST(CASE
            WHEN NB_PREDICTION_GAMEDAY = 0 THEN 0
            ELSE GAMEDAY_POINTS / NB_PREDICTION_GAMEDAY
        END AS DECIMAL(10,2)) AS AVERAGE_POINTS,
        RANK() OVER (ORDER BY GAMEDAY_POINTS DESC) AS RANK
    FROM
        {DATABASE}.CONSUMPTED.VW_USER_SCORES_GAMEDAY
    WHERE
        SEASON_ID = %s
        AND GAMEDAY = %s
    '''

#Query to list of calculated gameday per season
VW_GAMEDAY_CALCULATED_QUERY = f'''
    SELECT
        GAMEDAY,
        NB_PREDICTION
    FROM
        {DATABASE}.CONSUMPTED.VW_GAMEDAY 
    WHERE
        SEASON_ID = %s
        AND IS_CALCULATED = 1
    ORDER BY
        END_DATE_LOCAL, END_TIME_LOCAL
    '''

#Query to get result for the prediction championship
VW_GAME_PREDICTCHAMP_QUERY = f'''
    SELECT
        GAME_KEY,
        GAME_MESSAGE_SHORT,
        TEAM_HOME_NAME,
        TEAM_AWAY_NAME,
        IS_FOR_RANK,
        HAS_HOME_ADV,
        POINTS_BONUS,
        POINTS_HOME,
        POINTS_AWAY,
        WINNER
    FROM
        {DATABASE}.CONSUMPTED.VW_GAME_PREDICTCHAMP
    WHERE
        SEASON_ID = %s
        AND GAMEDAY = %s
    ORDER BY
        GAME_MESSAGE_SHORT
    '''

#Query to get result detailed per user for the prediction championship
VW_GAME_PREDICTCHAMP_DETAILS_QUERY = f'''
    SELECT
        GAME_KEY,
        TEAM_NAME,
        USER_NAME,
        POINTS,
        RANK_USER_TEAM
    FROM
        {DATABASE}.CONSUMPTED.VW_GAME_PREDICTCHAMP_DETAILS
    WHERE
        SEASON_ID = %s
        AND GAMEDAY = %s
    ORDER BY
        RANK_USER_TEAM      
'''

#Query to get ranking for the prediction championship
VW_TEAM_SCORES_QUERY = f'''
    SELECT
        TEAM_NAME,
        WIN,
        LOSS,
        PERC_WIN,
        POINTS_PRO,
        POINTS_AGAINST,
        POINTS_DIFF,
        RANK() OVER (ORDER BY PERC_WIN DESC,POINTS_DIFF DESC) AS RANK
    FROM
        {DATABASE}.CONSUMPTED.VW_TEAM_SCORES
    WHERE
        SEASON_ID = %s
'''

#Query to get correction related to predictions per user
VW_CORRECTION_QUERY = f'''
    SELECT
        USER_NAME,
        PREDICT_ID
    FROM
        {DATABASE}.CONSUMPTED.VW_CORRECTION
    WHERE
        SEASON_ID = %s
        AND GAMEDAY = %s   
    ORDER BY
        USER_NAME,
        PREDICT_ID

'''

#Query to get users figure for month MVP race
VW_USER_SCORES_GAMEDAY_MVPMONTH_QUERY = f'''
    SELECT 
        USER_NAME,
        SUM(GAMEDAY_POINTS) AS POINTS,
        SUM(WIN) AS WIN,
        SUM(LOSS) AS LOSS,
        LISTAGG(DISTINCT TEAM_NAME, ', ') WITHIN GROUP (ORDER BY TEAM_NAME) AS LIST_TEAMS
    FROM 
        {DATABASE}.CONSUMPTED.VW_USER_SCORES_GAMEDAY
    WHERE
        SEASON_ID = %s
        AND END_YEARMONTH_LOCAL = %s
    GROUP BY    
        USER_NAME
    ORDER BY 
        POINTS DESC, 
        USER_NAME
'''

#Query to list gameday related to the month MVP race
VW_GAMEDAY_CALCULATED_MVPMONTH_QUERY = f'''
    SELECT
        GAMEDAY,
        NB_PREDICTION
    FROM
        {DATABASE}.CONSUMPTED.VW_GAMEDAY 
    WHERE
        SEASON_ID = %s
        AND END_YEARMONTH_LOCAL = %s
    ORDER BY
        END_DATE_LOCAL, END_TIME_LOCAL
    '''


#Query to get users figure for competition MVP race
VW_USER_SCORES_GAMEDAY_MVPCOMPET_QUERY = f'''
    SELECT 
        USER_NAME,
        SUM(GAMEDAY_POINTS) AS POINTS,
        SUM(WIN) AS WIN,
        SUM(LOSS) AS LOSS,
        LISTAGG(DISTINCT TEAM_NAME, ', ') WITHIN GROUP (ORDER BY TEAM_NAME) AS LIST_TEAMS
    FROM 
        {DATABASE}.CONSUMPTED.VW_USER_SCORES_GAMEDAY
    WHERE
        SEASON_ID = %s
        AND COMPETITION_LABEL = %s
    GROUP BY    
        USER_NAME
    ORDER BY 
        POINTS DESC, 
        USER_NAME
'''

VW_GAMEDAY_CALCULATED_MVPCOMPET_QUERY = f'''
    SELECT
        GAMEDAY,
        NB_PREDICTION
    FROM
        {DATABASE}.CONSUMPTED.VW_GAMEDAY 
    WHERE
        SEASON_ID = %s
        AND COMPETITION_LABEL = %s
    ORDER BY
        END_DATE_LOCAL, END_TIME_LOCAL
    '''