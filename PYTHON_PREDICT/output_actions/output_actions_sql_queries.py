'''The purpose of this file is to gather all sql queries used to generate output message'''

#Query to get gameday details for inited gameday message
qGamedayOutput = f'''
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
        DISPLAY_COMPET_MVP_RANKING,
        DISPLAY_MONTH_MVP_RANKING
    FROM
        #DATABASE#.CONSUMPTED.VW_GAMEDAY   
    WHERE 
        SEASON_ID = %s
        AND GAMEDAY = %s
'''

#Query to get topics where to post the inited gameday message
qTopics_Init = f'''
    SELECT
        FORUM_SOURCE,
        FORUM_COUNTRY,
        FORUM_TIMEZONE,
        TOPIC_NUMBER,
        MESSAGE_NUMBER_TO_EDIT,
        IS_FOR_PREDICT,
        IS_FOR_RESULT
    FROM
        #DATABASE#.CONSUMPTED.VW_TOPIC
    WHERE
        IS_FOR_PREDICT = 1
        AND SEASON_ID = %s;
'''

#Query to get topics where to post the calculated gameday message
qTopics_Calculate = f'''
    SELECT
        FORUM_SOURCE,
        FORUM_COUNTRY,
        FORUM_TIMEZONE,
        TOPIC_NUMBER,
        MESSAGE_NUMBER_TO_EDIT,
        IS_FOR_PREDICT,
        IS_FOR_RESULT
    FROM
        #DATABASE#.CONSUMPTED.VW_TOPIC
    WHERE
        IS_FOR_RESULT = 1
        AND SEASON_ID = %s;
'''

#Query to get topics where to list games
qGame = f'''
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
        #DATABASE#.CONSUMPTED.VW_GAME
    WHERE
        SEASON_ID = %s
        AND GAMEDAY = %s
    ORDER BY GAME_MESSAGE;
'''

#Query to get remaining games for already started gameday at a specific date
qGame_Remaining_AtDate = f'''
        SELECT
            GAMEDAY_MESSAGE,
            GAMEDAY,
            GAME_MESSAGE,
            TEAM_HOME_NAME,
            TEAM_AWAY_NAME,
            DATE_GAME_LOCAL,
            TIME_GAME_LOCAL
        FROM 
            #DATABASE#.CONSUMPTED.VW_GAME
        WHERE
            SEASON_ID = %s
            AND GAMEDAY != %s
            AND GAMEDAY_BEGIN_DATE_UTC <= TO_DATE(%s,'YYYY-MM-DD')
            AND GAMEDAY_END_DATE_UTC > TO_DATE(%s,'YYYY-MM-DD')
            AND DATE_GAME_UTC > TO_DATE(%s,'YYYY-MM-DD')
        ORDER BY 
            GAME_MESSAGE;
    '''

#Query to get prediction and result per game and user
qPredictGame = f'''
    SELECT
        *
    FROM
        #DATABASE#.CONSUMPTED.VW_PREDICT_GAME
    WHERE
        SEASON_ID = %s
        AND GAMEDAY = %s
    ORDER BY
        lower(USER_NAME)
    '''

#Query to get global (per season) result per user
qUserScores_Global = f'''
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
        #DATABASE#.CONSUMPTED.VW_USER_SCORES_GLOBAL
    WHERE
        SEASON_ID = %s
    '''

#Query to get result per gameday per user
qUserScores_Gameday = f'''
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
        #DATABASE#.CONSUMPTED.VW_USER_SCORES_GAMEDAY
    WHERE
        SEASON_ID = %s
        AND GAMEDAY = %s
    '''

#Query to list of calculated gameday per season
qList_Gameday_Calculated = f'''
    SELECT
        GAMEDAY,
        NB_PREDICTION
    FROM
        #DATABASE#.CONSUMPTED.VW_GAMEDAY 
    WHERE
        SEASON_ID = %s
        AND IS_CALCULATED = 1
    ORDER BY
        BEGIN_DATE_LOCAL, BEGIN_TIME_LOCAL
    '''

#Query to get result for the prediction championship
qGamePredictchamp = f'''
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
        #DATABASE#.CONSUMPTED.VW_GAME_PREDICTCHAMP
    WHERE
        SEASON_ID = %s
        AND GAMEDAY = %s
    ORDER BY
        GAME_MESSAGE_SHORT
    '''

#Query to get result detailed per user for the prediction championship
qGamePredictchampDetail = f'''
    SELECT
        GAME_KEY,
        TEAM_NAME,
        USER_NAME,
        POINTS,
        RANK_USER_TEAM
    FROM
        #DATABASE#.CONSUMPTED.VW_GAME_PREDICTCHAMP_DETAILS
    WHERE
        SEASON_ID = %s
        AND GAMEDAY = %s
    ORDER BY
        RANK_USER_TEAM      
'''

#Query to get ranking for the prediction championship
qTeamScores = f'''
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
        #DATABASE#.CONSUMPTED.VW_TEAM_SCORES
    WHERE
        SEASON_ID = %s
'''

#Query to get correction related to predictions per user
qCorrection = f'''
    SELECT
        USER_NAME,
        PREDICT_ID
    FROM
        #DATABASE#.CONSUMPTED.VW_CORRECTION
    WHERE
        SEASON_ID = %s
        AND GAMEDAY = %s   
    ORDER BY
        USER_NAME,
        PREDICT_ID

'''

#Query to get users figure for month MVP race
qMVPRace_month_figures = f'''
    SELECT 
        USER_NAME,
        SUM(GAMEDAY_POINTS) AS POINTS,
        SUM(WIN) AS WIN,
        SUM(LOSS) AS LOSS,
        LISTAGG(DISTINCT TEAM_NAME, ', ') WITHIN GROUP (ORDER BY TEAM_NAME) AS LIST_TEAMS
    FROM 
        #DATABASE#.CONSUMPTED.VW_USER_SCORES_GAMEDAY
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
qList_Gameday_Calculated_MVPMonth = f'''
    SELECT
        GAMEDAY,
        NB_PREDICTION
    FROM
        #DATABASE#.CONSUMPTED.VW_GAMEDAY 
    WHERE
        SEASON_ID = %s
        AND END_YEARMONTH_LOCAL = %s
    ORDER BY
        BEGIN_DATE_LOCAL, BEGIN_TIME_LOCAL
    '''


#Query to get users figure for competition MVP race
qMVPRace_Compet_figures = f'''
    SELECT 
        USER_NAME,
        SUM(GAMEDAY_POINTS) AS POINTS,
        SUM(WIN) AS WIN,
        SUM(LOSS) AS LOSS,
        LISTAGG(DISTINCT TEAM_NAME, ', ') WITHIN GROUP (ORDER BY TEAM_NAME) AS LIST_TEAMS
    FROM 
        #DATABASE#.CONSUMPTED.VW_USER_SCORES_GAMEDAY
    WHERE
        SEASON_ID = %s
        AND COMPETITION_LABEL = %s
    GROUP BY    
        USER_NAME
    ORDER BY 
        POINTS DESC, 
        USER_NAME
'''

qList_Gameday_Calculated_MVPCompet = f'''
    SELECT
        GAMEDAY,
        NB_PREDICTION
    FROM
        #DATABASE#.CONSUMPTED.VW_GAMEDAY 
    WHERE
        SEASON_ID = %s
        AND COMPETITION_LABEL = %s
    ORDER BY
        BEGIN_DATE_LOCAL, BEGIN_TIME_LOCAL
    '''