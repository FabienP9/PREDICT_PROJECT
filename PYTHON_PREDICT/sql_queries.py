'''The purpose of this file is to gather all sql queries used to process the run'''

#Query to get the calendar of runs - used in calendar_actions module
calendar_actions_qCalendar = f"""
            SELECT
                TASK_RUN,
                SEASON_ID,
                SEASON_SPORT,
                SEASON_COUNTRY,
                SEASON_NAME,
                SEASON_DIVISION,
                COMPETITION_ID,
                GAMEDAY,
                TS_TASK_UTC, 
                TS_TASK_LOCAL,
                IS_TO_INIT,
                IS_TO_CALCULATE,
                IS_TO_DELETE,
                IS_TO_RECALCULATE,
                MESSAGE_ACTION,
                GAME_ACTION
            FROM
                #DATABASE#.CONSUMPTED.VW_CALENDAR;
        """

#Query to get the games already extracted in the database - used in game_actions module
game_actions_qGames_already_extracted = f"""
        SELECT
            SEASON_ID,
            COMPETITION_ID,
            COMPETITION_SOURCE,
            GAME_SOURCE_ID
        FROM
            #DATABASE#.CONSUMPTED.VW_GAME;
    """

#Query to get the games to extract for the database - used in game_actions module
game_actions_qGames_to_extract = f"""
        SELECT
            SEASON_ID,
            COMPETITION_ID,
            COMPETITION_SOURCE,
            GAME_SOURCE_ID
        FROM
            #DATABASE#.CONSUMPTED.VW_GAME
        WHERE
            SEASON_ID = %s
            AND GAMEDAY = %s;
    """

#Query to get topics where to extract message for the database - used in message_actions module
message_actions_qTopics_to_extract = f"""
        SELECT
            FORUM_SOURCE,
            FORUM_TIMEZONE,
            TOPIC_NUMBER
        FROM
            #DATABASE#.CONSUMPTED.VW_TOPIC
        WHERE
            SEASON_ID = %s;
    """

#Query to get end of gameday to limit message extraction - used in message_actions module
message_actions_qGameDayEnd = f'''
        SELECT
            END_DATE_UTC,
            END_TIME_UTC
        FROM
            #DATABASE#.CONSUMPTED.VW_GAMEDAY
        WHERE
            SEASON_ID = %s
            AND GAMEDAY = %s;
    '''

#Query to delete data from a snowflake table - used in snowflake_actions module
snowflake_actions_qDeleteData = f"""
        TRUNCATE TABLE #DATABASE#.#SCHEMA#.#TABLE_NAME#;
    """

#Query to delete files from a snowflake stage - used in snowflake_actions module
snowflake_actions_qRemoveFromStage = f"""
        REMOVE @#DATABASE#.#SCHEMA#.%#TABLE_NAME#;
    """

#Query to list table in a snowflake schema - used in snowflake_actions module
snowflake_actions_qListTables = f"""
        SHOW TABLES IN #DATABASE#.#SCHEMA#;
    """

#Query to select data from a snowflake table - used in snowflake_actions module
snowflake_actions_qSelectData = f"""
        SELECT * FROM #DATABASE#.#SCHEMA#.#TABLE_NAME#;
    """

#Query to put a file in a snowflake stage - used in snowflake_actions module
snowflake_actions_qPutToStage = f"""
        PUT file://#FILE_PATH_ABS# @#DATABASE#.#SCHEMA#.%#TABLE_NAME#;
    """

#Query to copy data from a snowflake stage to a table - used in snowflake_actions module
snowflake_actions_qInsertData = f"""
        COPY INTO #DATABASE#.#SCHEMA#.#TABLE_NAME#
        FROM @#DATABASE#.#SCHEMA#.%#TABLE_NAME#
        FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER=1 #ISENCLOSED#);
    """