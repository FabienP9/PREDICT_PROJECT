'''The purpose of this file is to gather all sql queries used to process the run'''

#constant
TABLE_NAME = "#TABLE_NAME#"
SCHEMA = "#SCHEMA#"
DATABASE = "#DATABASE#"

#Query to get the calendar of runs - used in calendar_actions module
CALENDAR_QUERY = f"""
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
        {DATABASE}.CONSUMPTED.VW_CALENDAR;
"""

#Query to get topics where to extract message for the database - used in message_actions module
TOPICS_QUERY = f"""
    SELECT
        FORUM_SOURCE,
        FORUM_TIMEZONE,
        TOPIC_NUMBER
    FROM
        {DATABASE}.CONSUMPTED.VW_TOPIC
    WHERE
        SEASON_ID = %s;
"""

#Query to get end of gameday to limit message extraction - used in message_actions module
GAMEDAY_END_DETAILS_QUERY = f"""
    SELECT
        END_DATE_UTC,
        END_TIME_UTC
    FROM
        {DATABASE}.CONSUMPTED.VW_GAMEDAY
    WHERE
        SEASON_ID = %s
        AND GAMEDAY = %s;
"""

#Query to delete data from a snowflake table - used in snowflake_actions module
DELETE_DATA_QUERY = f"""
    TRUNCATE TABLE {DATABASE}.{SCHEMA}.{TABLE_NAME};
"""

#Query to delete files from a snowflake stage - used in snowflake_actions module
REMOVE_FROM_STAGE_QUERY = f"""
    REMOVE @{DATABASE}.{SCHEMA}.%{TABLE_NAME};
"""

#Query to list table in a snowflake schema - used in snowflake_actions module
LIST_TABLES_QUERY = f"""
    SHOW TABLES IN {DATABASE}.{SCHEMA};
"""

#Query to select data from a snowflake table - used in snowflake_actions module
SELECT_TABLE_QUERY = f"""
    SELECT * FROM {DATABASE}.{SCHEMA}.{TABLE_NAME};
"""

#Query to put a file in a snowflake stage - used in snowflake_actions module
PUT_TO_STAGE_QUERY = f"""
    PUT file://#FILE_PATH_ABS# @{DATABASE}.{SCHEMA}.%{TABLE_NAME};
"""

#Query to copy data from a snowflake stage to a table - used in snowflake_actions module
INSERT_DATA_QUERY = f"""
    COPY INTO {DATABASE}.{SCHEMA}.{TABLE_NAME}
    FROM @{DATABASE}.{SCHEMA}.%{TABLE_NAME}
    FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER=1 #ISENCLOSED#);
"""