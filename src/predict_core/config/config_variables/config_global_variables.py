'''
    This module is a utility module for all other modules. 
    It centralizes global variables, used along the program
'''

import logging
import os

logging.basicConfig(level=logging.INFO)

# Following is paths parameters
PROJECT_ROOT = "PREDICT_PROJECT"
DROPBOX_FOLDER_ROOT = 'dropbox:prediction_files'
if os.getenv("IS_TESTRUN") == '0':
    DROPBOX_FOLDER = os.path.join(DROPBOX_FOLDER_ROOT,"Prod")
    logging.info("Prod run")
else:
    DROPBOX_FOLDER = os.path.join(DROPBOX_FOLDER_ROOT,"Test")
    logging.info("Test run")    
PATHS_FILE = os.path.join(DROPBOX_FOLDER_ROOT,'docs/paths.csv')
RCLONE_CONFIG_PATH = '~/.config/rclone/rclone.conf'
DBT_DIRECTORY = "database_dbt_management"
DBT_PROFILES_PATH = "database_dbt_management/profiles.yml"
DBT_SOURCES_FOLDER = "database_dbt_management/models/sources/"
TMPF = 'TMP_FOLDER'
TMPD = 'TMP_DATABASE'
NEXT_RUN_TIME_FILE_PATH = os.path.join(DROPBOX_FOLDER,"current/outputs/python/next_run_time_utc.txt")
TROPHY_FILE_PATH = os.path.join(DROPBOX_FOLDER_ROOT,'docs/Trophy.JPG')
PLAYOFFS_TABLE_CODE = os.path.join(DROPBOX_FOLDER_ROOT,'docs/playoffs_table.txt')

# Following is csv file encapsulation parameters
TASK_DONE_ENCAPSULATED = 0
PATHS_FILE_ENCAPSULATED = 1
MESSAGE_FILE_ENCAPSULATED = 1
RUNTYPE_FILE_ENCAPSULATED = 0
GAME_ENCAPSULATED = 0
OUTPUT_NEED_ENCAPSULATED = 0

# Following is time to wait (sec) for external dependencies
DROPBOX_WAIT_TIME = 30
TIME_MESSAGE_WAIT = 90
GAME_EXTRACTION_WAIT_TIME = 30
SNOWFLAKE_LOGIN_WAIT_TIME = 30

# Following is python maps:
DOWNLOAD_INITIAL_MAP_PER_CALLER = {
    "main": "INITIAL_MAIN",
    "init_snowflake": "INITIAL_SNOWFLAKE",
    "init_compet": "INITIAL_COMPET"
}

DOWNLOAD_ADDITIONAL_MAP = {
    "MESSAGE": "MESSAGE",
    "GAME_RUN": "GAME_RUN"
}

UPLOAD_FOLDER_MAP_PER_CALLER = {
    "main": [TMPF, TMPD],
    "init_compet": [TMPF, TMPD],
    # if called by init_snowflake, we don't need to upload database files, we didn't modify them
    "init_snowflake": [TMPF]
}

DATABASE_RUN_CATEGORY_MAP = {
    "INIT_COMPET": "INIT_COMPET",
    "MESSAGE_CHECK": "MESSAGE_CHECK",
    "MESSAGE_RUN": "MESSAGE_RUN",
    "GAME_RUN": "GAME_RUN",
    "CALCULATION": "CALCULATION"
}

CALLER = {
    "MAIN": "main",
    "SNOWFLAKE": "init_snowflake",
    "COMPET": "init_compet",
    "PLAYOFFS": "playoffs"
}

MESSAGE_ACTION_MAP = {
    "RUN": "RUN",
    "CHECK": "CHECK",
    "AVOID": "AVOID"
}

GAME_ACTION_MAP = {
    "RUN": "RUN",
    "AVOID": "AVOID"
}

TASK_RUN_MAP = {
    "UPDATEGAMES": "UPDATEGAMES",
    "CHECK": "CHECK",
    "CALCULATE": "CALCULATE",
    "INIT": "INIT"
}

DROPBOX_FOLDER_MAP = { 
    "CURRENT" : 'current',
    "-1" :'-1',
    "-2" : '-2',
    "-3" : '-3',
    'global_manual_inputs' : 'global_manual_inputs',
    'local_manual_inputs' : 'local_manual_inputs',
    'manual_current': 'current/inputs/manual'
}

# Following is string parameters used along the program
LANDING_DATABASE_SCHEMA = "LANDING"
ROLE_DATABASE = "ACCOUNTADMIN"
GAME_FILTERING_CATEGORY = "GAME"
MESSAGE_FILTERING_CATEGORY = "MESSAGE"
MESSAGE_PREFIX_PROGRAM_STRING = "+++++"
MESSAGE_PREFIX_TECHNICAL_STRING = "*****"
