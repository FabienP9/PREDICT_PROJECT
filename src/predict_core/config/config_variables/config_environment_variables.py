'''
    This module is a utility module. 
    It checks environment variables for a good run of the program
'''
import logging
import os

from .. import config_decorators

logging.basicConfig(level=logging.INFO)

@config_decorators.exit_program(log_filter=lambda args: dict(args), execute_final_function = 0)
def check_environment_variable(called_by: str):

    '''
        Check all environment variables existences and values.
        Raises:
            Error if missing an expected existance or a bad value and exit program (with decorator)
    '''

    ENV_VARS = [
        {"var": "IS_TESTRUN", "is_boolean": 1,  "main": 1,  "init_snowflake": 1, "compet": 1, "playoffs": 0},
        {"var": "IS_OUTPUT_AUTO", "is_boolean": 1,  "main": 1,  "init_snowflake": 0, "compet": 0, "playoffs": 0},
        {"var": "OVERWRITE_GAMES_STATUS", "is_boolean": 1,  "main": 1,  "init_snowflake": 0, "compet": 0, "playoffs": 0},
        {"var": "BI_URL", "is_boolean": 0, "main": 1, "init_snowflake": 0, "compet": 0, "playoffs": 0},
        {"var": "BI_USERNAME", "is_boolean": 0, "main": 1, "init_snowflake": 0, "compet": 0, "playoffs": 0},
        {"var": "BI_PASSWORD", "is_boolean": 0, "main": 1, "init_snowflake": 0, "compet": 0, "playoffs": 0},
        {"var": "SNOWFLAKE_USERNAME", "is_boolean": 0, "main": 1, "init_snowflake": 1, "compet": 1, "playoffs": 0},
        {"var": "SNOWFLAKE_PASSWORD", "is_boolean": 0, "main": 1, "init_snowflake": 1, "compet": 1, "playoffs": 0},
        {"var": "LNB_URL", "is_boolean": 0, "main": 1, "init_snowflake": 0, "compet": 1, "playoffs": 0},
        {"var": "IMGBB_API_KEY", "is_boolean": 0, "main": 1, "init_snowflake": 0, "compet": 0, "playoffs": 1},
    ]

    for env in ENV_VARS:
        if not env.get(called_by.lower()):
            continue

        value = os.environ.get(env["var"])
        if value is None:
            raise ValueError(f"Missing environment variable: {env['var']}")
        if env["is_boolean"] == 1 and int(value) not in (0,1):
            raise ValueError(f"Bad boolean value for {env['var']}")
