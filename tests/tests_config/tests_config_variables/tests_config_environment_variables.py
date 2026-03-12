'''
This tests file concern all functions in the config_environment_variables module.
It units test the happy path for each function
'''

import os
from unittest.mock import patch

from src.predict_core.config.config_variables import config_environment_variables

def test_check_environment_variable():
    
    # this test function check_environment_variable
    env_values = {
        "IS_TESTRUN": "1",
        "IS_OUTPUT_AUTO": "1",
        "OVERWRITE_GAMES_STATUS": "0",
        "BI_URL": "biurl",
        "BI_USERNAME": "user",
        "BI_PASSWORD": "password",
        "SNOWFLAKE_USERNAME": "sfuser",
        "SNOWFLAKE_PASSWORD": "sfpass", # NOSONAR
        "LNB_URL": "lnburl",
        "IMGBB_API_KEY": "key123"
    }

    with patch.dict(os.environ, env_values, clear=False):
        config_environment_variables.check_environment_variable("main")

