'''
This tests file concern all functions in the config_environment_variables module.
It units test unexpected paths
'''
import os
from unittest.mock import patch

from src.predict_core.config.config_variables import config_environment_variables

def test_check_environment_variable_missingvar(assert_exit):
    
    # this test function check_environment_variable with missing var BI_URL
    env_values = {
        "IS_TESTRUN": "1",
        "IS_OUTPUT_AUTO": "1",
        "OVERWRITE_GAMES_STATUS": "0",
        "BI_USERNAME": "user",
        "BI_PASSWORD": "password",
        "SNOWFLAKE_USERNAME": "sfuser", 
        "SNOWFLAKE_PASSWORD": "sfpass", # NOSONAR
        "LNB_URL": "lnburl",
        "IMGBB_API_KEY": "key123"
    }

    with patch.dict(os.environ, env_values, clear=False):
        assert_exit(lambda: config_environment_variables.check_environment_variable("main"))

def test_check_environment_variable_badtype(assert_exit):
    
    # this test function check_environment_variable with bad type for OVERWRITE_GAMES_STATUS
    env_values = {
        "IS_TESTRUN": "1",
        "IS_OUTPUT_AUTO": "1",
        "OVERWRITE_GAMES_STATUS": "badtype",
        "BI_URL": "biurl",
        "BI_USERNAME": "user",
        "BI_PASSWORD": "password",
        "SNOWFLAKE_USERNAME": "sfuser",
        "SNOWFLAKE_PASSWORD": "sfpass", # NOSONAR
        "LNB_URL": "lnburl",
        "IMGBB_API_KEY": "key123",
        "RCLONE_CONFIG_BASE64": "base64string",
    }

    with patch.dict(os.environ, env_values, clear=False):
        assert_exit(lambda: config_environment_variables.check_environment_variable("main"))
