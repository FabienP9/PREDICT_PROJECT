'''
This tests file concern all functions in the local_environment_manipulation module.
It units test the happy path for each function
'''

import os
import tempfile
from unittest.mock import patch, MagicMock

from src.predict_core.files_manipulation.local_files_manipulation import local_environment_manipulation
import src.predict_core.config.config_variables.config_global_variables as var

def test_create_and_destroy_local_folder():
    
    # this test functions create_local_folder and destroy_local_folder
    local_environment_manipulation.create_local_folder()
    assert os.path.exists(var.TMPF)
    assert os.path.exists(var.TMPD)

    local_environment_manipulation.destroy_local_folder()
    assert not os.path.exists(var.TMPF)
    assert not os.path.exists(var.TMPD)

def test_initiate_local_environment(read_csv,read_yml_as_serie):
    
    # this test the function initiate_local_environment
    called_by = "main"
    mock_df_paths_dict = {"df_paths" : read_csv("paths.csv")}
    mock_sr_snowflake_account_connect = read_yml_as_serie("snowflake_account_connect.yml")
    mock_str_next_run_time_utc = "2024-01-01 08:01:00.000"
    mock_df_run_type = read_csv("RUN_TYPE_after_initiate.csv")
    mock_data_dict = {
        "sr_snowflake_account_connect": mock_sr_snowflake_account_connect,
        "str_next_run_time_utc": mock_str_next_run_time_utc,
        "df_run_type": mock_df_run_type,
    }

    with patch.object(local_environment_manipulation,"create_local_folder"), \
         patch.object(local_environment_manipulation.specific_files_operations,"get_paths_file_details", return_value=mock_df_paths_dict), \
         patch.object(local_environment_manipulation,"multithread_run", return_value=[mock_data_dict]), \
         patch.object(local_environment_manipulation.specific_files_operations,"modify_run_type_file", return_value=mock_df_run_type), \
         patch.object(local_environment_manipulation.dropbox,"upload_file"), \
         patch.object(local_environment_manipulation.specific_files_operations,"personalize_yml_dbt_file"):
        
        local_environment_manipulation.initiate_local_environment(called_by)

def test_terminate_local_environment(read_csv):
    
    # this test the function terminate_local_environment
    with tempfile.TemporaryDirectory() as tmpdir:
        called_by = "main"
        mock_df_run_type = read_csv("RUN_TYPE_after_initiate.csv")
        context_dict = {
            "df_run_type" : mock_df_run_type,
            "df_paths": read_csv("paths.csv")
        }
        mock_dir_entry = MagicMock()
        mock_dir_entry.path = "../TMP_FOLDER/game.csv"
        mock_dir_entry.name = "game"
        
        with patch.object(local_environment_manipulation.specific_files_operations,"parametrize_yml_dbt_file"), \
            patch.object(local_environment_manipulation.specific_files_operations,"modify_run_type_file", return_value=mock_df_run_type), \
            patch.object(local_environment_manipulation.var,"UPLOAD_FOLDER_MAP_PER_CALLER", {"main": [tmpdir]}), \
            patch.object(local_environment_manipulation,"multithread_run"), \
            patch.object(local_environment_manipulation,"destroy_local_folder"):

            local_environment_manipulation.terminate_local_environment(called_by,context_dict)
