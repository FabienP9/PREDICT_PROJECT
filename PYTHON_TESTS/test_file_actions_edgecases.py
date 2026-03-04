'''
This tests file concern all functions in the file_actions module.
It units test unexpected paths for each function
'''

import unittest
from unittest.mock import patch, MagicMock,mock_open
import pandas as pd
import sys
import os
import tempfile

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'PYTHON_PREDICT'))
sys.path.insert(0, ROOT)
from file_actions import file_actions
from testutils import assertExit
from testutils import read_json

def test_read_json_file_not_found():
    
    # this test the function read_json with a file non existant. Must exit the program
    with patch("builtins.open", side_effect=FileNotFoundError("no file")):
        assertExit(lambda: file_actions.read_json("does_not_exist.json"))

def test_read_json_invalid_json():
    
    # this test the function read_json with a file json type invalid. Must exit the program
    with patch("builtins.open", mock_open(read_data="not-json")), \
         patch("json.load", side_effect=ValueError("bad json")):
        assertExit(lambda: file_actions.read_json("bad.json"))

def test_read_and_check_csv_missing_columns():
    
    # this test the function read_and_check_csv with a file having a missing column. Must exit the program
    local_file_path = "materials/read_csv.csv"
    mock_schema = read_json("materials/edgecases/read_csv_schema_with_fake_column.json")

    with patch.object(file_actions, "read_json", return_value=mock_schema):
        assertExit(lambda: file_actions.read_and_check_csv(local_file_path))

def test_read_and_check_csv_type_mismatch():
    
    # this test the function read_and_check_csv with a file having a column of a different type than expected. Must exit the program
    local_file_path = pd.read_csv("materials/edgecases/read_csv_type_mismatch.csv")
    mock_schema = read_json("materials/read_csv_schema.json")

    with patch.object(file_actions, "read_json", return_value=mock_schema):
        assertExit(lambda: file_actions.read_and_check_csv(local_file_path))

def test_read_yml_file_not_found():
    
    # this test the function read_yml with a file non existant. Must exit the program
    with patch("builtins.open", side_effect=FileNotFoundError("no file")):
        assertExit(lambda: file_actions.read_yml("missing.yml"))

def test_read_txt_file_not_found():
    
    # this test the function read_txt with a file non existant. Must exit the program
    with patch("builtins.open", side_effect=FileNotFoundError("no file")):
        assertExit(lambda: file_actions.read_txt("missing.txt"))

def test_create_csv_write_failure():

    # this test the function create_csv forcing a write failure. Must exit the program.
    local_file_path = "create_csv.csv"
    df = pd.read_csv("materials/read_csv.csv")

    with patch("pandas.DataFrame.to_csv", side_effect=OSError("disk full")):
        assertExit(lambda: file_actions.create_csv(local_file_path, df))

def test_create_yml_failure():
    
    # this test the function create_yml forcing a write failure. Must exit the program.
    local_file_path = "create_yml.yml"
    s = file_actions.read_yml("materials/read_yml.yml")

    with patch("builtins.open", side_effect=OSError("cannot write")):
        assertExit(lambda: file_actions.create_yml(local_file_path, s))

def test_create_txt_failure():
    
    # this test the function create_txt forcing a write failure. Must exit the program.
    local_file_path = "create_txt.txt"
    s = file_actions.read_txt("materials/read_txt.txt")

    with patch("builtins.open", side_effect=OSError("cannot write")):
        assertExit(lambda: file_actions.create_yml(local_file_path, s))

def test_create_jpg_save_failure():
    
    # this test the function create_jpg forcing an error while saving it. Must exit the program.
    with tempfile.TemporaryDirectory() as tmpdir:
        local_file_path = tmpdir+"/create_jpg.jpg"
        fake_fig = MagicMock()
        fake_fig.savefig.side_effect = Exception("save error")
        assertExit(lambda: file_actions.create_jpg(local_file_path, fake_fig))

def test_personalize_yml_missing_env_vars():
    
    # this test the function personalize_yml_dbt_file with the environment variables non provided. Must exit the program.
    with patch.object(file_actions,"read_yml", return_value="#DATABASE# something"), \
         patch.dict("os.environ", {}, clear=True), \
         patch.object(file_actions,"create_yml"):
        
        sr = {"ACCOUNT": "acc", "DATABASE_PROD": "prod", "DATABASE_TEST": "test", "WAREHOUSE": "wh"}
        assertExit(lambda: file_actions.personalize_yml_dbt_file("profiles.yml", sr))

def test_parametrize_yml_dbt_file_bad_read():
    
    # this test the function parametrize_yml_dbt_file forcing an error reading it. Must exit the program.
    with patch.object(file_actions,"read_yml", side_effect=OSError("cannot read")):
        assertExit(lambda: file_actions.parametrize_yml_dbt_file("profiles.yml"))

def test_get_locally_from_dropbox_dropbox_failure():
    
    # this test the function get_locally_from_dropbox forcing an error coming from dropbox. Must exit the program.
    file_name = 'game'
    local_folder = 'local_folder'
    df_paths = pd.read_csv('materials/paths.csv')

    with patch.object(file_actions.dropboxA,"download_file", side_effect=Exception("network error")):
        assertExit(lambda: file_actions.get_locally_from_dropbox(file_name, local_folder, df_paths))

def test_modify_run_file_invalid_event():
    
    # this test the function modify_run_file with an unexpected event. Must exit the program.
        df_RUN_TYPE = pd.read_csv("materials/RUN_TYPE_before_initiate.csv")
        called_by = "main"
        event="unexpected"
        os.environ["IS_OUTPUT_AUTO"] = "1"

        assertExit(lambda: file_actions.modify_run_file(df_RUN_TYPE,called_by,event))

def test_download_paths_file_invalid_literal():
    
    # this test the function download_paths_file with a badly written FILTERING_COLUMN. Must exit the program
    mock_data_dict = {"df_paths": pd.read_csv("materials/edgecases/paths_with_bad_filtering_column.csv")}

    with patch.object(file_actions.dropboxA,"download_file", return_value=mock_data_dict):
        assertExit(lambda: file_actions.download_paths_file())

def test_initiate_local_environment_missing_flag():
    
    # this test initial_local_environment with unknown caller. Must exit the program
    called_by = "unknown caller"
    mock_df_paths_dict = {
        "df_paths": pd.read_csv("materials/paths.csv")
    }
    mock_df_snowflake_account_connect = pd.read_csv("materials/snowflake_account_connect.csv")
    mock_str_next_run_time_utc = "2024-01-01 08:01:00.000"
    mock_df_RUN_TYPE = pd.read_csv("materials/RUN_TYPE_after_initiate.csv")
    mock_data_dict = {
        "df_snowflake_account_connect": mock_df_snowflake_account_connect,
        "str_next_run_time_utc": mock_str_next_run_time_utc,
        "df_RUN_TYPE": mock_df_RUN_TYPE,
    }

    with patch.object(file_actions.config,"create_local_folder"), \
         patch.object(file_actions,"download_paths_file", return_value=mock_df_paths_dict), \
         patch.object(file_actions.config,"multithreading_run", return_value=[mock_data_dict]), \
         patch.object(file_actions,"modify_run_file", return_value=mock_df_RUN_TYPE), \
         patch.object(file_actions.dropboxA,"upload_file"), \
         patch.object(file_actions,"personalize_yml_dbt_file"):
        
        assertExit(lambda: file_actions.initiate_local_environment(called_by))

def test_initiate_local_environment_empty_df_paths():
    
    # this test initial_local_environment with an empty df_paths. Must exit the program
    called_by = "main"
    mock_df_paths_dict = {
        "df_paths": pd.read_csv("materials/edgecases/paths_empty.csv")
    }
    mock_df_snowflake_account_connect = pd.read_csv("materials/snowflake_account_connect.csv")
    mock_str_next_run_time_utc = "2024-01-01 08:01:00.000"
    mock_df_RUN_TYPE = pd.read_csv("materials/RUN_TYPE_after_initiate.csv")
    mock_data_dict = {
        "df_snowflake_account_connect": mock_df_snowflake_account_connect,
        "str_next_run_time_utc": mock_str_next_run_time_utc,
        "df_RUN_TYPE": mock_df_RUN_TYPE,
    }

    with patch.object(file_actions.config,"create_local_folder"), \
         patch.object(file_actions,"download_paths_file", return_value=mock_df_paths_dict), \
         patch.object(file_actions.config,"multithreading_run", return_value=[mock_data_dict]), \
         patch.object(file_actions,"modify_run_file", return_value=mock_df_RUN_TYPE), \
         patch.object(file_actions.dropboxA,"upload_file"), \
         patch.object(file_actions,"personalize_yml_dbt_file"):
        
        assertExit(lambda: file_actions.initiate_local_environment(called_by))

if __name__ == '__main__':
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.FunctionTestCase(test_read_json_file_not_found))
    test_suite.addTest(unittest.FunctionTestCase(test_read_json_invalid_json))
    test_suite.addTest(unittest.FunctionTestCase(test_read_and_check_csv_missing_columns))
    test_suite.addTest(unittest.FunctionTestCase(test_read_and_check_csv_type_mismatch))
    test_suite.addTest(unittest.FunctionTestCase(test_read_yml_file_not_found))
    test_suite.addTest(unittest.FunctionTestCase(test_read_txt_file_not_found))
    test_suite.addTest(unittest.FunctionTestCase(test_create_csv_write_failure))
    test_suite.addTest(unittest.FunctionTestCase(test_create_yml_failure))
    test_suite.addTest(unittest.FunctionTestCase(test_create_txt_failure))
    test_suite.addTest(unittest.FunctionTestCase(test_create_jpg_save_failure))
    test_suite.addTest(unittest.FunctionTestCase(test_personalize_yml_missing_env_vars))
    test_suite.addTest(unittest.FunctionTestCase(test_parametrize_yml_dbt_file_bad_read))
    test_suite.addTest(unittest.FunctionTestCase(test_get_locally_from_dropbox_dropbox_failure))
    test_suite.addTest(unittest.FunctionTestCase(test_modify_run_file_invalid_event))
    test_suite.addTest(unittest.FunctionTestCase(test_download_paths_file_invalid_literal))
    test_suite.addTest(unittest.FunctionTestCase(test_initiate_local_environment_missing_flag))
    test_suite.addTest(unittest.FunctionTestCase(test_initiate_local_environment_empty_df_paths))
    runner = unittest.TextTestRunner()
    runner.run(test_suite)