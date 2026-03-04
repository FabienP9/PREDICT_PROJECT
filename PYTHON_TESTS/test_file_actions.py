'''
This tests file concern all functions in the file_actions module.
It units test the happy path for each function
'''

import unittest
from unittest.mock import patch, MagicMock,mock_open
from pandas.testing import assert_frame_equal
import pandas as pd
import sys
import os
import tempfile
import matplotlib.pyplot as plt

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'PYTHON_PREDICT'))
sys.path.insert(0, ROOT)
from file_actions import file_actions
from testutils import read_json

def test_read_json():
    
    # this test the function read_json
    expected_result = {"key": "value"}
    result = file_actions.read_json("materials/read_json.json")
    assert result == expected_result

def test_read_and_check_csv():
    
    # this test the function read_and_check_csv
    local_file_path = "materials/read_csv.csv"
    mock_schema = read_json("materials/read_csv_schema.json")
    expected_df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
    
    with patch.object(file_actions, "read_json", return_value=mock_schema):
        df_result = file_actions.read_and_check_csv(local_file_path)
        assert_frame_equal(df_result.reset_index(drop=True), expected_df.reset_index(drop=True))

def test_read_yml():
    
    # this test the function read_yml
    local_file_path = "materials/read_yml.yml"
    expected_result = "key: value"
    
    result = file_actions.read_yml(local_file_path)
    assert result == expected_result

def test_read_txt():

    # this test the function read_txt
    local_file_path = "materials/read_txt.txt"
    expected_result = "hello world!"
    
    result = file_actions.read_txt(local_file_path)
    assert result == expected_result

def test_create_csv():
    
    # this test the function create_csv
    local_file_path = "create_csv.csv"
    df = pd.read_csv("materials/read_csv.csv")

    m = MagicMock()
    with patch.object(df, "to_csv", m):
        file_actions.create_csv(local_file_path, df)
        unittest.TestCase().assertTrue(m.called)

def test_create_yml():
    
    # this test the function create_yml
    local_file_path = "create_yml.yml"
    s = file_actions.read_yml("materials/read_yml.yml")

    m = mock_open()
    with patch("builtins.open", m):
        file_actions.create_yml(local_file_path, s)
        m().write.assert_called_once_with("key: value")

def test_create_txt():
    
    # this test the function create_txt
    local_file_path = "create_txt.txt"
    s = file_actions.read_txt("materials/read_txt.txt")

    m = mock_open()
    with patch("builtins.open", m):
        file_actions.create_txt(local_file_path, s)
        m().write.assert_called_once_with("hello world!")

def test_create_jpg():
    
    # this test the function test_create_jpg creating a temp folder
    with tempfile.TemporaryDirectory() as tmpdir:
        
        local_file_path = tmpdir+"/create_jpg.jpg"
        fig, ax = plt.subplots()
        ax.plot([1, 2, 3], [4, 5, 6])
        ax.set_title("Happy Path Test")
        
        file_actions.create_jpg(local_file_path, fig)
        assert os.path.exists(local_file_path), "Expected JPG file was not created."
        assert os.path.getsize(local_file_path) > 0, "Created JPG file is empty."

        plt.close(fig)
        
def test_personalize_yml_dbt_file():
    
    # this test the function personalize_yml_dbt_file creating a temp folder
    with patch.object(file_actions,'read_yml', return_value="#ACCOUNT# #DATABASE#"), \
         patch.object(file_actions,'create_yml') as mock_create_yml, \
         patch.dict(os.environ, {"SNOWFLAKE_USERNAME":"u","SNOWFLAKE_PASSWORD":"p","IS_TESTRUN":"0"}):
        file_actions.personalize_yml_dbt_file("dummy.yml", {"ACCOUNT":"acc","DATABASE_PROD":"db","DATABASE_TEST":"dbt","WAREHOUSE":"w"})
    unittest.TestCase().assertTrue(mock_create_yml.called)

def test_parametrize_yml_dbt_file():
    
    # this test the function parametrize yml dbt file
    with patch.object(file_actions,"read_yml", return_value="account: A\ndatabase: B\nuser: U\npassword: P"), \
         patch.object(file_actions,"create_yml") as mock_create_yml:
        file_actions.parametrize_yml_dbt_file("dummy.yml")
    unittest.TestCase().assertTrue(mock_create_yml.called)

def test_filter_data():
    
    # this test the function filter_data with two dependant files
    data_dict = {
        "df_df1": pd.DataFrame({"col": [1, 2]}),
        "df_df2": pd.DataFrame({"col": [2, 3]})
    }


    df_paths = pd.DataFrame({
        "NAME": ["df1", "df2"],
        "FILTERING_CATEGORY": ["cat", "cat"],
        "FILTERING_FILE": ["", "df1"],
        "FILTERING_COLUMN": ["col", "col"],
        "IS_FOR_UPLOAD": [0, 0]
    })
    
    expected = {
        "df_df1": pd.DataFrame({"col": [1, 2]}),
        "df_df2": pd.DataFrame({"col": [2]})
    }
    with patch.object(file_actions,"create_csv"):
        result = file_actions.filter_data(data_dict, df_paths, "cat")
    
    assert_frame_equal(result["df_df1"].reset_index(drop=True), expected["df_df1"].reset_index(drop=True))
    assert_frame_equal(result["df_df2"].reset_index(drop=True), expected["df_df2"].reset_index(drop=True))

def test_get_locally_from_dropbox():
    
    # this test the function get_locally_from_dropbox
    file_name = 'game'
    local_folder = 'local_folder'
    df_paths = pd.read_csv('materials/paths.csv')
    mock_df_game = pd.read_csv("materials/game.csv")
    
    with patch.object(file_actions.dropboxA,"download_file", return_value={"df_game": mock_df_game}):
        result = file_actions.get_locally_from_dropbox(file_name, local_folder, df_paths)
        unittest.TestCase().assertIn("df_game", result)

def test_modify_run_file():
    
    # this test the function modify_run_file
    df_RUN_TYPE = pd.read_csv("materials/RUN_TYPE_before_initiate.csv")
    called_by = "main"
    event="initiate"
    os.environ["IS_OUTPUT_AUTO"] = "1"
    expected_df = pd.read_csv("materials/RUN_TYPE_after_initiate.csv") #we won't check RUN_TIME_UTC
    with patch.object(file_actions,"create_csv"):
        result_df = file_actions.modify_run_file(df_RUN_TYPE,called_by,event)
        assert_frame_equal(result_df[1:].astype(str).reset_index(drop=True), expected_df[1:].astype(str).reset_index(drop=True),check_dtype=False)
        assert result_df.at[0,'EVENT'] == expected_df.at[0,'EVENT'] 
        assert result_df.at[0,'RUN_TYPE'] == expected_df.at[0,'RUN_TYPE'] 
        assert (result_df.at[0,'RUN_METHOD'] == expected_df.at[0,'RUN_METHOD']) or (pd.isna(result_df.at[0,'RUN_METHOD']) and pd.isna(expected_df.at[0,'RUN_METHOD']))
        assert int(result_df.at[0,'OUTPUT_AUTO']) == int(expected_df.at[0,'OUTPUT_AUTO']) 
        assert (result_df.at[0,'PLANNED_RUN_TIME_UTC'] == expected_df.at[0,'PLANNED_RUN_TIME_UTC']) or (pd.isna(result_df.at[0,'PLANNED_RUN_TIME_UTC']) and pd.isna(expected_df.at[0,'PLANNED_RUN_TIME_UTC']))

def test_download_paths_file():
    
    # this test the function download_paths_file
    mock_data_dict = {"df_paths": pd.read_csv("materials/paths.csv")}

    with patch.object(file_actions.dropboxA,"download_file", return_value=mock_data_dict):
        result = file_actions.download_paths_file()

        assert isinstance(result, dict)
        assert "df_paths" in result
        df = result["df_paths"]
        # Ensure columns are converted to lists
        assert all(isinstance(df.loc[0, c], list) for c in ["FILTERING_COLUMN", "DOWNLOAD_CATEGORY", "PYTHON_CATEGORY", "DBT_CATEGORY"])

def test_initiate_local_environment():
    
    # this test the function initiate_local_environment
    called_by = "main"
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
        
        file_actions.initiate_local_environment(called_by)

def test_terminate_local_environment():
    
    # this test the function terminate_local_environment
    with tempfile.TemporaryDirectory() as tmpdir:
        called_by = "main"
        mock_df_RUN_TYPE = pd.read_csv("materials/RUN_TYPE_after_initiate.csv")
        context_dict = {
            "df_RUN_TYPE" : mock_df_RUN_TYPE,
            "df_paths": pd.read_csv("materials/paths.csv")
        }
        mock_dir_entry = MagicMock()
        mock_dir_entry.path = "../TMP_FOLDER/game.csv"
        mock_dir_entry.name = "game"
        
        with patch.object(file_actions,"parametrize_yml_dbt_file"), \
            patch.object(file_actions,"modify_run_file", return_value=mock_df_RUN_TYPE), \
            patch.object(file_actions.config,"UPLOAD_FOLDER_MAP_PER_CALLER", {"main": [tmpdir]}), \
            patch.object(file_actions.config,"multithreading_run"), \
            patch.object(file_actions.config,"destroy_local_folder"):

            file_actions.terminate_local_environment(called_by,context_dict)

def test_download_needed_files():
    
    # this test the download_needed files function.
    df_paths = pd.read_csv("materials/paths.csv")
    sr_output_need = pd.read_csv("materials/output_need_calculate.csv").iloc[0]

    with patch.object(file_actions.config,"multithreading_run"):
        file_actions.download_needed_files(df_paths, sr_output_need)

if __name__ == '__main__':
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.FunctionTestCase(test_read_json))
    test_suite.addTest(unittest.FunctionTestCase(test_read_and_check_csv))
    test_suite.addTest(unittest.FunctionTestCase(test_read_yml))
    test_suite.addTest(unittest.FunctionTestCase(test_read_txt))
    test_suite.addTest(unittest.FunctionTestCase(test_create_csv))
    test_suite.addTest(unittest.FunctionTestCase(test_create_yml))
    test_suite.addTest(unittest.FunctionTestCase(test_create_txt))
    test_suite.addTest(unittest.FunctionTestCase(test_create_jpg))
    test_suite.addTest(unittest.FunctionTestCase(test_personalize_yml_dbt_file))
    test_suite.addTest(unittest.FunctionTestCase(test_parametrize_yml_dbt_file))
    test_suite.addTest(unittest.FunctionTestCase(test_filter_data))
    test_suite.addTest(unittest.FunctionTestCase(test_get_locally_from_dropbox))
    test_suite.addTest(unittest.FunctionTestCase(test_modify_run_file))
    test_suite.addTest(unittest.FunctionTestCase(test_download_paths_file))
    test_suite.addTest(unittest.FunctionTestCase(test_initiate_local_environment))
    test_suite.addTest(unittest.FunctionTestCase(test_terminate_local_environment))
    test_suite.addTest(unittest.FunctionTestCase(test_download_needed_files))
    runner = unittest.TextTestRunner()
    runner.run(test_suite)

