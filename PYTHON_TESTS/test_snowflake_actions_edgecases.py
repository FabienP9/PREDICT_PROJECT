'''
This tests file concern all functions in the snowflake_actions module.
It units test unexpected paths for each function
'''
import unittest
from unittest.mock import patch, MagicMock,call
import pandas as pd
import sys
import os
import tempfile

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'PYTHON_PREDICT'))
sys.path.insert(0, ROOT)
import snowflake_actions
from testutils import assertExit

def test_snowflake_execute_select_path():
    
    # this test the function snowflake_execute select path
    sr_snowflake_account = pd.read_csv("materials/snowflake_account_connect.csv").iloc[0]
    query = "SELECT * FROM #DATABASE#.table"

    mock_cursor = MagicMock()
    mock_cursor.__enter__.return_value = mock_cursor
    mock_cursor.fetch_pandas_all.return_value = pd.DataFrame({"col": [1, 2]})
    
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.is_closed.return_value = False

    with patch.object(snowflake_actions,'snowflake_connect', return_value=mock_conn), \
         patch.object(snowflake_actions.os,'getenv', return_value='0'):

        result = snowflake_actions.snowflake_execute(sr_snowflake_account, query)
        assert isinstance(result, pd.DataFrame)

def test_snowflake_execute_show_path():
    
    # this test the function snowflake_execute for show command
    sr_snowflake_account = pd.read_csv("materials/snowflake_account_connect.csv").iloc[0]
    query = "SHOW TABLES;"

    mock_cursor = MagicMock()
    mock_cursor.__enter__.return_value = mock_cursor
    mock_cursor.fetchall.return_value = [("t1",)]
    
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.is_closed.return_value = False

    with patch.object(snowflake_actions,'snowflake_connect', return_value=mock_conn), \
         patch.object(snowflake_actions.os,'getenv', return_value='0'):

        result = snowflake_actions.snowflake_execute(sr_snowflake_account, query)
        assert isinstance(result, list)

def test_snowflake_execute_invalidquery():
    
    # this test the function snowflake_execute with invalid query. Must exit the program
    sr_snowflake_account = pd.read_csv("materials/snowflake_account_connect.csv").iloc[0]
    query = "INVALID QUERY;"

    mock_cursor = MagicMock()
    mock_cursor.__enter__.return_value = mock_cursor
    
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.is_closed.return_value = False

    with patch.object(snowflake_actions,'snowflake_connect', return_value=mock_conn), \
         patch.object(snowflake_actions.os,'getenv', return_value='0'),\
         patch.object(snowflake_actions.sqlglot,'parse_one') as mock_parse_one:

        mock_parse_one.return_value = object()
        assertExit(lambda: snowflake_actions.snowflake_execute(sr_snowflake_account, query))

def test_snowflake_execute_script_empty_script():
    
    # this test the function snowflake_execute_script with empty script. Must run but do nothing
    sr_snowflake_account = pd.read_csv("materials/snowflake_account_connect.csv").iloc[0]
    script = ""

    with patch.object(snowflake_actions.os,"getenv", return_value="0"):  
        with patch.object(snowflake_actions,"snowflake_connect") as mock_connect:
            mock_connection = mock_connect.return_value
            snowflake_actions.snowflake_execute_script(sr_snowflake_account, script)
            mock_connection.execute_string.assert_called_once_with("")

def test_delete_table_data_executes_expected_queries():
    
    # this test the function delete_table_data truncate and remove queries
    sr_snowflake_account = pd.read_csv("materials/snowflake_account_connect.csv").iloc[0]
    schema = "landing"
    table_metadata = [None, "test_table"]

    with patch.object(snowflake_actions,'snowflake_execute') as mock_exec:
        snowflake_actions.delete_table_data(sr_snowflake_account, schema, table_metadata)
        calls = [call(sr_snowflake_account, unittest.mock.ANY), call(sr_snowflake_account, unittest.mock.ANY)]
        mock_exec.assert_has_calls(calls, any_order=False)

def test_update_snowflake_from_python_encapsulated():
    
    # this test the function update_snowflake_from_python with encapsulated data
    called_by = 'main'
    sr_snowflake_account = pd.read_csv("materials/snowflake_account_connect.csv").iloc[0]
    table_name = "landing_message_check" #this file is encapsulated according to df_paths
    df_paths = pd.read_csv("materials/paths.csv")
    local_folder = 'local'

    exp_schema = "landing"
    exp_file_name = "message_check"

    with patch.object(snowflake_actions,"snowflake_execute") as mock_snowflake_execute, \
         patch.object(snowflake_actions,"create_table_file") :

        snowflake_actions.update_snowflake_from_python(called_by,sr_snowflake_account,table_name,df_paths,local_folder)

        assert mock_snowflake_execute.call_count == 2
        qPut_call = mock_snowflake_execute.call_args_list[0][0][1]
        assert str(exp_file_name) in qPut_call
        assert exp_schema in qPut_call
        assert table_name in qPut_call
        assert any("FIELD_OPTIONALLY_ENCLOSED_BY" in c[0][1] for c in mock_snowflake_execute.call_args_list)

def test_update_snowflake_from_dbt_failure():
    
    # this test the function update_snowflake_from_dbt a failing dbt command. Must exit the program
    called_by = "main"
    sr_snowflake_account = pd.read_csv("materials/snowflake_account_connect.csv").iloc[0]
    df_paths = pd.read_csv("materials/paths.csv")
    lst_dbt_tables=["curated_season"]

    with patch.object(snowflake_actions.subprocess,'run') as mock_run, \
         patch.object(snowflake_actions,'create_table_file'):

        mock_run.return_value.returncode = 1
        mock_run.return_value.stdout = "bad"
        mock_run.return_value.stderr = "error"

        assertExit(lambda: snowflake_actions.update_snowflake_from_dbt(called_by,sr_snowflake_account,df_paths,lst_dbt_tables))

def test_update_snowflake_initsnowflake_runs_python_and_dbt():
    
    # this test the function update_snowflake called by initsnowflake for python and dbt
    called_by = "init_snowflake"
    context_dict = {
        'df_paths': pd.read_csv("materials/paths.csv"),
        'sr_snowflake_account_connect': pd.read_csv("materials/snowflake_account_connect.csv").iloc[0],
        'sr_output_need': pd.read_csv("materials/output_need_calculate.csv").iloc[0]
    }
    with tempfile.TemporaryDirectory() as tmpdir:
        open(os.path.join(tmpdir, "table_a.csv"), 'w').close()
        open(os.path.join(tmpdir, "table_b.csv"), 'w').close()
        local_folder = tmpdir

        with patch.object(snowflake_actions,"update_snowflake_from_python"), \
            patch.object(snowflake_actions,"update_snowflake_from_dbt") as mock_update_dbt:

            snowflake_actions.update_snowflake(called_by, context_dict, local_folder) 
            mock_update_dbt.assert_called_once()

def test_update_snowflake_main_with_empty_lists():
    
    # this test the function update_snowflake with empty tables list for python and dbt
    called_by = "main"
    context_dict = {
        'df_paths': pd.read_csv("materials/paths.csv"),
        'sr_snowflake_account_connect': pd.read_csv("materials/snowflake_account_connect.csv").iloc[0],
        'sr_output_need': pd.read_csv("materials/output_need_calculate.csv").iloc[0]
    }
    with tempfile.TemporaryDirectory() as tmpdir:
        open(os.path.join(tmpdir, "table_a.csv"), 'w').close()
        open(os.path.join(tmpdir, "table_b.csv"), 'w').close()
        local_folder = tmpdir

        with patch.object(snowflake_actions,"get_list_tables_to_update", return_value=([], [])), \
             patch.object(snowflake_actions,"update_snowflake_from_python") as mock_update_snowflake, \
             patch.object(snowflake_actions,"update_snowflake_from_dbt") as mock_update_dbt:

            snowflake_actions.update_snowflake(called_by, context_dict, local_folder) 
            assert not mock_update_snowflake.called
            assert not mock_update_dbt.called

if __name__ == "__main__":
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.FunctionTestCase(test_snowflake_execute_select_path))
    test_suite.addTest(unittest.FunctionTestCase(test_snowflake_execute_show_path))
    test_suite.addTest(unittest.FunctionTestCase(test_snowflake_execute_invalidquery))
    test_suite.addTest(unittest.FunctionTestCase(test_snowflake_execute_script_empty_script))
    test_suite.addTest(unittest.FunctionTestCase(test_delete_table_data_executes_expected_queries))
    test_suite.addTest(unittest.FunctionTestCase(test_update_snowflake_from_python_encapsulated))
    test_suite.addTest(unittest.FunctionTestCase(test_update_snowflake_from_dbt_failure))
    test_suite.addTest(unittest.FunctionTestCase(test_update_snowflake_initsnowflake_runs_python_and_dbt))
    test_suite.addTest(unittest.FunctionTestCase(test_update_snowflake_main_with_empty_lists))
    runner = unittest.TextTestRunner()
    runner.run(test_suite)