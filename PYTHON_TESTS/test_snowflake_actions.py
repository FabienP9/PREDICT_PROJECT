'''
This tests file concern all functions in the snowflake_actions module.
It units test the happy path for each function
'''

import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from pandas.testing import assert_frame_equal
import tempfile
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'PYTHON_PREDICT'))
sys.path.insert(0, ROOT)
import snowflake_actions

def test_snowflake_connect():
    
    # this test the function snowflake_connect
    sr_snowflake_account = pd.read_csv("materials/snowflake_account_connect.csv").iloc[0]

    mock_conn = MagicMock()
    mock_conn.is_closed.return_value = False

    with patch.object(snowflake_actions.snowflake.connector,'connect', return_value=mock_conn) as mock_connect, \
         patch.object(snowflake_actions.os,'getenv', side_effect=lambda k: 'user' if k == 'SNOWFLAKE_USERNAME' else 'pass' if k == 'SNOWFLAKE_PASSWORD' else '0'):
        
        conn = snowflake_actions.snowflake_connect(sr_snowflake_account)

        assert conn == mock_conn
        mock_connect.assert_called_once()

def test_snowflake_execute():
    
    # this test the function snowflake_execute
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
        assert_frame_equal(result.reset_index(drop=True), pd.DataFrame({"col": [1, 2]}).reset_index(drop=True))

def test_snowflake_execute_script_uses_prod_db():
    
    # this test the function snowflake_execute_script with prod database
    sr_snowflake_account = pd.read_csv("materials/snowflake_account_connect.csv").iloc[0]
    script = "SELECT * FROM #DATABASE#.TABLE1;"
    expected_script = "SELECT * FROM PREDICT_PROD.TABLE1;"

    with patch.object(snowflake_actions.os,"getenv", return_value="0"):  # simulate prod run
        with patch.object(snowflake_actions,"snowflake_connect") as mock_connect:
            mock_connection = mock_connect.return_value
            snowflake_actions.snowflake_execute_script(sr_snowflake_account, script)

            mock_connect.assert_called_once_with(sr_snowflake_account)
            mock_connection.execute_string.assert_called_once_with(expected_script)

def test_snowflake_execute_script_uses_test_db():
    
    # this test the function snowflake_execute_script with test database
    sr_snowflake_account = pd.read_csv("materials/snowflake_account_connect.csv").iloc[0]
    script = "SELECT * FROM #DATABASE#.TABLE1;"
    expected_script = "SELECT * FROM PREDICT_TEST.TABLE1;"

    with patch.object(snowflake_actions.os,"getenv", return_value="1"):  # simulate test run
        with patch.object(snowflake_actions,"snowflake_connect") as mock_connect:
            mock_connection = mock_connect.return_value
            snowflake_actions.snowflake_execute_script(sr_snowflake_account, script)

            mock_connect.assert_called_once_with(sr_snowflake_account)
            mock_connection.execute_string.assert_called_once_with(expected_script)

def test_get_list_tables_to_update():
    
    # this test the function get_list_tables_to_update called by main
    called_by = "main"
    df_paths = pd.read_csv("materials/paths.csv")
    message_action="check"

    result = snowflake_actions.get_list_tables_to_update(called_by,df_paths,message_action)
    assert result[0] == ['landing_output_need']
    assert result[1] == []

def test_delete_table_data():
    
    # this test the function delete_table_data
    sr_snowflake_account = pd.read_csv("materials/snowflake_account_connect.csv").iloc[0]
    schema = "landing"
    table_metadata = [None, "test_table"]

    with patch.object(snowflake_actions,'snowflake_execute') as mock_exec:
        snowflake_actions.delete_table_data(sr_snowflake_account, schema, table_metadata)
        assert mock_exec.call_count == 2
        args1 = mock_exec.call_args_list[0][0][1]
        assert "TRUNCATE TABLE" in args1

def test_delete_tables_data_from_python():

    # this test the function delete_tables_data_from_python
    sr_snowflake_account = pd.read_csv("materials/snowflake_account_connect.csv").iloc[0]
    schema = "landing"

    mock_sql = MagicMock()
    mock_sql.snowflake_actions_qListTables = "SELECT * FROM #SCHEMA#.tables"

    with patch.object(snowflake_actions,"sqlQ", mock_sql), \
         patch.object(snowflake_actions,"snowflake_execute") as mock_snowflake_execute, \
         patch.object(snowflake_actions.config,"multithreading_run"):

        mock_snowflake_execute.return_value = [
            {"name": "table1"},
            {"name": "table2"}
        ]

        snowflake_actions.delete_tables_data_from_python(sr_snowflake_account, schema)

        
        mock_snowflake_execute.assert_called_once_with(
            sr_snowflake_account, "SELECT * FROM landing.tables"
        )

def test_create_table_file():
    
    # this test the function create_table_file
    sr_snowflake_account = pd.read_csv("materials/snowflake_account_connect.csv").iloc[0]
    table = "landing_season"
    is_encapsulated = 1
    mock_df = pd.DataFrame({'col': [1]})

    with patch.object(snowflake_actions,'snowflake_execute', return_value=mock_df), \
         patch.object(snowflake_actions,'create_csv') as mock_create_csv:

        snowflake_actions.create_table_file(sr_snowflake_account, table, is_encapsulated)
        mock_create_csv.assert_called_once()

def test_update_snowflake_from_python():

    # this test the function update_snowflake_from_python
    called_by = 'main'
    sr_snowflake_account = pd.read_csv("materials/snowflake_account_connect.csv").iloc[0]
    table_name = "landing_season"
    df_paths = pd.read_csv("materials/paths.csv")
    local_folder = 'local'

    exp_schema = "landing"
    exp_file_name = "season"

    with patch.object(snowflake_actions,"snowflake_execute") as mock_snowflake_execute, \
         patch.object(snowflake_actions,"create_table_file") :

        snowflake_actions.update_snowflake_from_python(called_by,sr_snowflake_account,table_name,df_paths,local_folder)

        assert mock_snowflake_execute.call_count == 2
        qPut_call = mock_snowflake_execute.call_args_list[0][0][1]
        assert str(exp_file_name) in qPut_call
        assert exp_schema in qPut_call
        assert table_name in qPut_call

def test_update_snowflake_from_dbt():
    
    # this test the function update_snowflake_from_dbt
    called_by = "main"
    sr_snowflake_account = pd.read_csv("materials/snowflake_account_connect.csv").iloc[0]
    df_paths = pd.read_csv("materials/paths.csv")
    lst_dbt_tables=["curated_season"]

    with patch.object(snowflake_actions.subprocess,'run') as mock_run, \
         patch.object(snowflake_actions,'create_table_file'):

        mock_run.return_value.returncode = 0
        mock_run.return_value.stdout = "Success"
        mock_run.return_value.stderr = ""

        snowflake_actions.update_snowflake_from_dbt(
            called_by,sr_snowflake_account,df_paths,lst_dbt_tables
        )

        mock_run.assert_called_once()

def test_update_snowflake_main():

    # this test the function update_snowflake_from_dbt called by main
    called_by = "main"
    context_dict = {
        'df_paths': pd.read_csv("materials/paths.csv"),
        'sr_snowflake_account_connect': pd.read_csv("materials/snowflake_account_connect.csv").iloc[0],
        'sr_output_need': pd.read_csv("materials/output_need_calculate.csv").iloc[0]
    }
    local_folder = "local"

    with patch.object(snowflake_actions,"get_list_tables_to_update") as mock_get_list, \
         patch.object(snowflake_actions,"update_snowflake_from_python"), \
         patch.object(snowflake_actions,"update_snowflake_from_dbt") as mock_update_dbt:

        mock_get_list.return_value = (["python_table_1"], ["dbt_table_1"])
        snowflake_actions.update_snowflake(called_by, context_dict, local_folder)

        mock_get_list.assert_called_once()
        mock_update_dbt.assert_called_once_with(
            called_by,
            context_dict['sr_snowflake_account_connect'],
            context_dict['df_paths'],
            ["dbt_table_1"]
        )

def test_update_snowflake_initsnowflake():

    # this test the function update_snowflake_from_dbt called by init_snowflake
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

if __name__ == "__main__":
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.FunctionTestCase(test_snowflake_connect))
    test_suite.addTest(unittest.FunctionTestCase(test_snowflake_execute))
    test_suite.addTest(unittest.FunctionTestCase(test_snowflake_execute_script_uses_prod_db))
    test_suite.addTest(unittest.FunctionTestCase(test_snowflake_execute_script_uses_test_db))
    test_suite.addTest(unittest.FunctionTestCase(test_get_list_tables_to_update))
    test_suite.addTest(unittest.FunctionTestCase(test_delete_table_data))
    test_suite.addTest(unittest.FunctionTestCase(test_delete_tables_data_from_python))
    test_suite.addTest(unittest.FunctionTestCase(test_create_table_file))
    test_suite.addTest(unittest.FunctionTestCase(test_update_snowflake_from_python))
    test_suite.addTest(unittest.FunctionTestCase(test_update_snowflake_from_dbt))
    test_suite.addTest(unittest.FunctionTestCase(test_update_snowflake_main))
    test_suite.addTest(unittest.FunctionTestCase(test_update_snowflake_initsnowflake))
    runner = unittest.TextTestRunner()
    runner.run(test_suite)