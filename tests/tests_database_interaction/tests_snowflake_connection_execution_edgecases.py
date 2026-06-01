'''
This tests file concern all functions in the snowflake_connection_execution module.
It units test unexpected paths
'''
from unittest.mock import MagicMock, patch
import pandas as pd

from src.predict_core.database_interaction import snowflake_connection_execution

def test_snowflake_execute_select_path(read_yml_as_serie):
    
    # this test the function snowflake_execute select path
    sr_snowflake_account_connect = read_yml_as_serie("snowflake_account_connect.yml")
    query = "SELECT * FROM #DATABASE#.table"

    mock_cursor = MagicMock()
    mock_cursor.__enter__.return_value = mock_cursor
    mock_cursor.fetch_pandas_all.return_value = pd.DataFrame({"col": [1, 2]})
    
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.is_closed.return_value = False

    with patch.object(snowflake_connection_execution,'snowflake_connect', return_value=mock_conn), \
         patch.object(snowflake_connection_execution.os,'getenv', return_value='0'):

        result = snowflake_connection_execution.snowflake_execute(sr_snowflake_account_connect, query, "#DATABASE#")
        assert isinstance(result, pd.DataFrame)

def test_snowflake_execute_show_path(read_yml_as_serie):
    
    # this test the function snowflake_execute for show command
    sr_snowflake_account_connect = read_yml_as_serie("snowflake_account_connect.yml")
    query = "SHOW TABLES;"

    mock_cursor = MagicMock()
    mock_cursor.__enter__.return_value = mock_cursor
    mock_cursor.fetchall.return_value = [("t1",)]
    
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.is_closed.return_value = False

    with patch.object(snowflake_connection_execution,'snowflake_connect', return_value=mock_conn), \
         patch.object(snowflake_connection_execution.os,'getenv', return_value='0'):

        result = snowflake_connection_execution.snowflake_execute(sr_snowflake_account_connect, query, "#DATABASE#")
        assert isinstance(result, list)

def test_snowflake_execute_invalidquery(read_yml_as_serie,assert_exit):
    
    # this test the function snowflake_execute with invalid query. Must exit the program
    sr_snowflake_account_connect = read_yml_as_serie("snowflake_account_connect.yml")
    query = "INVALID QUERY;"

    mock_cursor = MagicMock()
    mock_cursor.__enter__.return_value = mock_cursor
    
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.is_closed.return_value = False

    with patch.object(snowflake_connection_execution,'snowflake_connect', return_value=mock_conn), \
         patch.object(snowflake_connection_execution.os,'getenv', return_value='0'),\
         patch.object(snowflake_connection_execution.sqlglot,'parse_one') as mock_parse_one:

        mock_parse_one.return_value = object()
        assert_exit(lambda: snowflake_connection_execution.snowflake_execute(sr_snowflake_account_connect, query, "#DATABASE#"))

def test_snowflake_execute_script_empty_script(read_yml_as_serie):
    
    # this test the function snowflake_execute_script with empty script. Must run but do nothing
    sr_snowflake_account_connect = read_yml_as_serie("snowflake_account_connect.yml")
    script = ""

    with patch.object(snowflake_connection_execution.os,"getenv", return_value="0"):  
        with patch.object(snowflake_connection_execution,"snowflake_connect") as mock_connect:
            mock_connection = mock_connect.return_value
            snowflake_connection_execution.snowflake_execute_script(sr_snowflake_account_connect, script, "#DATABASE#")
            mock_connection.execute_string.assert_called_once_with("")