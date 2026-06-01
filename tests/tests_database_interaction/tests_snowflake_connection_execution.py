'''
This tests file concern all functions in the snowflake_connection_execution module.
It units test the happy path for each function
'''

from unittest.mock import MagicMock, patch
from pandas.testing import assert_frame_equal
import pandas as pd

from src.predict_core.database_interaction import snowflake_connection_execution

def test_snowflake_connect(read_yml_as_serie):
    
    # this test the function snowflake_connect
    sr_snowflake_account_connect = read_yml_as_serie("snowflake_account_connect.yml")

    mock_conn = MagicMock()
    mock_conn.is_closed.return_value = False

    with patch.object(snowflake_connection_execution.snowflake.connector,'connect', return_value=mock_conn) as mock_connect, \
         patch.object(snowflake_connection_execution.os,'getenv', 
                      side_effect=lambda k: {'SNOWFLAKE_USERNAME': 'user', 'SNOWFLAKE_PASSWORD': 'pass'}.get(k, '0')): # NOSONAR
        
        conn = snowflake_connection_execution.snowflake_connect(sr_snowflake_account_connect)

        assert conn == mock_conn
        mock_connect.assert_called_once()

def test_snowflake_execute(read_yml_as_serie):
    
    # this test the function snowflake_execute
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
        assert_frame_equal(result.reset_index(drop=True), pd.DataFrame({"col": [1, 2]}).reset_index(drop=True))

def test_snowflake_execute_script_uses_prod_db(read_yml_as_serie):
    
    # this test the function snowflake_execute_script with prod database
    sr_snowflake_account_connect = read_yml_as_serie("snowflake_account_connect.yml")
    script = "SELECT * FROM #DATABASE#.TABLE1;"
    expected_script = "SELECT * FROM PREDICT_PROD.TABLE1;"

    with patch.object(snowflake_connection_execution.os,"getenv", return_value="0"):  # simulate prod run
        with patch.object(snowflake_connection_execution,"snowflake_connect") as mock_connect:
            mock_connection = mock_connect.return_value
            snowflake_connection_execution.snowflake_execute_script(sr_snowflake_account_connect, script, "#DATABASE#")

            mock_connect.assert_called_once_with(sr_snowflake_account_connect)
            mock_connection.execute_string.assert_called_once_with(expected_script)

def test_snowflake_execute_script_uses_test_db(read_yml_as_serie):
    
    # this test the function snowflake_execute_script with test database
    sr_snowflake_account_connect = read_yml_as_serie("snowflake_account_connect.yml")
    script = "SELECT * FROM #DATABASE#.TABLE1;"
    expected_script = "SELECT * FROM PREDICT_TEST.TABLE1;"

    with patch.object(snowflake_connection_execution.os,"getenv", return_value="1"):  # simulate test run
        with patch.object(snowflake_connection_execution,"snowflake_connect") as mock_connect:
            mock_connection = mock_connect.return_value
            snowflake_connection_execution.snowflake_execute_script(sr_snowflake_account_connect, script,"#DATABASE#")

            mock_connect.assert_called_once_with(sr_snowflake_account_connect)
            mock_connection.execute_string.assert_called_once_with(expected_script)
