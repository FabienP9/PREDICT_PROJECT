'''
This tests file concern all functions in the snowflake_etl_process module.
It units test the happy path for each function
'''

import os
import tempfile
from unittest.mock import MagicMock, patch
import pandas as pd

from src.predict_core.database_interaction.snowflake_etl_process import snowflake_etl_process

def test_get_list_tables_to_update(read_csv):
    
    # this test the function get_list_tables_to_update called by main
    called_by = "main"
    df_paths = read_csv("paths.csv")
    message_action="check"

    result = snowflake_etl_process.get_list_tables_to_update(called_by,df_paths,message_action)
    assert result[0] == ['landing_output_need']
    assert result[1] == []

def test_delete_table_data(read_yml_as_serie):
    
    # this test the function delete_table_data
    sr_snowflake_account_connect = read_yml_as_serie("snowflake_account_connect.yml")
    schema = "landing"
    table_metadata = [None, "test_table"]

    with patch.object(snowflake_etl_process,'snowflake_execute') as mock_exec:
        snowflake_etl_process.delete_table_data(sr_snowflake_account_connect, schema, table_metadata)
        assert mock_exec.call_count == 2
        args1 = mock_exec.call_args_list[0][0][1]
        assert "TRUNCATE TABLE" in args1

def test_delete_tables_data_from_python(read_yml_as_serie):

    # this test the function delete_tables_data_from_python
    sr_snowflake_account_connect = read_yml_as_serie("snowflake_account_connect.yml")
    schema = "landing"

    mock_sql = MagicMock()
    mock_sql.snowflake_actions_qListTables = "SELECT * FROM #SCHEMA#.tables"
    mock_sql.SCHEMA = "#SCHEMA#"
    mock_sql.LIST_TABLES_QUERY.replace.return_value = "SELECT * FROM landing.tables"

    with patch.object(snowflake_etl_process,"sql", mock_sql), \
         patch.object(snowflake_etl_process,"snowflake_execute") as mock_snowflake_execute, \
         patch.object(snowflake_etl_process,"multithread_run"):

        mock_snowflake_execute.return_value = [
            {"name": "table1"},
            {"name": "table2"}
        ]

        snowflake_etl_process.delete_tables_data_from_python(sr_snowflake_account_connect, schema)

        mock_snowflake_execute.assert_called_once_with(
            sr_snowflake_account_connect, "SELECT * FROM landing.tables", snowflake_etl_process.sql.DATABASE
        )

def test_create_table_file(read_yml_as_serie):
    
    # this test the function create_table_file
    sr_snowflake_account_connect = read_yml_as_serie("snowflake_account_connect.yml")
    table = "landing_season"
    is_encapsulated = 1
    mock_df = pd.DataFrame({'col': [1]})

    with patch.object(snowflake_etl_process,'snowflake_execute', return_value=mock_df), \
         patch.object(snowflake_etl_process,'create_csv') as mock_create_csv:

        snowflake_etl_process.create_table_file(sr_snowflake_account_connect, table, is_encapsulated)
        mock_create_csv.assert_called_once()

def test_update_snowflake_from_python(read_yml_as_serie, read_csv):

    # this test the function update_snowflake_from_python
    called_by = 'main'
    sr_snowflake_account_connect = read_yml_as_serie("snowflake_account_connect.yml")
    table_name = "landing_season"
    df_paths = read_csv("paths.csv")
    local_folder = 'local'

    exp_schema = "landing"
    exp_file_name = "season"

    with patch.object(snowflake_etl_process,"snowflake_execute") as mock_snowflake_execute, \
         patch.object(snowflake_etl_process,"create_table_file") :

        snowflake_etl_process.update_snowflake_from_python(called_by,sr_snowflake_account_connect,table_name,df_paths,local_folder)

        assert mock_snowflake_execute.call_count == 2
        q_put_call = mock_snowflake_execute.call_args_list[0][0][1]
        assert str(exp_file_name) in q_put_call
        assert exp_schema in q_put_call
        assert table_name in q_put_call

def test_update_snowflake_from_dbt(read_csv,read_yml_as_serie):
    
    # this test the function update_snowflake_from_dbt
    called_by = "main"
    sr_snowflake_account_connect = read_yml_as_serie("snowflake_account_connect.yml")
    df_paths = read_csv("paths.csv")
    lst_dbt_tables=["curated_season"]

    with patch.object(snowflake_etl_process.subprocess,'run') as mock_run, \
         patch.object(snowflake_etl_process,'create_table_file'):

        mock_run.return_value.returncode = 0
        mock_run.return_value.stdout = "Success"
        mock_run.return_value.stderr = ""

        snowflake_etl_process.update_snowflake_from_dbt(
            called_by,sr_snowflake_account_connect,df_paths,lst_dbt_tables
        )

        mock_run.assert_called_once()

def test_update_snowflake_main(read_csv,read_yml_as_serie):

    # this test the function update_snowflake_from_dbt called by main
    called_by = "main"
    context_dict = {
        'df_paths': read_csv("paths.csv"),
        "sr_snowflake_account_connect":  read_yml_as_serie("snowflake_account_connect.yml"),
        'sr_output_need': read_csv("output_need_calculate.csv").iloc[0]
    }
    local_folder = "local"

    with patch.object(snowflake_etl_process,"get_list_tables_to_update") as mock_get_list, \
         patch.object(snowflake_etl_process,"update_snowflake_from_python"), \
         patch.object(snowflake_etl_process,"update_snowflake_from_dbt") as mock_update_dbt:

        mock_get_list.return_value = (["python_table_1"], ["dbt_table_1"])
        snowflake_etl_process.update_snowflake(called_by, context_dict, local_folder)

        mock_get_list.assert_called_once()
        mock_update_dbt.assert_called_once_with(
            called_by,
            context_dict['sr_snowflake_account_connect'],
            context_dict['df_paths'],
            ["dbt_table_1"]
        )

def test_update_snowflake_initsnowflake(read_csv,read_yml_as_serie):

    # this test the function update_snowflake_from_dbt called by init_snowflake
    called_by = "init_snowflake"
    context_dict = {
        'df_paths': read_csv("paths.csv"),
        "sr_snowflake_account_connect":  read_yml_as_serie("snowflake_account_connect.yml"),
        'sr_output_need': read_csv("output_need_calculate.csv").iloc[0]
    }
    
    with tempfile.TemporaryDirectory() as tmpdir:
        open(os.path.join(tmpdir, "table_a.csv"), 'w').close()
        open(os.path.join(tmpdir, "table_b.csv"), 'w').close()
        local_folder = tmpdir

        with patch.object(snowflake_etl_process,"update_snowflake_from_python"), \
            patch.object(snowflake_etl_process,"update_snowflake_from_dbt") as mock_update_dbt:

            snowflake_etl_process.update_snowflake(called_by, context_dict, local_folder) 
            mock_update_dbt.assert_called_once()
