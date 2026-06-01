'''
This tests file concern all functions in the snowflake_etl_process module.
It units test unexpected paths for each function
'''
import os
import tempfile
from unittest.mock import call, patch, ANY

from src.predict_core.database_interaction.snowflake_etl_process import snowflake_etl_process

def test_delete_table_data_executes_expected_queries(read_yml_as_serie):
    
    # this test the function delete_table_data truncate and remove queries
    sr_snowflake_account_connect = read_yml_as_serie( "snowflake_account_connect.yml")
    schema = "landing"
    table_metadata = [None, "test_table"]

    with patch.object(snowflake_etl_process,'snowflake_execute') as mock_exec:
        snowflake_etl_process.delete_table_data(sr_snowflake_account_connect, schema, table_metadata)
        calls = [call(sr_snowflake_account_connect, ANY,snowflake_etl_process.sql.DATABASE), 
                 call(sr_snowflake_account_connect, ANY,snowflake_etl_process.sql.DATABASE)]
        mock_exec.assert_has_calls(calls, any_order=False)

def test_update_snowflake_from_python_encapsulated(read_yml_as_serie,read_csv):
    
    # this test the function update_snowflake_from_python with encapsulated data
    called_by = 'main'
    sr_snowflake_account_connect = read_yml_as_serie( "snowflake_account_connect.yml")
    table_name = "landing_message_check" #this file is encapsulated according to df_paths
    df_paths = read_csv( "paths.csv")
    local_folder = 'local'

    exp_schema = "landing"
    exp_file_name = "message_check"

    with patch.object(snowflake_etl_process,"snowflake_execute") as mock_snowflake_execute, \
         patch.object(snowflake_etl_process,"create_table_file") :

        snowflake_etl_process.update_snowflake_from_python(called_by,sr_snowflake_account_connect,table_name,df_paths,local_folder)

        assert mock_snowflake_execute.call_count == 2
        q_put_call = mock_snowflake_execute.call_args_list[0][0][1]
        assert str(exp_file_name) in q_put_call
        assert exp_schema in q_put_call
        assert table_name in q_put_call
        assert any("FIELD_OPTIONALLY_ENCLOSED_BY" in c[0][1] for c in mock_snowflake_execute.call_args_list)

def test_update_snowflake_from_dbt_failure(assert_exit,read_yml_as_serie,read_csv):
    
    # this test the function update_snowflake_from_dbt a failing dbt command. Must exit the program
    called_by = "main"
    sr_snowflake_account_connect = read_yml_as_serie( "snowflake_account_connect.yml")
    df_paths = read_csv( "paths.csv")
    lst_dbt_tables=["curated_season"]

    with patch.object(snowflake_etl_process.subprocess,'run') as mock_run, \
         patch.object(snowflake_etl_process,'create_table_file'):

        mock_run.return_value.returncode = 1
        mock_run.return_value.stdout = "bad"
        mock_run.return_value.stderr = "error"

        assert_exit(lambda: snowflake_etl_process.update_snowflake_from_dbt(called_by,sr_snowflake_account_connect,df_paths,lst_dbt_tables))

def test_update_snowflake_initsnowflake_runs_python_and_dbt(read_csv,read_yml_as_serie):
    
    # this test the function update_snowflake called by initsnowflake for python and dbt
    called_by = "init_snowflake"
    context_dict = {
        'df_paths': read_csv( "paths.csv"),
        'sr_snowflake_account_connect': read_yml_as_serie( "snowflake_account_connect.yml"),
        'sr_output_need': read_csv( "output_need_calculate.csv").iloc[0]
    }
    with tempfile.TemporaryDirectory() as tmpdir:
        open(os.path.join(tmpdir, "table_a.csv"), 'w').close()
        open(os.path.join(tmpdir, "table_b.csv"), 'w').close()
        local_folder = tmpdir

        with patch.object(snowflake_etl_process,"update_snowflake_from_python"), \
            patch.object(snowflake_etl_process,"update_snowflake_from_dbt") as mock_update_dbt:

            snowflake_etl_process.update_snowflake(called_by, context_dict, local_folder) 
            mock_update_dbt.assert_called_once()

def test_update_snowflake_main_with_empty_lists(read_csv,read_yml_as_serie):
    
    # this test the function update_snowflake with empty tables list for python and dbt
    called_by = "main"
    context_dict = {
        'df_paths': read_csv( "paths.csv"),
        'sr_snowflake_account_connect': read_yml_as_serie( "snowflake_account_connect.yml"),
        'sr_output_need': read_csv( "output_need_calculate.csv").iloc[0]
    }
    with tempfile.TemporaryDirectory() as tmpdir:
        open(os.path.join(tmpdir, "table_a.csv"), 'w').close()
        open(os.path.join(tmpdir, "table_b.csv"), 'w').close()
        local_folder = tmpdir

        with patch.object(snowflake_etl_process,"get_list_tables_to_update", return_value=([], [])), \
             patch.object(snowflake_etl_process,"update_snowflake_from_python") as mock_update_snowflake, \
             patch.object(snowflake_etl_process,"update_snowflake_from_dbt") as mock_update_dbt:

            snowflake_etl_process.update_snowflake(called_by, context_dict, local_folder) 
            assert not mock_update_snowflake.called
            assert not mock_update_dbt.called

