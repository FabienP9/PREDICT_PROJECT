'''
This tests file concern all functions in the config_decorators module.
It units test happy paths for each function
'''

import pytest
from unittest.mock import patch

from src.predict_core.config import config_decorators
import src.predict_core.config.config_variables.config_global_variables as var

def test_execute_finally_on_error_parametrizes_file(write_yml,read_yml):
    
    # this test function execute_finally_on_error
    mock_profile_file = "profiles.yml"
    mock_profile_content = "account: myacc\ndatabase: mydb\npassword: mypwd\nuser: me\n"
    write_yml(mock_profile_file,mock_profile_content)
    expected_parametrized_profile_file = read_yml("dbt_profile_parametrized_sample.yml")
    with patch.object(var,"DBT_PROFILES_PATH", mock_profile_file):
        config_decorators.execute_finally_on_error()
        content = read_yml(mock_profile_file)
        assert content == expected_parametrized_profile_file

def test_exit_program_decorator_exits(assert_exit):
    
    # this test decorator exit_program, by forcing an error on a created function
    @config_decorators.exit_program()
    def error():
        raise ValueError("boom")

    assert_exit(lambda: error())

def test_retry_function_eventually_succeeds():
    
    # this test decorator retry_function with a success state at second attempt, by calling a created function
    calls = {"n": 0}
    @config_decorators.retry_function(max_attempts=3, delay_secs=0)
    def error_til_success():
        calls["n"] += 1
        if calls["n"] < 2:
            raise ValueError("fail once")
        return "ok"

    result = error_til_success()
    assert result == "ok"
    assert calls["n"] == 2

def test_retry_function_fails_after_max_attempts():

    # this test decorator retry_function eventually failing by forcing an error on a created function
    @config_decorators.retry_function()
    def error():
        raise ValueError("boom")

    with pytest.raises(ValueError):
        error()

def test_raise_issue_to_caller():
    
    # this test decorator raise_issue_to_caller by forcing an error on a created function
    @config_decorators.raise_issue_to_caller()
    def bad():
        raise RuntimeError("problem")

    with pytest.raises(RuntimeError):
        bad()
