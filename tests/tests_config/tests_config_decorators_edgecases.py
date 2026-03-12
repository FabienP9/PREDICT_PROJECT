'''
This tests file concern all functions in the config_decorators module.
It units test unexpected path
'''
import os
import tempfile
from unittest.mock import patch
import pytest

from src.predict_core.config import config_decorators
import src.predict_core.config.config_variables.config_global_variables as var

def test_execute_finally_on_error_missing_profiles_file():
    
    # this test the function execute_finally_on_error with dbt profile missing, must log an issue
    with tempfile.TemporaryDirectory() as tmpdir:
        fake_path = os.path.join(tmpdir, "does_not_exist.yml")
        with patch.object(var, "DBT_PROFILES_PATH", fake_path):
            config_decorators.execute_finally_on_error()

def test_execute_finally_on_error_tmp_folder_cleanup_error():
    
    # this test the function execute_finally_on_error with a permission error for destroying folders. Must log on issue.
    with tempfile.TemporaryDirectory() as tmpdir:
        bad_dir = os.path.join(tmpdir, "locked")
        os.makedirs(bad_dir)
        # remove permissions to trigger rmtree failure
        os.chmod(bad_dir, 0o400)

        with patch.object(var, "TMPF", bad_dir):
            with patch("shutil.rmtree", side_effect=PermissionError("locked")):
                config_decorators.execute_finally_on_error()

def test_exit_program_decorator_catches_exception_and_exits(assert_exit):
    
    # this test the decorator exit_program with exception. It musts exit the program
    @config_decorators.exit_program()
    def faulty_fn():
        raise ValueError("boom")

    with patch.object(config_decorators,"execute_finally_on_error"):
        assert_exit(lambda: faulty_fn())

def test_retry_function_eventual_success():
    
    # this test the decorator retry_function with final success
    calls = {"count": 0}

    @config_decorators.retry_function(max_attempts=3, delay_secs=0)
    def flaky_fn():
        calls["count"] += 1
        if calls["count"] < 2:
            raise RuntimeError("temporary fail")
        return "ok"

    result = flaky_fn()
    assert result == "ok"
    assert calls["count"] == 2

def test_retry_function_exhausts_attempts_and_raises():
    
    # this test the decorator retry_function with final failure.
    @config_decorators.retry_function(max_attempts=2, delay_secs=0)
    def always_fail():
        raise RuntimeError("fail!")

    with pytest.raises(RuntimeError):
        always_fail()

def test_raise_issue_to_caller_passes_exception():
    
    # this test the decorator raise_issue_to_caller with exception
    @config_decorators.raise_issue_to_caller()
    def fail_fn():
        raise ValueError("fail here")

    with pytest.raises(ValueError):
        fail_fn()
