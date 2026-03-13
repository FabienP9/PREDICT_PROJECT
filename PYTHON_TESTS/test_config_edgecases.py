'''
This tests file concern all functions in the config module.
It units test unexpected path for each function
'''
import unittest
import tempfile
from unittest.mock import patch
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'PYTHON_PREDICT'))
sys.path.insert(0, ROOT)
import config
from testutils import assertExit

def test_onError_final_execute_missing_profiles_file():
    
    # this test the function onError_final_execute with dbt profile missing, must log an issue
    with tempfile.TemporaryDirectory() as tmpdir:
        fake_path = os.path.join(tmpdir, "does_not_exist.yml")
        with patch.object(config, "dbt_profiles_path", fake_path):
            config.onError_final_execute()

def test_onError_final_execute_tmp_folder_cleanup_error():
    
    # this test the function onError_final_execute with a permission error for destroying folders. Must log on issue.
    with tempfile.TemporaryDirectory() as tmpdir:
        bad_dir = os.path.join(tmpdir, "locked")
        os.makedirs(bad_dir)
        # remove permissions to trigger rmtree failure
        os.chmod(bad_dir, 0o400)

        with patch.object(config, "TMPF", bad_dir):
            with patch("shutil.rmtree", side_effect=PermissionError("locked")):
                config.onError_final_execute()

def test_exit_program_decorator_catches_exception_and_exits():
    
    # this test the decorator exit_program with exception. It musts exit the program
    @config.exit_program()
    def faulty_fn():
        raise ValueError("boom")

    with patch.object(config,"onError_final_execute"):
        assertExit(lambda: faulty_fn())

def test_retry_function_eventual_success():
    
    # this test the decorator retry_function with final success
    calls = {"count": 0}

    @config.retry_function(max_attempts=3, delay_secs=0)
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
    @config.retry_function(max_attempts=2, delay_secs=0)
    def always_fail():
        raise RuntimeError("fail!")

    with unittest.TestCase().assertRaises(RuntimeError):
        always_fail()

def test_raise_issue_to_caller_passes_exception():
    
    # this test the decorator raise_issue_to_caller with exception
    @config.raise_issue_to_caller()
    def fail_fn():
        raise ValueError("fail here")

    with unittest.TestCase().assertRaises(ValueError):
        fail_fn()

def test_create_local_folder_when_already_exists():
    
    # this test the function create_local_folder with folder already existing, without errors
    with tempfile.TemporaryDirectory() as tmpdir:
        f1 = os.path.join(tmpdir, "TMPF")
        f2 = os.path.join(tmpdir, "TMPD")
        os.makedirs(f1)
        os.makedirs(f2)
        with patch.object(config, "TMPF", f1), \
             patch.object(config, "TMPD", f2), \
             patch.object(config,"sys_exit") as mock_exit:
            config.create_local_folder()
            # sys_exit should not be called (no error)
            mock_exit.assert_not_called()
            assert os.path.exists(f1)
            assert os.path.exists(f2)

def test_multithreading_run_with_empty_args():
    
    # this test the function multithreading_run with empty args passes. Must return an empty list
    result = config.multithreading_run(lambda x: x, [])
    assert result == []

def test_multithreading_run_with_failures():
    
    # this test the function multithreading_run with failure. Must propagate it to all thread
    def bad_fn(x):
        raise ValueError("oops")
    
    with unittest.TestCase().assertRaisesRegex(ValueError, "oops"):
        config.multithreading_run(bad_fn, [(1,), (2,), (3,)])

def test_check_environment_variable_missingvar():
    
    # this test functions create_local_folder and check_environment_variable with missing var BI_URL
    env_values = {
        "IS_TESTRUN": "1",
        "IS_OUTPUT_AUTO": "1",
        "OVERWRITE_GAMES_STATUS": "0",
        "BI_USERNAME": "user",
        "BI_PASSWORD": "password",
        "SNOWFLAKE_USERNAME": "sfuser",
        "SNOWFLAKE_PASSWORD": "sfpass",
        "LNB_URL": "lnburl",
        "IMGBB_API_KEY": "key123"
    }

    with patch.dict(os.environ, env_values, clear=False):
        assertExit(lambda: config.check_environment_variable("main"))

def test_check_environment_variable_badtype():
    
    # this test functions create_local_folder and check_environment_variable with bad type for OVERWRITE_GAMES_STATUS
    env_values = {
        "IS_TESTRUN": "1",
        "IS_OUTPUT_AUTO": "1",
        "OVERWRITE_GAMES_STATUS": "badtype",
        "BI_URL": "biurl",
        "BI_USERNAME": "user",
        "BI_PASSWORD": "password",
        "SNOWFLAKE_USERNAME": "sfuser",
        "SNOWFLAKE_PASSWORD": "sfpass",
        "LNB_URL": "lnburl",
        "IMGBB_API_KEY": "key123",
        "RCLONE_CONFIG_BASE64": "base64string",
    }

    with patch.dict(os.environ, env_values, clear=False):
        assertExit(lambda: config.check_environment_variable("main"))

if __name__ == '__main__':
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.FunctionTestCase(test_onError_final_execute_missing_profiles_file))
    test_suite.addTest(unittest.FunctionTestCase(test_onError_final_execute_tmp_folder_cleanup_error))
    test_suite.addTest(unittest.FunctionTestCase(test_exit_program_decorator_catches_exception_and_exits))
    test_suite.addTest(unittest.FunctionTestCase(test_retry_function_eventual_success))
    test_suite.addTest(unittest.FunctionTestCase(test_retry_function_exhausts_attempts_and_raises))
    test_suite.addTest(unittest.FunctionTestCase(test_raise_issue_to_caller_passes_exception))
    test_suite.addTest(unittest.FunctionTestCase(test_create_local_folder_when_already_exists))
    test_suite.addTest(unittest.FunctionTestCase(test_multithreading_run_with_empty_args))
    test_suite.addTest(unittest.FunctionTestCase(test_multithreading_run_with_failures))
    test_suite.addTest(unittest.FunctionTestCase(test_check_environment_variable_missingvar))
    test_suite.addTest(unittest.FunctionTestCase(test_check_environment_variable_badtype))
    runner = unittest.TextTestRunner()
    runner.run(test_suite)
