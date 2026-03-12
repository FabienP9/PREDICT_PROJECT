'''
This tests file concern all functions in the config_multithread module.
It units test unexpected path
'''
import pytest

from src.predict_core.config import config_multithread

def test_multithread_run_with_empty_args():
    
    # this test the function multithread_run with empty args passes. Must return an empty list
    result = config_multithread.multithread_run(lambda x: x, [])
    assert result == []

def test_multithread_run_with_failures():
    
    # this test the function multithread_run with failure. Must propagate it to all thread
    def bad_fn(x):
        raise ValueError("oops")
    
    with pytest.raises(ValueError) as exc_info:
        config_multithread.multithread_run(bad_fn, [(1,), (2,), (3,)])
    assert "oops" in str(exc_info.value)