'''
This tests file concern all functions in the config_multithread module.
It units test the happy path for each function
'''

from src.predict_core.config import config_multithread

def test_multithread_run():
    
    # this test function multithread_run with a function a+b
    def add(a, b):
        return a + b

    args = [(1, 2), (3, 4), (5, 6)]
    results = config_multithread.multithread_run(add, args, thread_max_workers=2)
    assert sorted(results) == [3, 7, 11]

