'''
    This module is a utility module for all other modules.
    It defines multiprocessing thread to run some function in parallel
'''
from concurrent.futures import ThreadPoolExecutor, as_completed

def multithread_run(fn, fn_args_list,thread_max_workers = 8):

    '''
        Runs function in parallel using several thread
        Args:
            fn: the name of the function we parallelize
            fn_args_list: the name of the function arguments
            thread_max_workers: number of thread
        Returns:
            The return of the function
    '''
    if len(fn_args_list) == 0:
        return []
    
    results = []
    with ThreadPoolExecutor(thread_max_workers) as executor:
        futures = [executor.submit(fn, *args) for args in fn_args_list]
        for future in as_completed(futures):
            results.append(future.result())
    return results