"""
Example of using fork_thread() and join_thread() to perform computations that run 'parallel'
to the overall redun job tree.
"""

import time

from redun import task
from redun.scheduler import fork_thread, join_thread

redun_namespace = "redun.examples.thread"


@task
def double(x):
    time.sleep(5)  # Make this job run longer than usual.
    return 2 * x


@task
def make_thread(x):
    # Compute the expression `double(x)` in a background redun "thread".
    thread = fork_thread(double(x))
    # Here, we conclude `make_thread` but `double` is still running.
    return thread


@task
def take_thread(thread):
    # Here, we join up with the thread to get its result. `take_thread` will only
    # conclude once `double` concludes.
    result = join_thread(thread)
    return result


@task
def main():
    thread = make_thread(10)
    result = take_thread(thread)
    return result
