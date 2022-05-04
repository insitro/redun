"""
To more accurately simulate subrun() usage, all user tasks are defined in this separate module
"""

import os
import time

from redun.file import File
from redun.scheduler import subrun
from redun.task import task

# This is needed to associate the separate scripts/workflow file with this unit test and to
# prevent pre-test fixture from deleting the tasks defined in this file.
redun_namespace = "test_subrun"


@task(executor="default")
def root_subtask(start_method: str, expected_chars: int, count_file: File):
    return (
        [
            os.getpid(),  # PID of the sub-scheduler as this task is evaluated by thread executor
            subtask(1, start_method, 2, count_file),
            subtask(2, start_method, 2, count_file),
            subtask(3, start_method, 2, count_file),
        ],
    )


@task(executor="process_sub")
def subtask(x: int, start_method: str, expected_chars: int, count_file: File):
    # Since we can't serialize and pass a Barrier object, we use a simple coordination
    # mechanism: each subtask writes a single char to the count file and waits until
    # expected_chars are present in the count file.
    with count_file.open("a") as out:
        out.write("x")
    while len(count_file.read()) < expected_chars:
        time.sleep(0.01)
    return os.getpid()


@task(executor="default", cache=False)
def main(start_method: str, config_dict: dict):
    # Use the length of file to count that all processes have run before exiting.
    # This ensures we get unique pids.
    count_file = File("count_file")
    count_file.touch()

    return subrun(
        [
            root_subtask(start_method, 2, count_file),
            os.getpid(),
        ],
        executor="process_main",
        config=config_dict,
        # The following kwargs don't do anything and merely serve to showcase how to pass
        # executor-specific task options to the Executor of the redun task that runs the
        # subscheduler.
        memory=32,
        vcpus=4,
    )
