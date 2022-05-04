import os
import threading
import time
from typing import List, cast

import pytest

from redun import File, Scheduler, task
from redun.config import Config
from redun.executors.local import LocalExecutor
from redun.tests.utils import use_tempdir

redun_namespace = "redun.tests.test_executors"


@task()
def task1(x: int, wait: int, count_file: File) -> int:
    with count_file.open("a") as out:
        out.write("x")
    while len(count_file.read()) < wait:
        time.sleep(0.01)
    return os.getpid()


@task()
def main(count_file: File) -> List[int]:
    return [task1(1, 2, count_file), task1(2, 2, count_file)]


@task()
def bad_task():
    raise ValueError("boom")


@use_tempdir
def test_process_executor() -> None:
    """
    Ensure tasks can run in different processes.
    """
    config = Config({"executors.default": {"type": "local", "mode": "process"}})
    scheduler = Scheduler(config=config)

    assert cast(LocalExecutor, scheduler.executors["default"]).mode == "process"

    # Use the length of file to count that all processes have run before exiting.
    # This ensures we get unique pids.
    count_file = File("count_file")
    count_file.touch()
    [pid1, pid2] = scheduler.run(main(count_file))
    assert pid1 != pid2

    # Errors should be raised across processes.
    with pytest.raises(ValueError):
        scheduler.run(bad_task())


@task(executor="process")
def task2(x: int, wait: int, count_file: File) -> int:
    with count_file.open("a") as out:
        out.write("x")
    while len(count_file.read()) < wait:
        time.sleep(0.01)
    return os.getpid()


@task()
def main2(count_file: File) -> List[int]:
    return [task2(1, 2, count_file), task2(2, 2, count_file)]


@use_tempdir
def test_process_executor_options() -> None:
    """
    Choose local executor mode using task_options.
    """
    scheduler = Scheduler()
    assert cast(LocalExecutor, scheduler.executors["default"]).mode == "thread"

    # Use the length of file to count that all processes have run before exiting.
    # This ensures we get unique pids.
    count_file = File("count_file")
    count_file.touch()
    [pid1, pid2] = scheduler.run(main2(count_file))
    assert pid1 != pid2


@task(executor="process", script=True)
def task3(x: int, wait: int, count_file: File) -> str:
    with count_file.open("a") as out:
        out.write("x")
    while len(count_file.read()) < wait:
        time.sleep(0.01)
    pid = os.getpid()
    return "echo {}".format(pid)


@task()
def main3(count_file: File) -> List[str]:
    return [task3(1, 2, count_file), task3(2, 2, count_file)]


@use_tempdir
def test_process_executor_script() -> None:
    """
    Ensure script tasks can run in different processes.
    """

    config = Config({"executors.default": {"type": "local", "mode": "process"}})
    scheduler = Scheduler(config=config)

    assert cast(LocalExecutor, scheduler.executors["default"]).mode == "process"

    # Use the length of file to count that all processes have run before exiting.
    # This ensures we get unique pids.
    count_file = File("count_file")
    count_file.touch()
    [pid1, pid2] = scheduler.run(main3(count_file))
    assert pid1 != pid2
    assert pid1.strip().isdigit()
    assert pid2.strip().isdigit()


@task(executor="process")
def process_task(x: int):
    time.sleep(0.1)
    return {"thread_count": threading.active_count(), "pid": os.getpid()}


@task()
def thread_task(x: int) -> dict:
    return {"thread_count": threading.active_count(), "process": process_task(x), "data": x}


@task()
def main4() -> List[dict]:
    return [thread_task(i) for i in range(10)]


def test_thread_count() -> None:
    """
    Ensure there are no extra threads in subprocesses.

    If creating a new process copies all the extra threads it could lead to
    unstable results.
    """
    scheduler = Scheduler()
    results = scheduler.run(main4())

    for row in results:
        assert row["process"]["thread_count"] == 1

    # The thread tasks will be in a process with multiple threads.
    assert max(row["thread_count"] for row in results) > 1
