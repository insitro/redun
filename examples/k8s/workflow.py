import time
import sys
import random
from redun import File, script, task


@task(executor="k8s")
def waste_cpu(max_i=1000, max_j=1000):
    """Script that wastes cpu (to test container CPU limits)"""
    for j in range(max_i):
        for i in range(max_j):
            a = 5
            a += 1


@task(script=True, executor="k8s")
def script_command(command):
    """Script that runs  command"""
    return command


@task(executor="k8s")
def sysexit1(exit_code):
    """Task that calls sys.exit(code).  This aborts redun oneshot inside the container, causing the job to fail with no useful logging info."""
    sys.exit(code)


@task(executor="k8s")
def task_raises_exception() -> list:
    """Test task that raises exception.  This aborts redun oneshot inside the container, causing the job to fail with no useful logging info."""
    raise RuntimeError


@task(executor="k8s")
def task_sleep(delay=60):
    """Task that sleeps for delay seconds"""
    time.sleep(delay)


@task(script=True, executor="k8s")
def script_sleep(delay):
    """Script that sleeps for 60 seconds"""
    return f"sleep {delay}"


@task(executor="k8s")
def task_io() -> list:
    """Task that writes to stdout and stderr"""
    return [
        "task",
        print("hello stdout"),
        print("hello stderr", file=sys.stderr),
    ]


@task()
def script_inside_task() -> File:
    """Test script embedded in task"""
    # The outer task is just for preparing the script and its arguments.
    return script(  # This inner task will run as a bash script inside the container.
        f"ls /",
        executor="k8s",
        outputs=File("-"),
    )


@task
def main(n: int = 10):
    """Main entrypoint for all tasks and scripts"""
    return [
        "main",
        waste_cpu(1000, 1000),
        script_command("true"),
        task_sleep(60),
        script_sleep(60),
        task_io(),
        script_inside_task(),
        # The remainder of tasks abort redun oneshot inside the container.
        # script_false(),
        # sysexit1(),
        # sysexit0(),
        # task_raises_exception(),
    ]
