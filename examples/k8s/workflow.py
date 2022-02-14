import time
import sys
import random
from redun import File, script, task


@task(executor="k8s")
def waste_cpu():
    """Script that wastes cpu (to test container CPU limits)"""
    for j in range(100):
        for i in range(10000000):
            a = 5
            a += 1


@task(script=True, executor="k8s")
def script_false():
    """Script that runs the false command"""
    return "false"


@task(script=True, executor="k8s")
def script_true():
    """Script that runs the true command"""
    return "true"


@task(executor="k8s")
def sysexit1():
    """Task that calls sys.exit(1).  This aborts redun oneshot inside the container, causing the job to fail with no useful logging info."""
    sys.exit(1)


@task(executor="k8s")
def sysexit0():
    """Task that calls sys.exit(0).  This aborts redun oneshot inside the container, causing the job to fail with no useful logging info"""
    sys.exit(0)


@task(executor="k8s")
def task_raises_exception() -> list:
    """Test task that raises exception.  This aborts redun oneshot inside the container, causing the job to fail with no useful logging info."""
    raise RuntimeError


@task(executor="k8s")
def task_sleep():
    """Task that sleeps for 60 seconds"""
    time.sleep(60)


@task(script=True, executor="k8s")
def script_sleep():
    """Script that sleeps for 60 seconds"""
    return "sleep 60"


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
        waste_cpu(),
        script_true(),
        task_sleep(),
        script_sleep(),
        task_io(),
        script_inside_task(),
        # The remainder of tasks abort redun oneshot inside the container.
        # script_false(),
        # sysexit1(),
        # sysexit0(),
        # task_raises_exception(),
    ]
