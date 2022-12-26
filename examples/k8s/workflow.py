import sys
import time

from redun import File, script, task

redun_namespace = "redun.examples.k8s"


@task(script=True, executor="k8s")
def script_command(command):
    """Script that runs  command"""
    return command


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


@task
def script_inside_task() -> File:
    """Test script embedded in task"""
    # The outer task is just for preparing the script and its arguments.
    return script(  # This inner task will run as a bash script inside the container.
        "ls /",
        executor="k8s",
        outputs=File("-"),
    )


@task
def main(n: int = 10):
    """Main entrypoint for all tasks and scripts"""
    return [
        "main",
        script_command("true"),
        task_sleep(1),
        script_sleep(10),
        task_io(),
        script_inside_task(),
    ]
