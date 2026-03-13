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


@task(executor="k8s")
def task_add(x: int, y: int) -> int:
    """Task that adds two numbers (used to demonstrate array jobs)"""
    return x + y


@task
def act(n: int = 10):
    """Execute all k8s tasks and collect results."""
    return {
        "script_command": script_command("echo hello"),
        "task_sleep": task_sleep(1),
        "script_sleep": script_sleep(1),
        "task_io": task_io(),
        "script_inside_task": script_inside_task(),
        "array_job": [task_add(i, 1) for i in range(n)],
    }


@task
def assert_results(results: dict):
    """Verify all k8s task results are correct."""
    assert results["script_command"] == [0, b"hello\n"], (
        f"script_command: {results['script_command']!r}"
    )
    assert results["task_sleep"] is None, f"task_sleep: {results['task_sleep']!r}"
    assert results["script_sleep"] == [0, b""], f"script_sleep: {results['script_sleep']!r}"
    assert results["task_io"] == ["task", None, None], f"task_io: {results['task_io']!r}"
    sit = results["script_inside_task"]
    assert isinstance(sit, list) and len(sit) == 2 and sit[0] == 0, f"script_inside_task: {sit!r}"
    expected_array = list(range(1, len(results["array_job"]) + 1))
    assert results["array_job"] == expected_array, f"array_job: {results['array_job']!r}"
    return "All assertions passed!"


@task
def main(n: int = 10):
    """Main entrypoint: run all k8s tasks then verify results."""
    return assert_results(act(n))
