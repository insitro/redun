"""k8s example that demonstrates a job that times out before the task completes"""
import time

from redun import task


redun_namespace = "redun.examples.k8s"


@task(executor="k8s")
def slow_task() -> str:
    """Sleep for 100 seconds since the minimum timeout we can set on batch job timeout is 60 seconds
    and we want to be well over that to avoid any sort of edge case."""
    time.sleep(100)


@task()
def main(job_timeout: int = 0) -> str:
    """Create a task with timeout option set"""
    task_options = {"timeout": job_timeout} if job_timeout else {}
    return slow_task.options(**task_options)()
