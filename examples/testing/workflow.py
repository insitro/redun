# Example workflow with a main task and subtasks (add).
from redun import task

redun_namespace = "redun.examples.testing"


@task()
def add(a: int, b: int) -> int:
    return a + b


@task()
def add4(a: int, b: int, c: int, d: int) -> int:
    return add(add(a, b), add(c, d))


@task()
def divide(a: float, b: float) -> float:
    return a / b


@task(executor="batch")
def batch_task(x: int) -> int:
    # Let's pretend this task is intended to run on AWS Batch.
    return x + 1


@task()
def main(x: int) -> int:
    y = add(x, x)
    z = add4(y, x, 1, 2)
    return z
