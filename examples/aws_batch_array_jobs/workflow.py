import time
from typing import List

from redun import task

redun_namespace = "redun.examples.aws_batch_array_jobs"


@task(executor="batch")
def add(x: int, y: int) -> int:
    time.sleep(10)
    return x + y


@task(executor="batch")
def mult(x: int, y: int) -> int:
    time.sleep(10)
    return x * y


@task
def main(n: int = 10) -> List[int]:
    # The add and mults in the list comprehension below will automatically run
    # in parallel. Further, the AWSBatchExecutor will automatically group
    # jobs together into a Array Jobs for faster submission. This design
    # allows submitting 1000s to 10,000s of jobs within seconds.
    return [mult(add(i, 1), mult(i, 2)) for i in range(n)]
