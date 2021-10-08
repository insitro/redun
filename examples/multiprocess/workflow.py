import os
import time
import threading
from typing import Dict, List, Tuple

from redun import task


@task(executor="process")
def run_in_subprocess(x: int) -> Tuple[int, int]:
    # Return the process id (pid) to prove this task runs in its own process.
    time.sleep(1)
    return (os.getpid(), threading.get_ident(), x + 1)


@task()
def regular_task(x):
    time.sleep(1)
    return (os.getpid(), threading.get_ident(), x + 1)


@task()
def main(n: int = 50) -> Dict[str, List[Tuple[int, int]]]:
    data = list(range(n))

    # Fanout across many processes.
    result = [run_in_subprocess(i) for i in data]

    # Fanout over threads.
    result2 = [regular_task(i) for i in data]

    # Specify executor at call-time.
    result3 = [regular_task.options(executor="process")(n + i) for i in data]

    return {
        "run_in_subprocess": result,
        "regular_task": result2,
        "regular_task_options": result3,
    }
