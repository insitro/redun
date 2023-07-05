import os
import subprocess
import threading
import time
from typing import Dict, List, Tuple

from redun import task

redun_namespace = "redun.examples.conda"


@task(executor="conda", conda="my_env")
def run_in_conda_process(x: int) -> Tuple[int, int, int]:
    # Return the process id (pid) to prove this task runs in its own process.
    time.sleep(1)
    return (
        os.getpid(),
        threading.get_ident(),
        subprocess.run(["python", "--version"], capture_output=True).stdout.decode(),
        x + 1,
    )


@task(executor="conda", conda=os.path.join(os.path.dirname(__file__), "my_py310_redun_env.yml"))
def run_in_conda_process_with_yml(x: int) -> Tuple[int, int, int]:
    # Return the process id (pid) to prove this task runs in its own process.
    time.sleep(1)
    return (
        os.getpid(),
        threading.get_ident(),
        subprocess.run(["python", "--version"], capture_output=True).stdout.decode(),
        x + 1,
    )


@task(
    executor="conda",
    conda=os.path.join(os.path.dirname(__file__), "my_py310_redun_env.yml"),
    pip="pandas==2.0.0",
)
def run_in_conda_process_with_yml_and_pip(x: int) -> Tuple[int, int, int]:
    import pandas

    # Return the process id (pid) to prove this task runs in its own process.
    time.sleep(1)
    return (
        os.getpid(),
        threading.get_ident(),
        subprocess.run(["python", "--version"], capture_output=True).stdout.decode(),
        x + 1,
        pandas.__version__,
    )


@task(
    executor="conda",
    conda=os.path.join(os.path.dirname(__file__), "my_py310_redun_env.yml"),
    pip=["pandas==2.0.0", "colorama==0.4.6"],
)
def run_in_conda_process_with_yml_and_pip2(x: int) -> Tuple[int, int, int]:
    import colorama
    import pandas

    # Return the process id (pid) to prove this task runs in its own process.
    time.sleep(1)
    return (
        os.getpid(),
        threading.get_ident(),
        subprocess.run(["python", "--version"], capture_output=True).stdout.decode(),
        x + 1,
        pandas.__version__,
        colorama.__version__,
    )


@task(
    executor="conda", conda=os.path.join(os.path.dirname(__file__), "my_py27_env.yml"), script=True
)
def run_script_in_conda_process_with_yml(x: int) -> str:
    return f"""
    #!/usr/bin/env python
    import os
    import thread
    import subprocess
    import sys

    print os.getpid(), thread.get_ident(), sys.version, {x + 1}
    """


@task()
def regular_task(x):
    time.sleep(1)
    return (
        os.getpid(),
        threading.get_ident(),
        subprocess.run(["python", "--version"], capture_output=True).stdout.decode(),
        x + 1,
    )


@task()
def main(n: int = 5) -> Dict[str, List[Tuple[int, int, int]]]:
    data = list(range(n))

    # Fanout across many processes.
    result = [run_in_conda_process_with_yml(i) for i in data]

    # Fanout over threads.
    result2 = [regular_task(i) for i in data]

    # Specify executor at call-time.
    result3 = [
        regular_task.options(
            executor="conda",
            conda=os.path.join(os.path.dirname(__file__), "my_py310_redun_env.yml"),
        )(n + i)
        for i in data
    ]

    # check pip
    result4 = [run_in_conda_process_with_yml_and_pip(i) for i in data]

    # check pip with 2 packages
    result5 = [run_in_conda_process_with_yml_and_pip2(i) for i in data]

    # check script
    result6 = [run_script_in_conda_process_with_yml(i) for i in data]

    return {
        "run_in_conda_process_with_yml": result,
        "regular_task": result2,
        "regular_task_options": result3,
        "run_in_conda_process_with_yml_and_pip": result4,
        "run_in_conda_process_with_yml_and_pip2": result5,
        "run_script_in_conda_process_with_yml": result6,
    }
