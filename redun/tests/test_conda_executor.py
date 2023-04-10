import os
import sys
import tempfile
from textwrap import dedent
import time
from typing import List
from pathlib import Path
import pytest

from redun import File, Scheduler, task
from redun.config import Config
from redun.tests.utils import use_tempdir

redun_namespace = "redun.tests.test_executors"


@task(executor="conda")
def task1(x: int, wait: int, count_file: File) -> int:
    import sys
    with count_file.open("a") as out:
        out.write("x")
    while len(count_file.read()) < wait:
        time.sleep(0.01)
    return os.getpid(), sys.version


@task()
def main(count_file: File, conda: str, pip: str) -> List[int]:
    return [task1.options(conda=conda, pip=pip)(1, 2, count_file), task1.options(conda=conda, pip=pip)(2, 2, count_file)]


def create_conda_env_file():
    with tempfile.NamedTemporaryFile(mode="w", prefix="redun_test_env", suffix=".yml", delete=False) as f:
        f.write(dedent(f"""
        dependencies:
            - python=3.9.16
            - graphviz
            - pip
            - pip:
                - {Path(__file__).parent.parent.parent}
        """))
    return f.name

@use_tempdir
def test_conda_executor() -> None:
    """
    Ensure tasks can run in different processes.
    """
    config = Config({"executors.default": {"type": "local"}, "executors.conda": {"type": "conda"}})
    scheduler = Scheduler(config=config)

    # Use the length of file to count that all processes have run before exiting.
    # This ensures we get unique pids.
    env_file = create_conda_env_file()
    count_file = File("count_file")
    count_file.touch()
    dev_requirements = Path(__file__).parent.parent.parent / "requirements-dev.txt"
    [(pid1, pyv1), (pid2, pyv2)] = scheduler.run(main(count_file, conda=env_file, pip=str(dev_requirements)))
    assert pid1 != pid2
    assert pyv1 == pyv2
    assert pyv1.startswith("3.9.16")
    assert pyv1 != sys.version
