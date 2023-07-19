import copy
import os
import sys
from typing import Optional

import pytest

from redun import Scheduler, task
from redun.config import Config
from redun.federated_tasks import (
    federated_task,
    lambda_federated_task,
    launch_federated_task,
    rest_federated_task,
)
from redun.scheduler import SchedulerError
from redun.scheduler_config import postprocess_config
from redun.tests.utils import use_tempdir, wait_until


@task(namespace="redun_test")
def module_task(x, y=3, result_path: Optional[str] = None):
    result = x + y

    # Write to a path, so we can tell if a fire-and-forget run worked.
    if result_path is not None:
        with open(result_path, "w") as fp:
            fp.write(f"{result}")

    return result


@use_tempdir
def test_federated_task() -> None:

    # Use a process executor
    config_dict = {
        "federated_tasks.sample_task": {
            "executor": "process",
            "namespace": "redun_test",
            "task_name": "module_task",
            "load_module": "redun.tests.test_federated_tasks",
            "config_dir": os.path.join(
                os.path.dirname(__file__),
                "test_data",
                "federated_configs",
                "federated_task_config",
            ),
            "new_execution": "True",
        },
        "executors.process": {
            "type": "local",
            "mode": "process",
            "start_method": "spawn",
        },
    }

    config = Config(config_dict=config_dict)
    config = postprocess_config(config, config_dir=os.getcwd())

    scheduler = Scheduler(config)
    scheduler.load()

    federated = federated_task("sample_task", 3)
    assert 6 == scheduler.run(federated)
    federated = federated_task("sample_task", 3, y=8)
    assert 11 == scheduler.run(federated)
    federated = federated_task("sample_task", x=4, y=8)
    assert 12 == scheduler.run(federated)

    # Check error on incorrect task
    federated = federated_task("wrong_task", task_args=[3])
    with pytest.raises(AssertionError, match="Could not find the entrypoint `wrong_task`"):
        assert 6 == scheduler.run(federated)

    # Check behavior on missing executor
    config_dict_broken = copy.deepcopy(config_dict)
    config_dict_broken["federated_tasks.sample_task"]["executor"] = "bad_executor"

    config = Config(config_dict=config_dict_broken)
    config = postprocess_config(config, config_dir=os.getcwd())

    scheduler = Scheduler(config)
    scheduler.load()

    federated = federated_task("sample_task", task_args=[4])
    with pytest.raises(
        SchedulerError,
        match='Unknown executor "bad_executor"',
    ):
        scheduler.run(federated)

    # Check behavior on missing entrypoint configs
    config_dict_broken = copy.deepcopy(config_dict)
    del config_dict_broken["federated_tasks.sample_task"]["executor"]

    config = Config(config_dict=config_dict_broken)
    config = postprocess_config(config, config_dir=os.getcwd())

    scheduler = Scheduler(config)
    scheduler.load()

    federated = federated_task("sample_task", task_args=[4])
    with pytest.raises(
        AssertionError,
        match="Federated task entry `sample_task` does not have the "
        "required keys, missing `{'executor'}`",
    ):
        scheduler.run(federated)


@pytest.mark.skipif(
    (sys.version_info.major, sys.version_info.minor) == (3, 8),
    reason="Test fails on 3.8 due to 3.8-specific python bug.",
)
def test_lambda_federated_task(tmpdir) -> None:
    """Demonstrate that we can use the data from the lambda invocation to start a task."""
    config_dir = os.path.join(
        os.path.dirname(__file__),
        "test_data",
        "federated_configs",
        "federated_demo_config",
    )

    config = Config()
    config.read_path(os.path.join(config_dir, "redun.ini"))
    config = postprocess_config(config, config_dir=os.getcwd())

    scheduler = Scheduler(config)
    scheduler.load()

    result_path = os.path.join(tmpdir, "out.txt")

    # Run the invocation, which packages the inputs for us. Use the scratch redun defaults to.
    execution_id, invocation_data = scheduler.run(
        lambda_federated_task(
            config_name=config_dir,
            entrypoint="sample_task",
            lambda_function_name="fake-lambda-function",
            scratch_prefix="/tmp/redun",
            dryrun=True,
            x=8,
            y=9,
            result_path=result_path,
        )
    )

    assert invocation_data["config_name"].endswith(
        "redun/tests/test_data/federated_configs/federated_demo_config"
    )
    assert invocation_data["entrypoint"] == "sample_task"
    assert invocation_data["input_path"].endswith("input")
    assert invocation_data["execution_id"] == execution_id

    invocation_data["federated_config_path"] = invocation_data.pop("config_name")

    # Now launch the task. Local executors happen to work because the process pool is shut down
    # gently.
    launch_federated_task(**invocation_data)

    # The launched task is async to us, so let it finish.
    wait_until(lambda: os.path.isfile(result_path), timeout=5)

    with open(result_path, "r") as fp:
        result = fp.read()
    assert result == "17"


def test_rest_federated_task(tmpdir) -> None:
    """Demonstrate that we can use the data from the REST submission to start a task."""
    config_dir = os.path.join(
        os.path.dirname(__file__),
        "test_data",
        "federated_configs",
        "federated_demo_config",
    )

    config = Config()
    config.read_path(os.path.join(config_dir, "redun.ini"))
    config = postprocess_config(config, config_dir=os.getcwd())

    scheduler = Scheduler(config)
    scheduler.load()

    result_path = os.path.join(tmpdir, "out.txt")

    # Run the submission, which packages the inputs for us. Use the scratch redun defaults to.
    execution_id, rest_data = scheduler.run(
        rest_federated_task(
            config_name=config_dir,
            entrypoint="sample_task",
            url="fake",
            scratch_prefix="/tmp/redun",
            dryrun=True,
            x=2,
            y=9,
            result_path=result_path,
        )
    )

    assert rest_data["config_name"].endswith(
        "redun/tests/test_data/federated_configs/federated_demo_config"
    )
    assert rest_data["entrypoint"] == "sample_task"
    assert rest_data["input_path"].endswith("input")
    assert rest_data["execution_id"] == execution_id

    rest_data["federated_config_path"] = rest_data.pop("config_name")

    # Now launch the task. Local executors happen to work because the process pool is shut down
    # gently.
    launch_federated_task(**rest_data)

    # The launched task is async to us, so let it finish.
    wait_until(lambda: os.path.isfile(result_path), timeout=10)

    with open(result_path, "r") as fp:
        result = fp.read()
    assert result == "11"
