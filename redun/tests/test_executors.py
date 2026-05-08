import pytest

from redun import Scheduler, task
from redun.config import Config, create_config_section
from redun.executors.aws_batch import AWSBatchExecutor
from redun.executors.base import (
    ExecutorError,
    _executor_providers,
    get_executor_class,
    get_executor_from_config,
)
from redun.executors.local import LocalExecutor


def test_default_executor(scheduler: Scheduler) -> None:
    """
    Without executor specified, default executor should be setup.
    """

    @task()
    def main():
        return 10

    assert isinstance(scheduler.executors["default"], LocalExecutor)
    assert scheduler.run(main()) == 10


def test_executor_config() -> None:
    """
    Specify additional executor by config object.
    """
    config_string = """
[executors.default]
type = local
max_workers = 100

[executors.batch]
type = aws_batch
image = 123.abc.ecr.us-west-2.amazonaws.com/amazonlinux-python3
queue = queue
s3_scratch = s3://example-bucket/redun/
"""

    config = Config()
    config.read_string(config_string)
    scheduler = Scheduler(config=config)

    assert isinstance(scheduler.executors["default"], LocalExecutor)
    assert isinstance(scheduler.executors["batch"], AWSBatchExecutor)

    assert scheduler.executors["default"].max_workers == 100
    assert scheduler.executors["batch"].queue == "queue"


def test_custom_executor() -> None:
    """
    Create uniquely named executor of type local.
    """

    @task(executor="custom")
    def main():
        return 10

    config_string = """
[executors.custom]
type = local
"""

    config = Config()
    config.read_string(config_string)
    scheduler = Scheduler(config=config)
    scheduler.executors.pop("default")

    assert main.get_task_option("executor") == "custom"
    assert isinstance(scheduler.executors["custom"], LocalExecutor)

    # Should be able to use executor.
    assert scheduler.run(main()) == 10


def test_task_options_executor() -> None:
    """
    Should be able to use task_options to specify executor.
    """

    @task(executor="bad_executor")
    def main():
        return 10

    config_string = """
[executors.custom]
type = local
"""

    config = Config()
    config.read_string(config_string)
    scheduler = Scheduler(config=config)
    scheduler.executors.pop("default")

    assert main.get_task_option("executor") == "bad_executor"
    assert isinstance(scheduler.executors["custom"], LocalExecutor)

    # Should be able to use executor.
    expr = main.options(executor="custom")()
    assert expr._options == {"executor": "custom"}

    assert scheduler.run(expr) == 10


@pytest.fixture
def clean_registry():
    """Save and restore the executor registry around each test."""
    original = dict(_executor_providers)
    yield _executor_providers
    _executor_providers.clear()
    _executor_providers.update(original)


def test_class_key_loads_unregistered_executor(clean_registry) -> None:
    """Config with class key loads an executor type not yet in the registry."""
    _executor_providers.pop("local", None)

    executor_class = get_executor_class("local", class_path="redun.executors.local.LocalExecutor")
    assert executor_class is LocalExecutor


def test_class_key_ignored_when_already_registered(clean_registry) -> None:
    """If type is already registered, class key is not consulted."""
    executor_class = get_executor_class(
        "local", class_path="redun.executors.aws_batch.AWSBatchExecutor"
    )
    assert executor_class is LocalExecutor


def test_class_key_invalid_module(clean_registry) -> None:
    """Invalid module in class path raises ExecutorError with clear message."""
    with pytest.raises(ExecutorError, match="Failed to load executor 'badtype'"):
        get_executor_class("badtype", class_path="nonexistent.module.FakeExecutor")


def test_class_key_invalid_class_name(clean_registry) -> None:
    """Valid module but invalid class name raises ExecutorError."""
    with pytest.raises(ExecutorError, match="Failed to load executor 'badtype'"):
        get_executor_class("badtype", class_path="redun.executors.local.NonexistentClass")


def test_error_message_suggests_class_key(clean_registry) -> None:
    """Unknown executor error message mentions the class key as a solution."""
    with pytest.raises(ExecutorError, match="class = your.module.ClassName"):
        get_executor_class("totally_unknown")


def test_get_executor_from_config_with_class_key(clean_registry) -> None:
    """get_executor_from_config supports the class key."""
    _executor_providers.pop("local", None)

    section = create_config_section(
        {"type": "local", "class": "redun.executors.local.LocalExecutor"}
    )
    config = {"my_exec": section}
    executor = get_executor_from_config(config, "my_exec")
    assert isinstance(executor, LocalExecutor)
    assert executor.name == "my_exec"


def test_no_class_key_regression(clean_registry) -> None:
    """Configs without class key continue to work unchanged."""
    executor_class = get_executor_class("local")
    assert executor_class is LocalExecutor


def test_integration_scheduler_with_class_key(clean_registry) -> None:
    """Full integration: Scheduler loads executor via class key in config."""
    config_string = """
[executors.default]
type = local

[executors.custom_local]
type = my_custom_type
class = redun.executors.local.LocalExecutor
max_workers = 4
"""
    config = Config()
    config.read_string(config_string)
    scheduler = Scheduler(config=config)

    assert "custom_local" in scheduler.executors
    assert isinstance(scheduler.executors["custom_local"], LocalExecutor)


def test_class_key_non_executor_subclass(clean_registry) -> None:
    """Class path pointing to a non-Executor class raises ExecutorError."""
    with pytest.raises(ExecutorError, match="is not a subclass of Executor"):
        get_executor_class("badtype", class_path="redun.config.Config")
