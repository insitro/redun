from redun import Scheduler, task
from redun.config import Config
from redun.executors.aws_batch import AWSBatchExecutor
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
    assert expr.task_expr_options == {"executor": "custom"}

    assert scheduler.run(expr) == 10
