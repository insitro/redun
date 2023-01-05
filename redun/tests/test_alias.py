from unittest.mock import Mock

import pytest

from redun import AliasExecutor, Scheduler
from redun.config import Config
from redun.executors.base import ExecutorError


def test_alias_executor(scheduler: Scheduler) -> None:

    config = Config({"batch": {"target": "default"}})
    executor = AliasExecutor("batch", scheduler, config["batch"])
    scheduler.executors["default"] = Mock()
    executor.submit(None)  # type: ignore # fake arguments for the mock

    # mypy doesn't understand the mock on the line below
    scheduler.executors["default"].submit.assert_called_with(None)  # type: ignore

    with pytest.raises(ExecutorError, match="Could not find executor `missing` from options"):
        config = Config({"batch": {"target": "missing"}})
        executor = AliasExecutor("batch", scheduler, config["batch"])
        executor.submit(None)  # type: ignore # fake arguments for the mock

    # Now try initializing with the
    scheduler.executors["default"].name = "default"
    executor = AliasExecutor("batch", scheduler, target=scheduler.executors["default"])

    scheduler.executors["default"].reset_mock()  # type: ignore
    executor.submit(None)  # type: ignore # fake arguments for the mock

    # mypy doesn't understand the mock on the line below
    scheduler.executors["default"].submit.assert_called_with(None)  # type: ignore

    with pytest.raises(
        AssertionError, match="Exactly one of `target` or `config` should be provided"
    ):
        AliasExecutor(
            "batch", scheduler, config=config["batch"], target=scheduler.executors["default"]
        )
