import sys
from unittest.mock import patch

import pytest

from redun import Scheduler, task
from redun.config import Config

redun_namespace = "acme"


def test_namespace() -> None:
    """
    Namespace should be set by module-level var `redun_namespace`.
    """

    @task()
    def task1():
        return 10

    assert task1.namespace == "acme"


def test_namespace_task_override() -> None:
    """
    Namespace should be set by task decorator.
    """

    @task(namespace="ns")
    def task1():
        return 10

    assert task1.namespace == "ns"


def test_no_namespace() -> None:
    """
    Namespace should be required for tasks.
    """
    module = sys.modules[test_no_namespace.__module__]

    scheduler = Scheduler()

    with patch.object(module, "redun_namespace", None), patch.object(
        scheduler.logger, "warning"
    ) as warning:

        @task()
        def task1():
            return 10

        assert scheduler.run(task1()) == 10
        assert "namespace" in warning.call_args[0][0]


def test_empty_namespace() -> None:
    """Until namespaces are required, setting an empty-string namespace is legal"""
    scheduler = Scheduler()

    with patch.object(scheduler.logger, "warning") as warning:

        @task(namespace="")
        def task1():
            return 10

        assert task1.namespace == ""
        assert scheduler.run(task1()) == 10
        assert "namespace" in warning.call_args[0][0]


def test_no_namespace_ignore() -> None:
    """
    Ensure we can ignore namespace warning.
    """
    module = sys.modules[test_no_namespace.__module__]

    scheduler = Scheduler(
        config=Config(
            {
                "scheduler": {
                    "ignore_warnings": "namespace",
                }
            }
        )
    )
    assert scheduler.ignore_warnings == {"namespace"}

    with patch.object(module, "redun_namespace", None), patch.object(
        scheduler.logger, "warning"
    ) as warning:

        @task()
        def task1():
            return 10

        assert scheduler.run(task1()) == 10
        assert warning.call_count == 0


def test_bad_namespace() -> None:

    with pytest.raises(ValueError):

        @task(name="1bad_task")
        def bad_task():
            pass

    with pytest.raises(ValueError):

        @task(name="bad task")
        def bad_task2():
            pass

    with pytest.raises(ValueError):

        @task(namespace="bad-task")
        def bad_task3():
            pass

    @task(name="good_task2", namespace="name.space2")
    def good_task():
        pass
