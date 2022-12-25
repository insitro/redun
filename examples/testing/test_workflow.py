"""
Example of a test suite for a workflow. Use the following command to run tests:

pytest test_workflow.py
"""

import pytest
from workflow import add, add4, batch_task, divide, main

from redun.executors.local import LocalExecutor
from redun.pytest import Scheduler


def test_main(scheduler: Scheduler) -> None:
    """
    End-to-end workflow test.
    """
    assert scheduler.run(main(1)) == 6
    assert scheduler.run(main(2)) == 9


def test_add(scheduler: Scheduler) -> None:
    """
    Unit-test a single task: add.
    """
    assert scheduler.run(add(1, 2)) == 3
    assert scheduler.run(add(0, 2)) == 2
    assert scheduler.run(add(-1, 1)) == 0


def test_failure(scheduler: Scheduler) -> None:
    """
    Test for failures using typical pytest features.
    """
    with pytest.raises(ZeroDivisionError):
        scheduler.run(divide(4, 0))


def test_add4_oneshot(scheduler: Scheduler) -> None:
    """
    Call a task's wrapped function in order to evaluate only one task and none
    of its children tasks.
    """
    # By calling add.func() we can access the returned expression tree.
    # Here, we assert the expression is what we expect.
    expr_tree = add4.func(1, 2, 3, 4)
    assert expr_tree.task_name == "redun.examples.testing.add"

    expr_a, expr_b = expr_tree.args
    assert expr_a.task_name == "redun.examples.testing.add"
    assert expr_a.args == (1, 2)

    assert expr_b.task_name == "redun.examples.testing.add"
    assert expr_b.args == (3, 4)


def test_mock_executor() -> None:
    """
    We can redefine the executors for testing in order to force a task to
    execute locally.
    """
    scheduler = Scheduler()

    # Define the 'batch' executor as a LocalExecutor for testing.
    scheduler.add_executor(LocalExecutor("batch"))

    assert scheduler.run(batch_task(10)) == 11
