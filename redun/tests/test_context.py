from typing import Optional

from redun import Scheduler, get_context, task
from redun.config import Config
from redun.context import get_context_value
from redun.tests.utils import wait_until


def test_get_context_value() -> None:
    """
    Should be able to fetch values from a context dict.
    """
    context = {
        "foo": {
            "bar": {
                "baz": 10,
                "baaz": True,
            }
        },
        "foo2": "hello",
        "list": [1, 2, 3],
    }
    assert get_context_value(context, "foo.bar.baz", 0) == 10
    assert get_context_value(context, "foo.bar.baaz", False) is True
    assert get_context_value(context, "foo.bar.baz2", 0) == 0
    assert get_context_value(context, "bad", -1) == -1
    assert get_context_value(context, "foo2", "") == "hello"
    assert get_context_value(context, "list", []) == [1, 2, 3]
    assert get_context_value(context, "list.bad", []) == []


def test_context(scheduler: Scheduler) -> None:
    """
    Basic context features.
    """

    task_calls = []

    @task
    def add(a: int, b: int, offset: int = get_context("offset", 3)) -> int:
        task_calls.append("add")
        return a + b + offset

    # The default value of a context variable should be used.
    assert scheduler.run(add(1, 2)) == 6

    # Context can be changed with task_options override.
    # Also caching considers the context arguments.
    assert scheduler.run(add.update_context({"offset": 4})(1, 2)) == 7
    assert task_calls == ["add", "add"]

    # Cache should be used.
    assert scheduler.run(add.update_context({"offset": 4})(1, 2)) == 7
    assert task_calls == ["add", "add"]

    # Alternative update_context call.
    assert scheduler.run(add.update_context(offset=5)(1, 2)) == 8

    @task
    def main():
        return add(3, 4)

    # The parent job's context should be used.
    assert scheduler.run(main.update_context({"offset": 10})()) == 17


def test_context_nested(scheduler: Scheduler) -> None:
    """
    Context should support variable nesting.
    """

    @task
    def task1(x: str = get_context("aa.bb.cc", "foo")) -> str:
        return x

    context = {"aa": {"bb": {"cc": "bar"}}}
    assert scheduler.run(task1.update_context(context)()) == "bar"


def test_context_default_none(scheduler: Scheduler) -> None:
    """
    If default is not provided, it should fallback to None.
    """

    @task
    def task1(x: str = get_context("a.b.c")) -> Optional[str]:
        return x

    assert scheduler.run(task1(), context={"a": {"b": 1}}) is None


def test_context_merge(scheduler: Scheduler) -> None:
    """
    Context should merge from higher tasks.
    """

    @task
    def add(
        a: int,
        b: int,
        offset: int = get_context("offset", 3),
        mult: int = get_context("mult", 1),
    ) -> int:
        return a + b + offset * mult

    # The default value of a context variable should be used.
    assert scheduler.run(add(1, 2)) == 6

    @task
    def main():
        return add(3, 4)

    # The parent job's context should be used.
    assert scheduler.run(main.update_context({"mult": 2})()) == 13


def test_context_run(scheduler: Scheduler) -> None:
    """
    Context should be settable from Scheduler.run().
    """

    @task
    def add(
        a: int,
        b: int,
        offset: int = get_context("offset", 3),
        mult: int = get_context("mult", 1),
    ) -> int:
        return a + b + offset * mult

    assert scheduler.run(add(1, 2), context={"offset": 10, "mult": 20}) == 203


def test_context_config() -> None:
    """
    Context should be settable from Config.
    """

    @task
    def add(
        a: int,
        b: int,
        offset: int = get_context("offset", 3),
        mult: int = get_context("mult", 1),
    ) -> int:
        return a + b + offset * mult

    scheduler = Scheduler(config=Config({"scheduler": {"context": '{"offset": 10, "mult": 20}'}}))
    assert scheduler.run(add(1, 2)) == 203


def test_context_update(scheduler: Scheduler) -> None:
    """
    We should be able to partially update context multiple times.
    """

    @task
    def add(
        a: int,
        b: int,
        offset: int = get_context("offset", 0),
        mult: int = get_context("mult", 1),
    ) -> int:
        return a + b + offset * mult

    assert scheduler.run(add(1, 2)) == 3
    assert scheduler.run(add.update_context({"offset": 4})(1, 2)) == 7
    assert scheduler.run(add.update_context({"offset": 4}).update_context({"mult": 2})(1, 2)) == 11


def test_context_cse(scheduler: Scheduler) -> None:
    """
    Ensure CSE does not collapse jobs with different contexts.
    """

    task_calls = []

    @task
    def add(a, b, offset=get_context("offset", 0)):
        task_calls.append("add")
        return a + b + offset

    @task
    def foo():
        return add(1, 2)

    @task
    def main():
        # The two foo() calls could collapse if context was not considered.
        return foo() + foo.update_context(offset=1)()

    assert scheduler.run(main()) == 7
    assert task_calls == ["add", "add"]


def test_context_cse2(scheduler: Scheduler) -> None:
    """
    Ensure CSE does not collapse jobs with different contexts.
    """

    task_calls = []

    @task
    def add(a, b, offset=get_context("offset", 0)):
        task_calls.append("add")
        return a + b + offset

    @task
    def foo():
        return add(1, 2)

    @task
    def bar():
        # Here we wait for the first foo to finish.
        # This tests the case where we check the db for CSE.
        wait_until(lambda: len(task_calls) > 0)
        return foo.update_context(offset=1)()

    @task
    def main():
        return foo() + bar()

    assert scheduler.run(main()) == 7
    assert task_calls == ["add", "add"]


def test_context_check_valid(scheduler: Scheduler) -> None:
    """
    Ensure check_valid="shallow" cache checking works with changing context.
    """
    task_calls = []

    @task
    def add(a, b, offset=get_context("offset", 0)):
        task_calls.append("add")
        return a + b + offset

    @task(check_valid="shallow")
    def foo():
        return add(1, 2)

    @task
    def bar():
        # Here we wait for the first foo to finish.
        # This tests the case where we check db cache.
        wait_until(lambda: len(task_calls) > 0)
        return foo.update_context(offset=1)()

    @task
    def main():
        # The two foo() calls could collapse if context was not considered.
        return foo() + bar()

    assert scheduler.run(main()) == 7
    assert task_calls == ["add", "add"]
