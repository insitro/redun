import time
from typing import List

import pytest

from redun import Scheduler, task
from redun.backends.db import Execution, Job, Session
from redun.functools import (
    apply_func,
    as_task,
    compose,
    const,
    delay,
    eval_,
    flat_map,
    flatten,
    force,
    identity,
    map_,
    seq,
    starmap,
    zip_,
)


def test_compose(scheduler: Scheduler) -> None:
    """
    compose() should be able to compose multiple tasks together.
    """

    @task()
    def task1(x: int) -> int:
        return x + 1

    @task()
    def task2(x: int) -> int:
        return x * 2

    task3 = compose(task2, task1)
    assert scheduler.run(task3(1)) == 4

    task4 = compose(task1, task2, task1)
    assert scheduler.run(task4(1)) == 5


def test_delay(scheduler: Scheduler) -> None:
    """
    delay() should delay evaluation of its argument. force() should force evaluation.
    """
    calls = []

    @task()
    def task1():
        calls.append("task1")
        return 10

    @task()
    def main(x):
        return x

    expr = delay(task1())
    scheduler.run(main(expr))
    assert calls == []

    assert scheduler.run(main(force(expr))) == 10
    assert calls == ["task1"]


def test_seq(scheduler: Scheduler) -> None:
    """
    seq() should sequence the execution of tasks.
    """
    calls = []

    @task()
    def subtask(name, interval):
        time.sleep(interval)
        calls.append(name)
        return name

    assert scheduler.run(
        seq(
            [
                subtask(1, 0.2),
                subtask(2, 0.1),
                subtask(3, 0.01),
                subtask(4, 0.01),
            ]
        )
    ) == [1, 2, 3, 4]
    assert calls == [1, 2, 3, 4]

    assert scheduler.run(seq([])) == []
    assert scheduler.run(seq([10])) == [10]
    assert scheduler.run(seq([subtask(1, 0)])) == [1]


def test_const(scheduler: Scheduler) -> None:
    """
    const() should discard the second argument.
    """

    assert scheduler.run(const(1, 2)) == 1
    assert scheduler.run(const(2, 1)) == 2


def add(a: int, b: int) -> int:
    return a + b


@task(namespace="redun.tests")
def range_(start, stop):
    return list(range(start, stop))


def test_apply_func(scheduler: Scheduler) -> None:
    """
    apply_func() should call a plain python function on lazy args.
    """
    assert scheduler.run(apply_func(max, 30, 20, 10)) == 30
    assert scheduler.run(apply_func(add, 10, b=20)) == 30
    assert scheduler.run(apply_func(add, a=2, b=20)) == 22

    assert scheduler.run(apply_func(max, range_(1, 4))) == 3


def test_as_task(scheduler: Scheduler) -> None:
    """
    as_task() should convert a python function to a redun Task.
    """
    assert scheduler.run(as_task(max)(range_(1, 4))) == 3

    inc = as_task(add).partial(1)
    assert scheduler.run(map_(inc, range_(1, 4))) == [2, 3, 4]
    assert scheduler.run(as_task(len)(range_(1, 4))) == 3


def test_eval(scheduler: Scheduler) -> None:
    """
    eval_() should evaluate a string of python.
    """

    # Evaluate a python string.
    assert scheduler.run(eval_("1 + 2")) == 3

    # Use keyword arguments.
    assert scheduler.run(eval_("{a, b}", a=1, b=2)) == {1, 2}

    # Use a positional argument.
    assert scheduler.run(eval_("x + 2", 1, pos_args=["x"])) == 3

    # Use a positional argument.
    assert scheduler.run(eval_("x + y", 1, 2, pos_args=["x", "y"])) == 3

    # Disallow too many positional arguments.
    with pytest.raises(TypeError):
        scheduler.run(eval_("x + y", 1, 2, pos_args=["x"]))

    # Allow indented code.
    assert (
        scheduler.run(
            eval_(
                """
                {a, b}
                """,
                a=1,
                b=2,
            )
        )
        == {1, 2}
    )

    # eval_ should work with partial application.
    assert scheduler.run(eval_.partial("{a, b}")(a=1, b=2)) == {1, 2}
    assert scheduler.run(eval_.partial("x + 1", pos_args=["x"])(5)) == 6


def test_map(scheduler: Scheduler, session: Session) -> None:
    """
    map_() should perform function composition optimizations.
    """

    @task()
    def inc(x: int) -> int:
        return x + 1

    @task()
    def double(x: int) -> int:
        return 2 * x

    @task()
    def get_values() -> List[int]:
        return [1, 2, 3]

    # Simple map over a list.
    assert scheduler.run(map_(inc, [1, 2, 3])) == [2, 3, 4]

    # List of values can be an expression as well.
    assert scheduler.run(map_(inc, get_values())) == [2, 3, 4]

    # Task can be an expression.
    assert scheduler.run(map_(identity(inc), [1, 2, 3])) == [2, 3, 4]

    # Multiple maps should be composed.
    @task()
    def main() -> List[int]:
        return map_(inc, map_(double, [1, 2, 3]))

    assert scheduler.run(main()) == [3, 5, 7]

    exec1 = (
        session.query(Execution)
        .join(Job, Job.id == Execution.job_id)
        .order_by(Job.start_time.desc())
        .first()
    )

    for job in exec1.job.child_jobs:
        assert job.task.name == "compose_apply"
        assert [child_job.task.name for child_job in job.child_jobs] == ["double", "inc"]


def test_starmap(scheduler: Scheduler, session: Session) -> None:
    """
    starmap_() should map a task over a list of keyword arguments.
    """

    kwargs = [
        {"a": 1, "b": 2},
        {"a": 3, "b": 4},
    ]

    @task()
    def add(a, b):
        return a + b

    assert scheduler.run(starmap(add, kwargs)) == [3, 7]

    @task()
    def get_kwargs():
        return [
            {"a": 1, "b": 2},
            {"a": 3, "b": 4},
        ]

    # starmap should be able to take kwargs as an Expression.
    assert scheduler.run(starmap(add, get_kwargs())) == [3, 7]

    @task()
    def swap(a, b):
        return {"a": b, "b": a}

    # Multiple maps should be composed.
    @task()
    def main() -> List[int]:
        return starmap(add, starmap(swap, kwargs))

    assert scheduler.run(main()) == [3, 7]

    exec1 = (
        session.query(Execution)
        .join(Job, Job.id == Execution.job_id)
        .order_by(Job.start_time.desc())
        .first()
    )

    for job in exec1.job.child_jobs:
        assert job.task.name == "compose_apply"
        assert [child_job.task.name for child_job in job.child_jobs] == ["eval_", "eval_"]
        assert [child_job.child_jobs[0].task.name for child_job in job.child_jobs] == [
            "swap",
            "add",
        ]


def test_flatten(scheduler: Scheduler) -> None:
    """
    flatten() should flatten lists.
    """

    @task()
    def split(text: str) -> List[str]:
        return text.split()

    texts = ["hello world", "not invented here"]
    assert scheduler.run(flatten(map_(split, texts))) == [
        "hello",
        "world",
        "not",
        "invented",
        "here",
    ]
    assert scheduler.run(flat_map(split, texts)) == ["hello", "world", "not", "invented", "here"]


def test_zip(scheduler: Scheduler) -> None:
    """
    zip_() should zip lists.
    """

    @task()
    def range_(a: int, b: int) -> List[int]:
        return list(range(a, b))

    assert scheduler.run(zip_(range_(0, 5), range_(1, 6))) == [
        (0, 1),
        (1, 2),
        (2, 3),
        (3, 4),
        (4, 5),
    ]
