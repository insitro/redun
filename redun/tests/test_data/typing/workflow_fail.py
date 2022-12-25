"""
This file is used as a canary to ensure mypy finds type errors related to redun task calls.

We do this via mypy itself and the use of --warn-unused-ignore option. If any of the ignored
errors in this file are _not_ encountered in a mypy run, mypy will report that the ignore is
unused and we will know that the expected error is not being raised.
"""

from typing import List

from redun import Scheduler, task


@task()
def int2str(x: int) -> str:
    return str(x)


@task()
def length(y: str) -> int:
    return len(y)


@task()
def add(a: int, b: int) -> int:
    return a + b


@task()
def sum_list(values: List[int]) -> int:
    return sum(values)


@task()
def fail_return_task(a: int) -> int:
    # ERROR: return type should be int.
    return str(a)  # type: ignore[return-value]


@task()
def fail_return_task2(a: int) -> int:
    # ERROR: TaskExpression[str] is not allowed for int.
    return int2str(a)  # type: ignore[return-value]


@task()
def ok_return_task3(a: int) -> int:
    return add(a, a)  # OK: TaskExpression[int] is allowed for int.


@task()
def ok_return_task4(a: int) -> int:
    return a  # OK: Regular int is allowed for Return[int].


@task()
def ok_return_arg() -> str:
    expr = ok_return_task3(10)  # Note: Return type is int.
    expr2 = int2str(expr)  # OK: expr should considered type int.
    return expr2


def plain_func(x: int) -> int:
    return x + 1


def main() -> None:
    int2str(12345)  # OK: int arg used, int expected.

    a = add(1, 2)  # type is TaskExpression[int]
    int2str(a)  # OK: TaskExpression[int] allowed for int arg in a task.
    plain_func(a)  # PUNT: Ideally, we shouldn't use TaskExpression[int] for int in plain function.
    a2: int = a  # PUNT: Ideally, we shouldn't allow assignment of TaskExpression[int] to an int.

    # ERROR: str arg used, int expected.
    int2str("12345")  # type: ignore[arg-type]

    str_expr = int2str(10)  # type is TaskExpression[str]
    # ERROR: TaskExpression[str] used, int expected.
    int2str(str_expr)  # type: ignore[arg-type]

    # ERROR: Return value is TaskExpression[str] and y is int.
    y: int = int2str(12345)  # type: ignore[assignment]

    sum_list([1, 2, 3])  # OK: arg is List[int].
    # ERROR: some items in list are not int.
    sum_list([1, "2", 3])  # type: ignore[list-item]

    b = add(1, 2)  # type is TaskExpression[int]
    sum_list([1, b, 3])  # OK: TaskExpression[int] can be used for int.
    sum_list([])  # OK: Empty list is ok.

    c = int2str(10)  # type is TaskExpression[str]
    # ERROR: TaskExpression[str] cannot be used for int.
    sum_list([1, 2, c])  # type: ignore[list-item]

    # Large expressions and run().
    scheduler = Scheduler()
    d: int = scheduler.run(sum_list([add(length(int2str(12345)), 1), 2]))  # OK

    add.partial()(1, 2)  # OK: PartialTask should still expect int arguments.

    # It isn't easy to compute the new type signature at the moment.
    add.partial()("1", 2)  # PUNT: We can't type check arguments to partial.

    # ERROR: Partial return value should type check.
    e: str = add.partial()(1, 2)  # type: ignore[assignment]

    # Use all variables to satisfy lint.
    _ = [a2, y, d, e]
