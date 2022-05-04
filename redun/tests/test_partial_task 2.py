from typing import List, cast

import pytest

from redun import Scheduler, task
from redun.expression import Expression, TaskExpression
from redun.functools import compose
from redun.task import PartialTask
from redun.value import get_type_registry

NULL = object()


@task(name="map", namespace="redun")
def task_map(a_task, values):
    return [a_task(value) for value in values]


# Monkey patch Expression with map.
def expression_map(self, a_task):
    return TaskExpression("redun.map", (a_task, self), {})


Expression.map = expression_map  # type: ignore


@task(name="reduce", namespace="redun")
def task_reduce(a_task, values, init=NULL):
    values = iter(values)
    if init is NULL:
        result = next(values)
    else:
        result = init
    for value in values:
        result = a_task(result, value)
    return result


def expression_reduce(self, a_task, init=NULL):
    return TaskExpression("redun.reduce", (a_task, self, init), {})


Expression.reduce = expression_reduce  # type: ignore


def test_expression_map(scheduler):
    @task()
    def get_list():
        return [1, 2, 3]

    @task()
    def inc(x):
        return x + 1

    assert scheduler.run(get_list().map(inc)) == [2, 3, 4]


def test_expression_reduce(scheduler):
    @task()
    def get_list():
        return [1, 2, 3]

    @task()
    def add(a, b):
        return a + b

    assert scheduler.run(get_list().reduce(add)) == 6
    assert scheduler.run(get_list().reduce(add, 10)) == 16


def test_partial(scheduler: Scheduler) -> None:
    """
    PartialTask should be created correctly and callable.
    """

    @task()
    def add(a: int, b: int) -> int:
        return a + b

    assert scheduler.run(add(1, 2)) == 3

    # Create PartialTask and call it.
    partial_task: PartialTask = PartialTask(add, (1,), {})
    expr: Expression[int] = partial_task(2)
    assert isinstance(expr, TaskExpression)
    assert expr.args == (1, 2)
    assert scheduler.run(expr) == 3

    # Use Task.partial() to make PartialTask.
    inc = add.partial(1)
    assert scheduler.run(inc(10)) == 11

    # Partial should be usable in a workflow.
    @task()
    def main() -> List[int]:
        inc = add.partial(1)
        return [inc(10), inc(12)]

    assert scheduler.run(main()) == [11, 13]

    # Arguments can be specified in different order using kwargs.
    assert scheduler.run(add.partial(b=1)(10)) == 11
    assert scheduler.run(add.partial(a=1)(b=10)) == 11


def test_partial_serialize() -> None:
    """
    PartialTask should serialize and deserialize.
    """

    @task(name="my_add", namespace="my_namespace")
    def add(a: int, b: int) -> int:
        return a + b

    inc = add.partial(1)
    state = inc.__getstate__()
    assert state == {
        "args": (1,),
        "compat": [],
        "hash": "13b99deec09100b663f0ab0d0e9af49ee0a800e0",
        "kwargs": {},
        "name": "my_add",
        "namespace": "my_namespace",
        "script": False,
        "task": {
            "compat": [],
            "hash": "7bc36e593df9b494ba5cb47be1664c04d12ae851",
            "name": "my_add",
            "namespace": "my_namespace",
            "script": False,
            "task_options": {},
            "version": None,
        },
        "task_options": {},
        "version": None,
    }

    # Round trip deserialization.
    inc2 = PartialTask.__new__(PartialTask)
    inc2.__setstate__(state)
    assert inc2.__getstate__() == state
    assert inc2.source == add.source


def test_partial_bad_args(scheduler: Scheduler) -> None:
    """
    Scheduler should be able to catch and properly record bad arguments.
    """

    @task()
    def add3(a, b, c):
        return a + b + c

    @task()
    def main():
        # Specify argument 'a' twice. Leave argument 'c' unspecified.
        part = add3.partial(a=1, b=2)
        return part(3)

    with pytest.raises(TypeError):
        scheduler.run(main())


def test_partial_options(scheduler: Scheduler) -> None:
    """
    PartialTask should be able to use task options.
    """

    @task()
    def add(a: int, b: int) -> int:
        return a + b

    assert not add.has_task_option("memory")
    assert add.get_task_option("memory") is None
    assert 1 == add.get_task_option("memory", 1)

    inc = add.partial(1).options(memory=4)
    inc = cast(PartialTask, inc)

    # Should be recorded on task.
    assert inc.task.has_task_option("memory")
    assert 4 == inc.task.get_task_option("memory")
    assert 4 == inc.task.get_task_option("memory", 1)

    # When we call the PartialTask, the task_options should propagate to the Expression.
    expr: Expression[int] = inc(2)
    assert expr.task_expr_options == {"memory": 4}

    # Task options should serialize.
    state = inc.__getstate__()
    assert state["task"]["task_options"] == {"memory": 4}

    # Round trip deserialization.
    inc2 = PartialTask.__new__(PartialTask)
    inc2.__setstate__(state)
    assert inc2.__getstate__() == state


def test_partial_partial(scheduler: Scheduler) -> None:
    """
    We should be able to perform chained partial application.
    """

    @task()
    def add3(a, b, c):
        return a + b + c

    part = add3.partial(1).partial(2)
    assert part.args == (1, 2)
    assert scheduler.run(part(3)) == 6

    part2 = add3.partial(b=2).partial(a=1)
    assert part2.args == ()
    assert part2.kwargs == {"a": 1, "b": 2}
    assert scheduler.run(part2(c=3)) == 6

    # Serialize and deserialize PartialTask (simulate fetching from cache).
    registry = get_type_registry()
    data = registry.serialize(part)
    part3 = registry.deserialize("redun.PartialTask", data)

    # Should get same result when calling.
    assert scheduler.run(part3(3)) == 6


def test_partial_partial_options(scheduler: Scheduler) -> None:
    """
    We should be able to mix partial and options calls.
    """

    @task()
    def add3(a, b, c):
        return a + b + c

    # Options should be present on PartialTask.
    part = add3.partial(1).partial(2).options(memory=4)
    part = cast(PartialTask, part)
    assert part.task.get_task_options() == {"memory": 4}

    # Options should propagate to Expression.
    expr: Expression[int] = part(3)
    assert expr.task_expr_options == {"memory": 4}


def test_partial_as_arg():
    task_calls = []

    @task()
    def add(a, b):
        task_calls.append("add")
        return a + b

    @task()
    def main(a_task):
        task_calls.append("main")
        return a_task(10)

    scheduler = Scheduler()
    inc = add.partial(1)
    assert scheduler.run(main(inc)) == 11
    assert task_calls == ["main", "add"]

    # Everything should be cached.
    task_calls = []
    assert scheduler.run(main(inc)) == 11
    assert task_calls == []


def test_partial_as_result():
    task_calls = []

    @task()
    def add(a, b):
        task_calls.append("add")
        return a + b

    @task()
    def make_partial():
        task_calls.append("make_partial")
        inc = add.partial(1)
        return inc

    @task()
    def main():
        task_calls.append("main")
        inc = make_partial()
        return inc(10)

    scheduler = Scheduler()
    task_calls = []
    assert scheduler.run(main()) == 11
    assert task_calls == ["main", "make_partial", "add"]

    # Everything should be cached.
    task_calls = []
    assert scheduler.run(main()) == 11
    assert task_calls == []


def test_compose():
    @task()
    def task1(x):
        return x + 1

    @task()
    def task2(x):
        return x * 2

    scheduler = Scheduler()

    task3 = compose(task2, task1)
    assert scheduler.run(task3(1)) == 4

    task4 = compose(task1, task2, task1)
    assert scheduler.run(task4(1)) == 5


def test_lisp(scheduler):
    @task()
    def pair(a, b, attr):
        if attr == "head":
            return a
        elif attr == "tail":
            return b
        else:
            raise ValueError()

    @task()
    def add(a, b):
        return a + b

    @task()
    def identity(x):
        return x

    def defer(x):
        return identity.partial(x)

    def cons(a, b):
        return pair.partial(a, b)

    @task()
    def head(lst):
        return lst("head")

    @task()
    def tail(lst):
        return lst("tail")

    @task()
    def is_null(value):
        return value is None

    @task()
    def _cond_eval(expr, result1, result2):
        if expr:
            return result1()
        else:
            return result2()

    def cond(expr, result1, result2):
        return _cond_eval(expr, defer(result1), defer(result2))

    @task()
    def map_(func, lst):
        return cond(is_null(lst), None, cons(func(head(lst)), map_(func, tail(lst))))

    @task()
    def reduce(func, lst, init):
        return cond(is_null(lst), init, func(head(lst), reduce(func, tail(lst), init)))

    @task()
    def as_list(lst):
        return cond(is_null(lst), [], add([head(lst)], as_list(tail(lst))))

    @task()
    def _sequence(low, high, attr):
        if attr == "head":
            return low
        elif attr == "tail":
            if low + 1 < high:
                return sequence(low + 1, high)
            else:
                return None
        else:
            return ValueError()

    def sequence(low, high):
        return _sequence.partial(low, high)

    @task()
    def take(k, lst):
        return cond(is_null(lst).__or__(k <= 0), None, cons(head(lst), take(k - 1, tail(lst))))

    lst = cons(1, cons(2, cons(3, None)))

    assert scheduler.run(head(lst)) == 1
    assert scheduler.run(head(tail(lst))) == 2
    assert scheduler.run(cond(identity(True), identity(1), identity(2))) == 1
    assert scheduler.run(cond(identity(False), identity(1), identity(2))) == 2

    assert scheduler.run(as_list(map_(add.partial(1), lst))) == [2, 3, 4]
    assert scheduler.run(reduce(add, lst, 0)) == 6
    # assert scheduler.run(reduce(add, map_(list, lst), [])) == [1, 2, 3]

    assert scheduler.run(as_list(sequence(1, 10))) == [1, 2, 3, 4, 5, 6, 7, 8, 9]

    assert scheduler.run(as_list(take(5, sequence(1, 10)))) == [1, 2, 3, 4, 5]


def test_seq(scheduler):
    task_calls = []

    @task()
    def identity(x):
        return x

    def defer(x):
        return identity.partial(x)

    @task()
    def task1(x):
        task_calls.append(x)
        return x

    @task()
    def seq_(t1, *rest):
        if rest:
            return seq_(rest[0](), *rest[1:])
        return t1

    def seq(t1, *rest):
        return seq_(t1, *map(defer, rest))

    scheduler.run(seq(task1("a"), task1("b")))
    assert task_calls == ["a", "b"]
