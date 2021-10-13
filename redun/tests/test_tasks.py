import pickle
from collections import namedtuple
from typing import Any, NamedTuple, Tuple

import pytest

import redun
from redun import Task, task
from redun.expression import Expression
from redun.scheduler import set_arg_defaults
from redun.task import get_tuple_type_length
from redun.utils import get_func_source, pickle_dumps


def test_task_config() -> None:
    """
    Task should be properly configured by decorator.
    """

    @task()
    def my_task():
        return 10

    @task(name="custom_name", namespace="my_namespace")
    def my_task2():
        return 10

    redun.namespace("my_namespace2")

    @task()
    def my_task3():
        return 10

    redun.namespace()

    assert my_task.name == "my_task"
    assert my_task.fullname == "my_task"
    assert my_task.hash
    assert my_task.func
    assert (
        my_task.source
        == """\
    def my_task():
        return 10
"""
    )

    registry = redun.get_task_registry()
    assert registry.get(my_task.name) == my_task

    assert my_task2.name == "custom_name"
    assert my_task2.fullname == "my_namespace.custom_name"

    assert my_task3.fullname == "my_namespace2.my_task3"


def test_task_hashing():  # typing: ignore
    """
    Test various forms of Task hashing.
    """

    @task()
    def task1():
        return 10

    assert task1.get_hash() == "ec0f851458d39044ae657dd250fa9526209ae6df"

    @task(name="other_name")
    def task1():
        return 10

    assert task1.get_hash() == "a285fea4f954a7f695cc0f390afef52034dd8615"

    @task(name="other_name", namespace="other_module")
    def task1():
        return 10

    assert task1.get_hash() == "4ef98ce3cc62f8dc741be21bc7a212b907c50a8e"

    @task(name="other_name", namespace="other_module", version="1")
    def task1():
        return 10

    assert task1.get_hash() == "c4a9e6d68a9ea97c2a7cb5dee6bff084ea2ffcfc"

    @task(name="other_name", namespace="other_module", version="1")
    def task1():
        return "totaly different implementation"

    assert task1.get_hash() == "c4a9e6d68a9ea97c2a7cb5dee6bff084ea2ffcfc"

    @task(name="another_name", namespace="another_module", version="1")
    def task1():
        return "totaly different implementation"

    assert task1.get_hash() == "48ce769e8b759be9812a19cfdb69898678eee0b3"


def test_version() -> None:
    """
    Hash depends on version instead function source.
    """

    @task(name="task1", version="1")
    def task1():
        return "hello"

    @task(name="task1", version="1")
    def task2():
        return "bye"

    assert task1.hash == task2.hash


def test_set_default_args() -> None:
    @task()
    def myfunc(arg1, arg2=10):
        return None

    args, kwargs = set_arg_defaults(myfunc, (1,), {"arg2": 2})
    assert args == (1,)
    assert kwargs == {"arg2": 2}

    args, kwargs = set_arg_defaults(myfunc, (1,), {})
    assert args == (1,)
    assert kwargs == {"arg2": 10}

    args, kwargs = set_arg_defaults(myfunc, (), {"arg1": 1})
    assert args == ()
    assert kwargs == {"arg1": 1, "arg2": 10}


def test_cache_task() -> None:
    """
    Ensure we can cache a task.
    """

    @task()
    def task1():
        return 10

    assert task1.__getstate__() == {
        "name": "task1",
        "namespace": "",
        "version": None,
        "hash": task1.hash,
        "compat": [],
        "script": False,
        "task_options": {},
    }

    # Serialize and deserialize the task.
    data = pickle_dumps(task1)
    task1b = pickle.loads(data)

    # Ensure task deserialized correctly.
    assert isinstance(task1b, Task)
    assert task1b.fullname == task1.fullname
    assert task1b.func == task1.func
    assert task1b.source == task1.source

    # The task is still valid because we have not changed its code since serialization.
    assert task1b.is_valid()

    # Only change task_options.
    @task(memory=4)  # type: ignore
    def task1():
        return 10

    # Deserialize again.
    task1c = pickle.loads(data)
    assert isinstance(task1c, Task)
    assert task1c.fullname == task1.fullname
    assert task1c.func == task1.func

    # Task should still be valid.
    assert task1c.is_valid()

    # Task should have new task options.
    assert task1c.task_options == {"memory": 4}

    # Change the definition of the task.
    @task()  # type: ignore
    def task1():
        return 20

    # Deserialize again.
    task1d = pickle.loads(data)
    assert isinstance(task1d, Task)
    assert task1d.fullname == task1.fullname
    assert task1d.func == task1.func

    # Since code changes, the task is not valid to use.
    assert not task1d.is_valid()


def test_cache_task_del() -> None:
    """
    Ensure we can deserialize a Task with deleted code.
    """
    state: dict = {
        "name": "test_cache_task_del_task",
        "namespace": None,
        "version": None,
        "hash": "HASH",
        "sync": False,
        "compat": [],
    }

    task1 = Task.__new__(Task)
    task1.__setstate__(state)

    # A Task that refers to non-existant code should not be valid.
    assert not task1.is_valid()

    # We should get an error calling the function.
    with pytest.raises(ValueError):
        task1.func()

    # Similarly, the corresponding Expression should not be valid either.
    expr = task1()
    assert isinstance(expr, Expression)
    assert not expr.is_valid()


def test_get_func_source() -> None:
    """
    get_func_source() should return the source of a function.
    """

    @task()
    def task1():
        return 10

    assert (
        get_func_source(task1.func)
        == """\
    def task1():
        return 10
"""
    )


def test_task_options() -> None:
    """
    Task options should be availble on the task and updates by options().
    """

    @task(executor="batch")
    def task1():
        return 10

    assert task1.task_options == {"executor": "batch"}

    # The expression should not need any updated options.
    assert task1().task_options == {}

    # When we apply an update to the task, it should appear on the new task and expression.
    task2 = task1.options(executor="local")
    assert task2.task_options_update == {"executor": "local"}
    assert task2().task_options == {"executor": "local"}

    # Additional updates should accumulate.
    task3 = task2.options(memory=4)
    assert task3.task_options_update == {
        "executor": "local",
        "memory": 4,
    }
    assert task3().task_options == {
        "executor": "local",
        "memory": 4,
    }


def test_task_options_hash() -> None:
    """
    task_options_update should contribute to task hash.
    """

    @task()
    def task1(x):
        return x + 1

    task2 = task1.options(memory=4)
    assert task1.get_hash() != task2.get_hash()

    # When using versioned tasks, hash should update as well.

    @task(version="1")
    def task3(x):
        return x + 1

    task4 = task3.options(memory=4)
    assert task3.get_hash() != task4.get_hash()


def test_task_typing() -> None:
    """
    Tasks with typing should pass mypy.
    """

    @task()
    def task1(x: int) -> str:
        return str(x)

    @task()
    def task2(x: str) -> bool:
        return True

    expr = task1(10)
    task2(expr)


# Defining NameTuple at top-level scope to work around:
# https://github.com/python/mypy/issues/7281#issuecomment-763842951
class User2(NamedTuple):
    name: str
    age: int


def test_tuple_type_length() -> None:
    """
    get_tuple_type_length() should infer the length of a tuple type.
    """
    assert get_tuple_type_length(Tuple[int, int]) == 2
    assert get_tuple_type_length(Tuple[int, int, int]) == 3
    assert get_tuple_type_length(Tuple[()]) == 0
    assert get_tuple_type_length(Tuple[Any, ...]) is None
    assert get_tuple_type_length(Tuple) is None

    User = namedtuple("User", ["name", "age", "friend"])
    assert get_tuple_type_length(User) == 3

    assert get_tuple_type_length(User2) == 2
