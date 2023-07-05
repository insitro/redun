import pickle
import sys
from collections import namedtuple
from typing import Any, Callable, NamedTuple, Tuple

import pytest

import redun
from redun import Task, task
from redun.expression import Expression
from redun.namespace import compute_namespace
from redun.scheduler import set_arg_defaults
from redun.task import (
    CacheCheckValid,
    CacheScope,
    Func,
    get_task_registry,
    get_tuple_type_length,
    wraps_task,
)
from redun.tests.task_test_helpers import square_task
from redun.utils import get_func_source, pickle_dumps
from redun.value import get_type_registry


def test_task_config() -> None:
    """
    Task should be properly configured by decorator.
    """

    @task()
    def my_task():
        return 10

    assert compute_namespace(my_task) == my_task.namespace
    assert my_task.fullname == Task._format_fullname(compute_namespace(my_task), "my_task")

    @task(name="custom_name", namespace="my_namespace", load_module="custom.module")
    def my_task2():
        return 10

    assert compute_namespace(my_task2, namespace="my_namespace") == my_task2.namespace
    assert my_task2.load_module == "custom.module"

    redun.namespace("my_namespace2")

    @task()
    def my_task3():
        return 10

    assert compute_namespace(my_task3) == my_task3.namespace

    redun.namespace()

    assert my_task.name == "my_task"
    assert my_task.fullname == "my_task"
    assert my_task.load_module == "redun.tests.test_tasks"
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


def helper():
    print("hello")


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

    # Hash includes are order invariant
    @task(hash_includes=["a", 2])
    def task1():
        return 10

    assert task1.get_hash() == "36fc0f089161ea4142ef20272e581ae8fc0b1921"

    @task(hash_includes=[2, "a"])
    def task1():
        return 10

    assert task1.get_hash() == "36fc0f089161ea4142ef20272e581ae8fc0b1921"

    # Testing that a change in helper source code will modify the task hash
    global helper

    @task(hash_includes=[helper])
    def task1():
        return 10

    assert task1.get_hash() == "3177281599f4961164534cd2f9d94a892db123b2"

    helper.__code__ = (lambda _: print("hello! i am different now!")).__code__

    @task(hash_includes=[helper])
    def task1():
        return 10

    assert task1.get_hash() == "a11d9a4abf033b0ced1f9e766f407b3c6151ed29"


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
    assert task1c.get_task_options() == {"memory": 4}

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

    # A Task that refers to non-existent code should not be valid.
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


def test_override_source() -> None:
    """
    Task source can be overridden
    """

    # A task's source can be overridden
    @task()
    def task1():
        return 10

    @task(source=get_func_source(task1.func))
    def task2():
        return 20

    assert task1.source == task2.source == get_func_source(task1.func)

    # A task's source is not pickled
    assert "source" not in task2.__getstate__()

    # But unpickling will reconstruct task.source from a same-name in-registry task.
    data = pickle_dumps(task2)
    task2b = pickle.loads(data)
    # Ensure task deserialized correctly.
    assert task2b.source == get_task_registry().get("task2").source


def test_hash_uses_source_instead_of_inspect() -> None:
    """For version=None tasks, the hash uses task.source instead of dynamic inspection."""

    # Use type ignore so we can redefine task3 multiple times
    @task
    def task3():
        return 30

    hash3a = task3.get_hash()

    @task  # type: ignore
    def task3():
        return 30

    hash3b = task3.get_hash()

    @task(source="some source")  # type: ignore
    def task3():
        return 30

    hash3c = task3.get_hash()

    assert hash3a == hash3b
    assert hash3a != hash3c


def test_task_options() -> None:
    """
    Task options should be available on the task and updates by options().
    """

    @task(executor="batch")
    def task1():
        return 10

    assert task1.has_task_option("executor")
    assert "batch" == task1.get_task_option("executor")
    assert "batch" == task1.get_task_option("executor", "default_executor")

    assert not task1.has_task_option("memory")
    assert task1.get_task_option("memory") is None
    assert 1000 == task1.get_task_option("memory", 1000)

    assert task1.get_task_options() == {"executor": "batch"}

    # The expression should not need any updated options.
    assert task1().task_expr_options == {}

    # When we apply an update to the task, it should appear on the new task and expression.
    task2 = task1.options(executor="local")

    assert task2.has_task_option("executor")
    assert "local" == task2.get_task_option("executor")
    assert "local" == task2.get_task_option("executor", "default_executor")

    assert not task2.has_task_option("memory")
    assert task2.get_task_option("memory") is None
    assert 1000 == task2.get_task_option("memory", 1000)

    assert task2.get_task_options() == {"executor": "local"}

    assert task2().task_expr_options == {"executor": "local"}

    # Additional updates should accumulate.
    task3 = task2.options(memory=4)

    assert task3.has_task_option("executor")
    assert "local" == task3.get_task_option("executor")
    assert "local" == task3.get_task_option("executor", "default_executor")

    assert task3.has_task_option("memory")
    assert 4 == task3.get_task_option("memory")
    assert 4 == task3.get_task_option("memory", 1000)

    assert task3.get_task_options() == {
        "executor": "local",
        "memory": 4,
    }

    assert task3().task_expr_options == {
        "executor": "local",
        "memory": 4,
    }


def test_task_options_hash() -> None:
    """
    _task_options_overrides should contribute to task hash.
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


def test_naked_task() -> None:
    """
    A naked task decorator (no arguments) should also produce a Task.
    """

    @task
    def add(a: int, b: int) -> int:
        return a + b

    assert isinstance(add, Task)


def test_wraps_task() -> None:
    def doubled_task(offset: int = 0) -> Callable[[Func], Task[Func]]:
        @wraps_task(wrapper_hash_includes=[offset])
        def _doubled_task(inner_task: Task) -> Callable[[Task[Func]], Func]:
            def do_doubling(*task_args, **task_kwargs) -> Any:

                return (
                    inner_task.func(*task_args, **task_kwargs)
                    + inner_task.func(*task_args, **task_kwargs)
                    + offset
                )

            return do_doubling

        return _doubled_task

    # 1. Test that the task will get implicitly created
    @doubled_task()
    def value_task_unnested(x: int):
        return 1 + x

    # Check we can call it and arguments gets passed.
    assert 8 == value_task_unnested.func(3)
    assert 10 == value_task_unnested.func(4)

    # The tasks get registered.
    assert "value_task_unnested" in get_task_registry()._tasks
    assert "_doubled_task.value_task_unnested" in get_task_registry()._tasks

    # 2. Test that we can accept an explicit task
    @doubled_task()
    @task
    def value_task_nested(x: int):
        return 1 + x

    # Check we can call it and arguments gets passed.
    assert 8 == value_task_nested.func(3)
    assert 10 == value_task_nested.func(4)

    # The tasks get registered.
    assert "value_task_nested" in get_task_registry()._tasks
    assert "_doubled_task.value_task_nested" in get_task_registry()._tasks

    @doubled_task()
    def value_task(x: int):
        return 1 + x

    # 3. Demonstrate that we can return tasks lazily.
    def listify_task() -> Callable[[Func], Task[Func]]:
        @wraps_task(wrapper_name="renamed_task")
        def _listify_task(inner_task: Task) -> Callable[[Task[Func]], Func]:
            def make_list(*task_args, copies=1, **task_kwargs) -> Any:

                return [inner_task] * copies

            return make_list

        return _listify_task

    @listify_task()
    def float_task():
        return 3.1415

    assert "float_task" in get_task_registry()._tasks
    assert "renamed_task.float_task" in get_task_registry()._tasks

    float_task_inner = get_task_registry()._tasks["renamed_task.float_task"]
    assert float_task.func() == [float_task_inner]
    assert float_task.func(copies=3) == [float_task_inner, float_task_inner, float_task_inner]

    # 4. Try using a wrapper that is not defined in this module, so we can tell whether the
    # `load_module` points here, as it should. This is more likely representative of real use,
    # that the wrapper and its use will not be colocated.
    @square_task()
    @task(namespace="custom_namespace")
    def value_task2():
        return 7

    assert value_task2.func() == 49
    assert value_task2.load_module == sys.modules[__name__].__name__
    assert value_task2.namespace == "custom_namespace"

    assert "custom_namespace.value_task2" in get_task_registry()._tasks
    assert "custom_namespace.square_task.value_task2" in get_task_registry()._tasks

    # 5. Try deeper nesting with explicit task creating
    @listify_task()
    @doubled_task(offset=1)
    @task()
    def extra_nested():
        return -2

    extra_nested_middle_task = get_task_registry()._tasks["renamed_task.extra_nested"]
    assert extra_nested.func(copies=2) == [extra_nested_middle_task, extra_nested_middle_task]
    assert extra_nested_middle_task.func() == -3
    assert get_task_registry()._tasks["_doubled_task.renamed_task.extra_nested"].func() == -2

    # 6. Deeper nesting with implicit task creation
    @listify_task()
    @doubled_task(offset=1)
    def extra_nested_implicit():
        return -2

    extra_nested_middle_task = get_task_registry()._tasks["renamed_task.extra_nested_implicit"]
    assert extra_nested_implicit.func(copies=2) == [
        extra_nested_middle_task,
        extra_nested_middle_task,
    ]
    assert extra_nested_middle_task.func() == -3
    assert (
        get_task_registry()._tasks["_doubled_task.renamed_task.extra_nested_implicit"].func() == -2
    )

    # 7. Test hash propagation via source
    extra_nested_stored_hash = extra_nested.hash

    @listify_task()  # type: ignore
    @doubled_task(offset=1)
    def extra_nested():
        return -3

    # These are the same except at the inner-most level
    assert extra_nested_stored_hash != extra_nested.hash

    # 8. Test hash propagation via extra data
    @listify_task()  # type: ignore
    @doubled_task(offset=2)
    def extra_nested():
        return -2

    assert extra_nested_stored_hash != extra_nested.hash

    # 9. It's turtles all the way down...
    # These tasks have repeated names, which have helped catch several errors.
    @doubled_task()
    @doubled_task()
    @doubled_task()
    @doubled_task()
    @doubled_task()
    def turtles():
        return 2

    assert turtles.func() == 64

    # After all our renaming, make sure our registry is still a correct index of task names.
    for task_name, task_ in get_task_registry()._tasks.items():
        assert task_name == task_.fullname


def test_cache_options() -> None:
    """All the methods for setting cache options get sanitized into enums."""

    # Start with setting them on the method
    @task(cache=False)
    def task1():
        return None

    assert task1.get_task_option("cache_scope") == CacheScope.CSE

    @task(cache=True)
    def task2():
        return None

    assert task2.get_task_option("cache_scope") == CacheScope.BACKEND

    @task(cache_scope="CSE")
    def task3():
        return None

    assert task3.get_task_option("cache_scope") == CacheScope.CSE

    @task(cache_scope=CacheScope.CSE)
    def task4():
        return None

    assert task4.get_task_option("cache_scope") == CacheScope.CSE

    @task()
    def task5():
        return None

    # And retest with option overrides.
    assert task5.options(cache=False).get_task_option("cache_scope") == CacheScope.CSE
    assert task5.options(cache=True).get_task_option("cache_scope") == CacheScope.BACKEND
    assert task5.options(cache_scope="CSE").get_task_option("cache_scope") == CacheScope.CSE
    assert (
        task5.options(cache_scope=CacheScope.CSE).get_task_option("cache_scope") == CacheScope.CSE
    )
    with pytest.raises(ValueError, match="'typo' is not a valid CacheScope"):
        task5.options(cache_scope="typo")

    assert task5.options(check_valid="full").get_task_option("check_valid") == CacheCheckValid.FULL
    assert (
        task5.options(check_valid=CacheCheckValid.FULL).get_task_option("check_valid")
        == CacheCheckValid.FULL
    )
    assert (
        task5.options(check_valid="shallow").get_task_option("check_valid")
        == CacheCheckValid.SHALLOW
    )
    assert (
        task5.options(check_valid=CacheCheckValid.SHALLOW).get_task_option("check_valid")
        == CacheCheckValid.SHALLOW
    )
    with pytest.raises(ValueError, match="'typo' is not a valid CacheCheckValid"):
        task5.options(check_valid="typo")

    # Check the enum will traverse serialization.
    type_registry = get_type_registry()
    deserialized = type_registry.deserialize(
        "redun.Task", type_registry.serialize(task4.options(cache_scope=CacheScope.NONE))
    )
    assert deserialized.get_task_option("cache_scope") == CacheScope.NONE
