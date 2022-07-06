import datetime
import pickle

import pytest
from dateutil.tz import tzoffset

from redun import File, Scheduler, task
from redun.tests.utils import use_tempdir
from redun.utils import pickle_dumps
from redun.value import (
    FileCache,
    Function,
    InvalidValueError,
    get_type_registry,
    is_unknown_function,
)


class Data:
    """
    Custom datatype.
    """

    def __init__(self, data):
        self.data = data


class DataType(FileCache):
    """
    Register Data to use file-based caching.
    """

    type = Data
    base_path = "tmp"

    def _serialize(self):
        # User defined serialization.
        return pickle_dumps(self.instance.data)

    @classmethod
    def _deserialize(cls, bytes):
        # User defined deserialization.
        return Data(pickle.loads(bytes))


@use_tempdir
def test_value_serialization() -> None:
    task_calls = []

    @task()
    def task1(data):
        task_calls.append("task1")
        return data

    @task()
    def main():
        data = Data("hello")
        return task1(data)

    scheduler = Scheduler()

    assert scheduler.run(main()).data == "hello"
    assert task_calls == ["task1"]

    # Value is cached to filesystem.
    assert File("tmp/773babcb7e0ba318c7981cb5595a5cbe640ab156").exists()

    assert scheduler.run(main()).data == "hello"
    assert task_calls == ["task1"]

    # Delete file.
    File("tmp/773babcb7e0ba318c7981cb5595a5cbe640ab156").remove()

    # Task will safely run again.
    assert scheduler.run(main()).data == "hello"
    assert task_calls == ["task1", "task1"]


def test_parse_datetime() -> None:
    """
    The TypeRegistry should be able to parse datetimes.
    """
    registry = get_type_registry()
    assert registry.parse_arg(datetime.datetime, "2022-07-01") == datetime.datetime(
        2022, 7, 1, 0, 0
    )
    assert registry.parse_arg(datetime.datetime, "2022-07-01 10:05:33") == datetime.datetime(
        2022, 7, 1, 10, 5, 33
    )
    assert registry.parse_arg(datetime.datetime, "2022-07-01 10:05:33 +0400") == datetime.datetime(
        2022, 7, 1, 10, 5, 33, tzinfo=tzoffset(None, 14400)
    )


def hello():
    # Example plain python function that we use below as an argument and return value in workflows.
    return "hello"


def test_function(scheduler: Scheduler) -> None:
    """
    Plain Python functions should be usable as arguments and return values.
    """

    @task
    def return_func():
        return hello

    @task
    def take_func(func):
        return func()

    @task
    def main():
        func = return_func()
        return take_func(func)

    assert scheduler.run(return_func()) == hello
    assert scheduler.run(take_func(hello)) == "hello"
    assert scheduler.run(main()) == "hello"

    registry = get_type_registry()
    assert registry.get_hash(hello) == "d57e3c02fb399ad72e41eba2d18054e7a5d6a196"


def test_function_parse_arg() -> None:
    """
    Functions should be parsable from the command line.
    """
    registry = get_type_registry()
    assert registry.parse_arg(type(hello), "redun.tests.test_value.hello") == hello

    with pytest.raises(ValueError, match="Unexpected format for function name: hello"):
        registry.parse_arg(type(hello), "hello")

    with pytest.raises(ValueError, match="Function not found: bad_module.hello"):
        registry.parse_arg(type(hello), "bad_module.hello")

    with pytest.raises(ValueError, match="Function not found: redun.tests.test_value.bad_func"):
        registry.parse_arg(type(hello), "redun.tests.test_value.bad_func")


def test_local_function(scheduler: Scheduler) -> None:
    """
    Local functions should be detected and rejected.
    """

    def hello2():
        return "hello"

    @task
    def return_func():
        return hello2

    @task
    def return_lambda():
        return lambda: 10

    with pytest.raises(InvalidValueError):
        scheduler.run(return_func())

    with pytest.raises(InvalidValueError):
        scheduler.run(return_lambda())


def test_deleted_function(scheduler: Scheduler) -> None:
    """
    Deleted functions should be safely detected.
    """

    value = Function(hello)
    data = value.serialize()

    # Alter the function name to something that does not exist.
    data2 = data.replace(b"hello", b"xxxxx")

    # Try to deserialize the function.
    func = Function.deserialize("builtin.function", data2)
    assert is_unknown_function(func)

    # Value should not be valid.
    assert not get_type_registry().is_valid(func)

    # Calling the function should trigger the stub function and fail.
    with pytest.raises(
        ValueError, match="Function 'redun.tests.test_value.xxxxx' cannot be found"
    ):
        func()
