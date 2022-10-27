from typing import Any, Callable

from redun.task import Task, wraps_task


def VarArg(type=Any):
    """A *args-style variadic positional argument"""
    return type


def KwArg(type=Any):
    """A **kwargs-style variadic keyword argument"""
    return type


def square_task():
    """A trivial wrapper that is not defined in the same module as the test."""

    @wraps_task(wrapper_name="square_task")
    def _square_task(inner_task: Task) -> Callable[[VarArg(Any), KwArg(Any)], Any]:
        def do_square(*args, **kwargs):
            return inner_task.func(*args, **kwargs) ** 2

        return do_square

    return _square_task
