from typing import Any, Optional, TypeVar, cast

from redun.expression import SchedulerExpression
from redun.promise import Promise
from redun.scheduler import Job, Scheduler
from redun.task import scheduler_task

T = TypeVar("T")


def get_context_value(context: dict, var_path: str, default: T) -> T:
    """
    Returns a value from a context dict using a dot-separated path (e.g. 'foo.bar.baz').

    Parameters
    ----------
    context : dict
        A redun context dict.
    var_path : str
        A dot-separated path for a variable to fetch from the current context.
    default : T
        A default value to return if the context variable is not defined.
    """
    parts = var_path.split(".")
    value = context
    try:
        for part in parts:
            if not isinstance(value, dict):
                return default
            value = value[part]
    except KeyError:
        return default
    return cast(T, value)


@scheduler_task(namespace="redun")
def get_context(
    scheduler: Scheduler,
    parent_job: Job,
    sexpr: SchedulerExpression,
    var_path: str,
    default: Optional[Any] = None,
) -> Promise:
    """
    Returns a value from the current context.

    Parameters
    ----------
    var_path : str
        A dot-separated path for a variable to fetch from the current context.
    default : Optional[Any]
        A default value to return if the context variable is not defined. None by default.
    """
    return scheduler.evaluate(parent_job.get_context(), parent_job=parent_job).then(
        lambda context: get_context_value(context, var_path, default)
    )
