from textwrap import dedent
from typing import Any, Callable, Dict, List, Sequence, Tuple, TypeVar, Union

from redun.expression import Expression, SchedulerExpression
from redun.promise import Promise
from redun.scheduler import Job, Scheduler, scheduler_task
from redun.task import Task, task

S = TypeVar("S")
T = TypeVar("T")


@task(namespace="redun", version="1")
def compose_apply(tasks: Sequence[Task], *args: Any, **kwargs: Any) -> Any:
    """
    Helper function for applying a composition of Tasks to arguments.
    """
    rev_tasks = reversed(tasks)
    result = next(rev_tasks)(*args, **kwargs)
    for a_task in rev_tasks:
        result = a_task(result)
    return result


def compose(*tasks: Task) -> Task:
    """
    Compose Tasks together.
    """
    return compose_apply.partial(tasks)


@task(namespace="redun", version="1")
def identity(x: T) -> T:
    """
    Returns its input argument.
    """
    return x


def delay(x: T) -> Task[Callable[[], T]]:
    """
    Delay the evaluation of a value x.

    The name `delay()` is inspired from other programming languages:
    - http://web.mit.edu/scheme_v9.2/doc/mit-scheme-ref/Promises.html
    - http://people.cs.aau.dk/~normark/prog3-03/html/notes/eval-order_themes-delay-stream-section.html  # noqa: E501
    - https://docs.racket-lang.org/reference/Delayed_Evaluation.html
    """
    # PartialTasks do not evaluate their arguments, so this acts similar to
    # a closure: `lambda: x`.
    return identity.partial(x)


def force(x: Task[Callable[[], T]]) -> T:
    """
    Force the evaluation of a delayed evaluation.

    The name `force()` is inspired from other programming languages:
    - http://web.mit.edu/scheme_v9.2/doc/mit-scheme-ref/Promises.html
    - http://people.cs.aau.dk/~normark/prog3-03/html/notes/eval-order_themes-delay-stream-section.html  # noqa: E501
    - https://docs.racket-lang.org/reference/Delayed_Evaluation.html
    """
    # delay() produces a Task of no arguments. To force evaluation we just need
    # to call the Task.
    return x()


@scheduler_task(namespace="redun")
def seq(
    scheduler: Scheduler, parent_job: Job, sexpr: SchedulerExpression, exprs: Sequence[Any]
) -> Promise:
    """
    Evaluate the expressions serially.

    The name `seq()` is inspired from Haskell:
    - https://wiki.haskell.org/Seq
    """
    if not exprs:
        # No expressions, so return empty list.
        return Promise(lambda resolve, reject: resolve([]))

    result = []

    def then(pair):
        i, value = pair
        i += 1
        result.append(value)
        if i < len(exprs):
            return scheduler.evaluate((i, exprs[i]), parent_job=parent_job).then(then)
        else:
            return result

    return scheduler.evaluate((0, exprs[0]), parent_job=parent_job).then(then)


@task(namespace="redun", version="1")
def const(x: T, _: Any) -> T:
    """
    Returns the first argument unchanged (constant) and discards the second.

    This is useful for forcing execution of the second argument, but discarding
    it's value from downstream parts of the workflow.
    """
    return x


@task(namespace="redun", version="1")
def apply_func(func: Callable[..., T], *args: Any, **kwargs: Any) -> T:
    """
    Apply a Python function to possibly lazy arguments.

    The function `func` must be defined at the module-level.

    .. code-block:: python

        lazy_list = a_task_returning_a_list()
        lazy_length = apply_func(len)(lazy_list)
    """
    return func(*args, **kwargs)


def as_task(func: Callable[..., T]) -> Task[Callable[..., T]]:
    """
    Transform a plain Python function into a redun Task.

    The function `func` must be defined at the module-level.

    .. code-block:: python

        assert as_task(max)(10, 20, 30, 25) == 30
    """
    return apply_func.partial(func)


@task(namespace="redun", version="1")
def eval_(code: str, *args: Any, **kwargs: Any) -> Any:
    """
    Evaluate `code` with `kwargs` as local variables.

    If a position argument is given, its value is assigned to local
    variables defined by `pos_args`.

    This can be useful for small manipulations of lazy values.

    .. code-block:: python

        records = task1()
        names = eval_("[record.name for record in records]", records=records)
        result = task2(names)
    """
    pos_args = kwargs.pop("pos_args", [])
    if len(pos_args) != len(args):
        raise TypeError(
            f"Wrong number of positional arguments given {len(args)}, expected {len(pos_args)}."
        )
    vars = dict(zip(pos_args, args))
    vars.update(kwargs)

    return eval(dedent(code), vars)


@scheduler_task(namespace="redun", version="1")
def map_(
    scheduler: Scheduler,
    parent_job: Job,
    sexpr: SchedulerExpression,
    a_task: Task,
    values: Sequence[Any],
) -> Promise:
    """
    Map a task to a list of values, similar to `map(f, xs)`.
    """
    tasks = [a_task]

    # As an optimization, compose multiple maps into one.
    # e.g. map_(g, map_(f, xs)) == map_(compose(g, f), xs)
    while isinstance(values, SchedulerExpression) and values.task_name == "redun.map_":
        tasks.append(values.args[0])
        values = values.args[1]

    if len(tasks) == 1:
        [a_task] = tasks
    else:
        a_task = compose(*tasks)

    def then(a_task: Task) -> Promise:
        if isinstance(values, (list, tuple)):
            # Ready to perform parallel map.
            return scheduler.evaluate(
                [a_task(value) for value in values],
                parent_job=parent_job,
            )
        else:
            # Need to evaluate list first.
            return scheduler.evaluate(values, parent_job=parent_job).then(
                lambda values: scheduler.evaluate(
                    [a_task(value) for value in values],
                    parent_job=parent_job,
                )
            )

    # Evaluate task first, in case it's an expression.
    return scheduler.evaluate(a_task, parent_job=parent_job).then(then)


def starmap(
    a_task: Task[Callable[..., T]], kwargs: Union[List[Dict], Expression[List[Dict]]] = []
) -> List[T]:
    """
    Map a task to a list of keyword arguments.
    """
    return map_(eval_.partial("a_task(**x)", pos_args=["x"], a_task=a_task), kwargs)


@task(namespace="redun", version="1")
def flatten(list_of_lists: Sequence[Sequence[T]]) -> List[T]:
    """
    Flatten a list of lists into a flat list.
    """
    return [value for lst in list_of_lists for value in lst]


@task(namespace="redun", version="1")
def flat_map(a_task: Task[Callable[..., List[T]]], values: List) -> List[T]:
    """
    Apply a task `a_task` on a sequence of `values` and flatten the result.
    """
    return flatten(map_(a_task, values))


@task(namespace="redun", version="1")
def zip_(*lists: List[T]) -> List[Tuple[T, ...]]:
    """
    Zips two or lists into a list of tuples.

    This is a task equivalent of zip().
    """
    return list(zip(*lists))
