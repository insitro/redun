import inspect
import re
import sys
from typing import Any, Callable, Dict, Generic, Iterable, List, Optional, Tuple, TypeVar, cast

from redun.expression import SchedulerExpression, TaskExpression
from redun.hashing import hash_arguments, hash_struct
from redun.namespace import get_current_namespace
from redun.promise import Promise
from redun.utils import get_func_source
from redun.value import Value, get_type_registry

Func = TypeVar("Func", bound=Callable)
Func2 = TypeVar("Func2", bound=Callable)
Result = TypeVar("Result")


def get_task_registry():
    """
    Returns the global task registry.
    """
    return _task_registry


def undefined_task(fullname: str, *args: Any, **kwargs: Any) -> None:
    """
    Default function used for a deserialized Task with unknown definition.
    """
    raise ValueError("Task {} is undefined.".format(fullname))


def get_tuple_type_length(tuple_type: Any) -> Optional[int]:
    """
    Returns the length of a tuple type if inferrable.
    """
    if getattr(tuple_type, "__origin__", None) in (tuple, Tuple):
        # Return type is Tuple[ * ].

        # __args__ is not available on Tuple type in Python 3.9+ because it was removed from
        # _SpecialGenericAlias in:
        #
        #       https://github.com/python/cpython/pull/19984
        #
        # For more info, see bpo-40397 here:
        #
        #       https://bugs.python.org/issue40397
        #
        # typing.get_args was added in Python 3.8 so we can use that instead if we detect we are
        # running on Python 3.8+
        if sys.version_info >= (3, 8):
            from typing import get_args

            tuple_type_args = get_args(tuple_type)
        else:
            tuple_type_args = tuple_type.__args__

        if Ellipsis in tuple_type_args or len(tuple_type_args) == 0:
            # Tuple of unknown length.
            return None

        if tuple_type_args == ((),):
            # Special case for length zero.
            return 0

        return len(tuple_type_args)

    if inspect.isclass(tuple_type) and issubclass(tuple_type, tuple):
        # Return type is namedtuple.
        fields = getattr(tuple_type, "_fields", None)
        if fields:
            return len(fields)

    return None


class Task(Value, Generic[Func]):
    """
    A redun Task.

    Tasks are the basic unit of execution in redun. Tasks are often defined
    using the :func:`redun.task.task` decorator:

    .. code-block:: python

        @task()
        def my_task(x: int) -> int:
            return x + 1
    """

    type_name = "redun.Task"

    def __init__(
        self,
        func: Callable,
        name: Optional[str] = None,
        namespace: Optional[str] = None,
        version: Optional[str] = None,
        compat: Optional[List[str]] = None,
        script: bool = False,
        task_options: Optional[dict] = None,
        task_options_update: Optional[dict] = None,
    ):
        self.name = name or func.__name__
        self.namespace = namespace or get_current_namespace()
        self.func = func
        self.source = get_func_source(func)
        self.version = version
        self.compat = compat or []
        self.script = script
        self.task_options = task_options or {}
        self.task_options_update = task_options_update or {}
        self.nout = self._get_nout()
        self.hash = self._calc_hash()
        self._signature: Optional[inspect.Signature] = None

        self._validate()

    def _get_nout(self) -> Optional[int]:
        """
        Determines nout from task options and return type.

        The precedence is:
        - task_options_update
        - task_options
        - function return type
        """
        nout = self.task_options_update.get("nout")
        if nout is None:
            nout = self.task_options.get("nout")
        if nout is None:
            return_type = self.func.__annotations__.get("return")
            if return_type:
                # Infer nout from return type.
                nout = get_tuple_type_length(return_type)

        # Validate nout.
        if nout is not None:
            if not isinstance(nout, int):
                raise TypeError("nout must be an int")
            if nout < 0:
                raise TypeError("nout must be non-negative")

        return nout

    def __repr__(self) -> str:
        return "Task(fullname={fullname}, hash={hash})".format(
            fullname=self.fullname,
            hash=self.hash[:8],
        )

    def _validate(self) -> None:
        config_args = self.task_options.get("config_args")
        if config_args:
            valid_args = list(self.signature.parameters)
            invalid_args = [
                config_arg for config_arg in config_args if config_arg not in valid_args
            ]
            if invalid_args:
                raise ValueError(
                    f"Invalid config args: {', '.join(invalid_args)}. Expected all config_args to "
                    f"be one of the function args/kwargs: {', '.join(valid_args)}"
                )

        if self.namespace:
            if not re.match("^[A-Za-z_][A-Za-z_0-9.]+$", self.namespace):
                raise ValueError(
                    "Task namespace must use only alphanumeric characters, "
                    "underscore '_', and dot '.'."
                )

        if not re.match("^[A-Za-z_][A-Za-z_0-9]+$", self.name):
            raise ValueError("Task name must use only alphanumeric characters and underscore '_'.")

    @property
    def fullname(self) -> str:
        """
        Returns the fullname of a Task: '{namespace}.{name}'
        """
        if self.namespace:
            return self.namespace + "." + self.name
        else:
            return self.name

    def _call(self: "Task[Callable[..., Result]]", *args: Any, **kwargs: Any) -> Result:
        """
        Returns a lazy Expression of calling the task.
        """
        return cast(
            Result,
            TaskExpression(
                self.fullname, args, kwargs, self.task_options_update, length=self.nout
            ),
        )

    # Typing strategy: Ideally, we could use more honest types for the arguments
    # and return type of `Task.__call__()`, such as `Union[Arg, Expression[Arg]]`
    # and `TaskExpression[Result]` respectively. However, dynamically creating
    # such a signature has several challenges at the moment, such as requiring a
    # mypy plugin and forcing users to wrap every task return type. Therefore, we
    # compromise and force cast `Task.__call__()` to have the same signature as
    # the wrapped function, `Func`. This allows users to write tasks with very
    # natural types, and mypy can catch most type errors. The one trade off is
    # that this approach is too permissive about using `TaskExpression[T]`
    # wherever `T` is allowed.

    # Cast the signature to match the wrapped function.
    __call__: Func = cast(Func, _call)

    def options(self, **task_options_update: Any) -> "Task[Func]":
        """
        Returns a new Task with task_option overrides.
        """
        new_task_options_update = {
            **self.task_options_update,
            **task_options_update,
        }
        return Task(
            self.func,
            name=self.name,
            namespace=self.namespace,
            version=self.version,
            compat=self.compat,
            script=self.script,
            task_options=self.task_options,
            task_options_update=new_task_options_update,
        )

    def _calc_hash(self) -> str:
        # TODO: implement for real.
        if self.compat:
            return self.compat[0]

        if self.task_options_update:
            task_options_hash = [get_type_registry().get_hash(self.task_options_update)]
        else:
            task_options_hash = []

        if self.version is None:
            source = get_func_source(self.func) if self.func else ""
            return hash_struct(["Task", self.fullname, "source", source] + task_options_hash)
        else:
            return hash_struct(
                ["Task", self.fullname, "version", self.version] + task_options_hash
            )

    def __getstate__(self) -> dict:
        # Note: We specifically don't serialize func. We will use the
        # TaskRegistry during deserialization to fetch the latest func for a task.
        return {
            "name": self.name,
            "namespace": self.namespace,
            "version": self.version,
            "hash": self.hash,
            "compat": self.compat,
            "script": self.script,
            "task_options": self.task_options_update,
        }

    def __setstate__(self, state) -> None:
        self.name = state["name"]
        self.namespace = state["namespace"]
        self.version = state["version"]
        self.hash = state["hash"]
        self.compat = state.get("compat", [])
        self.script = state.get("script", False)
        self.task_options_update = state.get("task_options", {})

        # Set func from TaskRegistry.
        registry = get_task_registry()
        _task = registry.get(self.fullname)
        if _task:
            self.func = _task.func
            self.task_options = _task.task_options
            self.source = get_func_source(self.func)
        else:
            self.func = lambda *args, **kwargs: undefined_task(self.fullname, *args, **kwargs)
            self.task_options = {}
            self.source = ""

        self.nout = self._get_nout()
        self._signature = None

    def is_valid(self) -> bool:
        """
        Returns True if the Task Value is still valid (task hash matches registry).

        Tasks are first-class Values in redun. They can be cached and fetched
        in future executions. When fetching a Task from the cache, the cached
        hash might no longer exist in the code base (registered tasks).
        """
        return self.hash == self._calc_hash()

    def get_hash(self, data: Optional[bytes] = None) -> str:
        """
        Returns the Task hash.
        """
        return self.hash

    # Note: we can't parameterize PartialTask to a more specific type at this
    # time, due to the complexity of calculating the remaining parameter signature.
    def partial(
        self: "Task[Callable[..., Result]]", *args, **kwargs
    ) -> "PartialTask[Callable[..., Result], Callable[..., Result]]":
        """
        Partially apply some arguments to the Task.
        """
        return PartialTask(self, args, kwargs)

    @property
    def signature(self) -> inspect.Signature:
        """
        Signature of the function wrapped by the task.
        """
        assert self.func
        if not self._signature:
            self._signature = inspect.signature(self.func)
        return self._signature


class SchedulerTask(Task[Func]):
    """
    A Task that executes within the scheduler to allow custom evaluation.
    """

    def _call(self: "SchedulerTask[Callable[..., Result]]", *args: Any, **kwargs: Any) -> Result:
        """
        Returns a lazy Expression of calling the task.
        """
        return cast(Result, SchedulerExpression(self.fullname, args, kwargs))

    __call__ = cast(Func, _call)


class PartialTask(Task[Func], Generic[Func, Func2]):
    """
    A Task with only some arguments partially applied.

    The type of this class is parameterized by `Func` and `Func2`, where
    `Func2` is the type of the original function and `Func` is the type
    of partially applied function. They should match on their return types.
    """

    type_name = "redun.PartialTask"

    def __init__(self, task: Task[Func2], args: tuple, kwargs: dict):
        self.task = task
        self.args = tuple(args)
        self.kwargs = kwargs

        super().__init__(task.func, name=task.name, namespace=task.namespace)

    def __repr__(self) -> str:
        return (
            "PartialTask(fullname={fullname}, hash={hash}, args={args}, kwargs={kwargs})".format(
                fullname=self.fullname,
                hash=self.hash[:8],
                args=repr(self.args),
                kwargs=repr(self.kwargs),
            )
        )

    def _call(
        self: "PartialTask[Callable[..., Result], Callable[..., Result]]",
        *args: Any,
        **kwargs: Any,
    ) -> Result:
        # By calling the original task, we ensure that a normal pre-registered
        # task will be the one in the CallGraph recording.
        return self.task(*self.args, *args, **self.kwargs, **kwargs)

    # Cast the signature to match the wrapped function.
    __call__: Func = cast(Func, _call)

    def __getstate__(self) -> dict:
        """
        Returns state for pickling.
        """
        state = super().__getstate__()
        state.update({"task": self.task.__getstate__(), "args": self.args, "kwargs": self.kwargs})
        return state

    def __setstate__(self, state: dict) -> None:
        """
        Sets state from pickle.
        """
        super().__setstate__(state)
        self.task = Task.__new__(Task)
        self.task.__setstate__(state["task"])
        self.args = state["args"]
        self.kwargs = state["kwargs"]

    def _calc_hash(self) -> str:
        return hash_struct(
            [
                "PartialTask",
                self.task._calc_hash(),
                hash_arguments(get_type_registry(), self.args, self.kwargs),
            ]
        )

    def is_valid(self) -> bool:
        return self.task.is_valid()

    def options(
        self: "PartialTask[Callable[..., Result], Callable[..., Result]]",
        **task_options_update: Any,
    ) -> "Task[Func]":
        """
        Returns a new Task with task_option overrides.
        """
        return cast(
            Task[Func],
            self.task.options(**task_options_update).partial(*self.args, **self.kwargs),
        )

    # Note: we can't parameterize PartialTask to a more specific type at this
    # time, due to the complexity of calculating the remaining parameter signature.
    def partial(
        self: "PartialTask[Callable[..., Result], Callable[..., Result]]", *args, **kwargs
    ) -> "PartialTask[Callable[..., Result], Callable[..., Result]]":
        """
        Partially apply some arguments to the Task.
        """
        # Combine new arguments to previously applied arguments.
        args2 = self.args + args
        kwargs2 = {
            **self.kwargs,
            **kwargs,
        }
        return PartialTask(self.task, args2, kwargs2)


def task(
    name: Optional[str] = None,
    namespace: Optional[str] = None,
    version: Optional[str] = None,
    compat: Optional[List[str]] = None,
    script: bool = False,
    **task_options: Any,
) -> Callable[[Func], Task[Func]]:
    """
    Decorator to register a function as a redun :class:`Task`.

    Parameters
    ----------
    name : Optional[str]
        Name of task (Default: infer from function `func.__name__`)
    namespace : Optional[str]
        Namespace of task (Default: None)
    version : Optional[str]
        Optional manual versioning for a task (Default: source code of task is
        hashed).
    compat : Optional[List[str]]
        Optional redun version compatibility. Not currently implemented.
    script : bool
        If True, this is a script-style task which returns a shell script string.
    **task_options
        Additional options for configuring a task.
    """

    def deco(func: Func) -> Task[Func]:
        nonlocal namespace

        # Determine task namespace.
        if not namespace:
            namespace = getattr(sys.modules[func.__module__], "redun_namespace", None)

        _task: Task[Func] = Task(
            func,
            name=name,
            namespace=namespace,
            version=version,
            compat=compat,
            script=script,
            task_options=task_options,
        )
        get_task_registry().add(_task)
        return _task

    return deco


def scheduler_task(
    name: Optional[str] = None, namespace: Optional[str] = None, version: str = "1"
) -> Callable[[Callable[..., Promise[Result]]], SchedulerTask[Callable[..., Result]]]:
    """
    Decorator to register a function as a scheduler task.

    Unlike usual tasks, scheduler tasks are lower-level tasks that are evaluated
    within the :class:`Scheduler` and allow defining custom evaluation semantics.
    For example, one can implement `cond()`, `seq()` and `catch()` using
    scheduler tasks.

    When evaluated, scheduler tasks are called with a reference to the
    :class:`Scheduler` and the parent :class:`Job` as it's first two arguments.
    It's remaining arguments are the same as those passed from the user, however,
    they are not evaluated and may contain :class:`Expression`s. It is the
    responsibility of the scheduler task to explicitly evaluate arguments
    by using `Scheduler.evaluate()` as needed. Overall, this allows the scheduler
    task to implement custom evaluation semantics. Lastly, the scheduler task
    must return a :class:`Promise` that resolves to the result of the task.

    This concept corresponds to fexpr in Lisp:
    - https://en.wikipedia.org/wiki/Fexpr

    For example, one could implement a lazy if-statement called `cond` using
    this scheduler task:

    .. code-block:: python

        @scheduler_task()
        def cond(scheduler, parent_job, pred_expr, then_expr, else_expr):
            def then(pred):
                if pred:
                    return scheduler.evaluate(then_expr, parent_job=parent_job)
                else:
                    return scheduler.evaluate(else_expr, parent_job=parent_job)

            return scheduler.evaluate(pred_expr, parent_job=parent_job).then(then)

    Once defined, the new `cond` expression can be used like this:

    .. code-block:: python

        @task()
        def main():
            result = task1()
            return cond(result, task2(), task3())
    """

    def deco(func: Callable[..., Promise[Result]]) -> SchedulerTask[Callable[..., Result]]:
        nonlocal name, namespace

        if not namespace:
            namespace = getattr(sys.modules[func.__module__], "redun_namespace", None)

        _task: SchedulerTask[Callable[..., Result]] = SchedulerTask(
            func,
            name=name,
            namespace=namespace,
            version=version,
        )
        get_task_registry().add(_task)
        return _task

    return deco


class TaskRegistry:
    """
    A registry of currently registered Tasks.

    The @task() decorator registers tasks to the current registry.
    """

    def __init__(self):
        self._tasks: Dict[str, Task] = {}

    def add(self, task: Task) -> None:
        self._tasks[task.fullname] = task

    def get(self, task_name: Optional[str] = None, hash: Optional[str] = None) -> Optional[Task]:
        if task_name:
            return self._tasks.get(task_name)
        elif hash:
            for task in self._tasks.values():
                if task.hash == hash:
                    return task
            return None
        else:
            raise ValueError("No task field given.")

    def __iter__(self) -> Iterable[Task]:
        return iter(self._tasks.values())


# Global signleton task registry.
_task_registry = TaskRegistry()
