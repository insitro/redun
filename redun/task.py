import enum
import inspect
import re
import sys
import typing
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    Iterable,
    List,
    Optional,
    Tuple,
    TypeVar,
    Union,
    cast,
    overload,
)

from redun.expression import SchedulerExpression, TaskExpression
from redun.hashing import hash_arguments, hash_eval, hash_struct
from redun.namespace import compute_namespace
from redun.promise import Promise
from redun.utils import get_func_source
from redun.value import TypeRegistry, Value, get_type_registry

Func = TypeVar("Func", bound=Callable)
Func2 = TypeVar("Func2", bound=Callable)
Result = TypeVar("Result")


class CacheCheckValid(enum.Enum):
    """
    Types of validity checking for cache hits.
    """

    FULL = "full"  # Recursively check all intermediate values in the call tree
    SHALLOW = "shallow"  # Only check the final result for validity


class CacheScope(enum.Enum):
    """
    Types of cache hits and misses.
    """

    NONE = "NONE"  # Do not cache
    CSE = "CSE"  # Only allow cache hits against tasks from this execution
    BACKEND = "BACKEND"  # Allow cache hits from any execution known to the backend


class CacheResult(enum.Enum):
    """
    Types of cache hits and misses.
    """

    CSE = "CSE"  # Common Subexpression Elimination hit, i.e., an ultimate reduction from this
    # execution
    SINGLE = "SINGLE"  # The result from a single reduction, from a different execution,
    # i.e., an evaluation cache hit
    ULTIMATE = "ULTIMATE"  # The ultimate result from the expression, from a different execution,
    # i.e., a call cache hit
    MISS = "MISS"  # Complete cache miss.


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
    Returns the length of a tuple type if inferable.
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


def split_task_fullname(task_fullname: str) -> Tuple[str, str]:
    """
    Split a Task fullname into a namespace and name.
    """
    if "." in task_fullname:
        namespace, name = task_fullname.rsplit(".", 1)
        return namespace, name
    else:
        return "", task_fullname


class Task(Value, Generic[Func]):
    """
    A redun Task.

    Tasks are the basic unit of execution in redun. Tasks are often defined
    using the :func:`redun.task.task` decorator:

    .. code-block:: python

        @task()
        def my_task(x: int) -> int:
            return x + 1

    Similar to pickling of functions, Tasks specify the work to execute by reference, not by
    value. This is important for serialization and caching, since a task is always reattached
    to the latest implementation and task options just-in-time.

    Task options may give direction to the scheduler about the execution environment, but must
    not alter the result of the function, since changing options will not trigger cache
    invalidation and we are allowed to return cached results with differing options. These
    can be provided at two times: 1) at module-load time by decorator options, 2) at run-time
    by creating task expressions. The former are not serialized so they are attached the latest
    value in the source code. The latter must be serialized, since they are not available
    except at run-time.

    Task are routinely serialized and deserialized and are cached based on their serialization.
    This relies on the serialized format being as minimal as possible, while providing the needed
    reference to the code to run.

    If needed, extra hashable data may be provided to trigger re-evaluation/cache-invalidation.
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
        task_options_base: Optional[dict] = None,
        task_options_override: Optional[dict] = None,
        hash_includes: Optional[list] = None,
        source: Optional[str] = None,
    ):
        self.name = name or func.__name__
        self.namespace = compute_namespace(func, namespace)
        self.func = func
        if source is not None:
            self.source = source
        else:
            self.source = get_func_source(func)

        self.version = version
        self.compat = compat or []
        self.script = script
        # The base set of options, typically defined by module-load time code, which are not
        # serialized and are not hashed
        self._task_options_base = task_options_base or {}
        # Overrides to the options, typically provided at run-time, which are serialized and
        # hashed.
        self._task_options_override = task_options_override or {}
        self._signature: Optional[inspect.Signature] = None
        # Extra data to hash, but not serialize
        self._hash_includes = hash_includes
        self.recompute_hash()

        self._validate()

    def recompute_hash(self):
        self.hash = self._calc_hash()

    @property
    def nout(self) -> Optional[int]:
        """
        Determines nout from task options and return type.

        The precedence is:
        - the task option
        - function return type
        """
        if self.has_task_option("nout"):
            nout = self.get_task_option("nout")
        else:
            # Infer nout from return type.
            return_type = self.func.__annotations__.get("return")
            nout = get_tuple_type_length(return_type)

        return nout

    T = TypeVar("T")

    @overload
    def get_task_option(self, option_name: str) -> Optional[Any]:
        ...

    @overload
    def get_task_option(self, option_name: str, default: T) -> T:
        ...

    def get_task_option(self, option_name: str, default: Optional[T] = None) -> Optional[T]:
        """
        Fetch the requested option, preferring run-time updates over options from task
        construction. Like the dictionary `get` method, returns the default
        a `KeyError` on missing keys.
        """

        if option_name in self._task_options_override:
            return self._task_options_override[option_name]
        if option_name in self._task_options_base:
            return self._task_options_base[option_name]
        return default

    def get_task_options(self) -> dict:
        """
        Merge and return the task options.
        """
        return {
            **self._task_options_base,
            **self._task_options_override,
        }

    def has_task_option(self, option_name: str) -> bool:
        """
        Return true if the task has an option with name `option_name`
        """
        return option_name in self._task_options_override or option_name in self._task_options_base

    def __repr__(self) -> str:
        return "Task(fullname={fullname}, hash={hash})".format(
            fullname=self.fullname,
            hash=self.hash[:8],
        )

    def _validate(self) -> None:
        config_args = self.get_task_option("config_args")
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
            if not re.match("^[A-Za-z_][A-Za-z_0-9.]*$", self.namespace):
                raise ValueError(
                    f"Task namespace must use only alphanumeric characters, "
                    f"underscore '_', and dot '.', and may not start with a dot: {self.namespace}"
                )

        if not re.match("^[A-Za-z_][A-Za-z_0-9]*$", self.name):
            raise ValueError(
                f"Task name must use only alphanumeric characters and underscore '_': {self.name}"
            )

        # Validate nout.
        if self.nout is not None:
            if not isinstance(self.nout, int):
                raise TypeError("nout must be an int")
            if self.nout < 0:
                raise TypeError("nout must be non-negative")

        for options_dict in [self._task_options_base, self._task_options_override]:
            # Update the legacy options
            if "cache" in options_dict:
                cache_value = options_dict.pop("cache")
                options_dict["cache_scope"] = CacheScope.BACKEND if cache_value else CacheScope.CSE

            # Sanitize the cache options into enums
            if "cache_scope" in options_dict:
                options_dict["cache_scope"] = CacheScope(options_dict["cache_scope"])

            if "check_valid" in options_dict:
                options_dict["check_valid"] = CacheCheckValid(options_dict["check_valid"])

    @property
    def fullname(self) -> str:
        """
        Returns the fullname of a Task: '{namespace}.{name}'
        """
        return self._format_fullname(self.namespace, self.name)

    @staticmethod
    def _format_fullname(namespace: Optional[str], name: str):
        """Format the fullname of a Task."""
        if namespace:
            return namespace + "." + name
        else:
            return name

    def _call(self: "Task[Callable[..., Result]]", *args: Any, **kwargs: Any) -> Result:
        """
        Returns a lazy Expression of calling the task.
        """
        return cast(
            Result,
            TaskExpression(
                self.fullname,
                args,
                kwargs,
                task_options=self._task_options_override,
                length=self.nout,
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
            **self._task_options_override,
            **task_options_update,
        }
        # Be sure to clone the actual type, in case it's a derived one.
        return self.__class__(
            self.func,
            name=self.name,
            namespace=self.namespace,
            version=self.version,
            compat=self.compat,
            script=self.script,
            source=self.source,
            task_options_base=self._task_options_base,
            task_options_override=new_task_options_update,
        )

    def _calc_hash(self) -> str:
        """The hash is designed for checking equality of `Task`s for the purposes of caching.
        That is, results of this Task may be cached if the hash value is the same, regardless
         of other data members."""
        # TODO: implement for real.
        if self.compat:
            return self.compat[0]

        # Note, we specifically do not hash `_task_options_base` since they are
        # not allowed to impact the results of computation.
        if self._task_options_override:
            task_options_hash = [get_type_registry().get_hash(self._task_options_override)]
        else:
            task_options_hash = []

        if self._hash_includes:
            # Sort to avoid order dependence on the includes.
            hash_includes_hash = sorted(map(get_type_registry().get_hash, self._hash_includes))
        else:
            hash_includes_hash = []

        if self.version is None:
            if self.source:
                source = self.source
            elif self.func:
                source = get_func_source(self.func)
            else:
                source = ""
            return hash_struct(
                ["Task", self.fullname, "source", source] + hash_includes_hash + task_options_hash
            )
        else:
            return hash_struct(
                ["Task", self.fullname, "version", self.version]
                + hash_includes_hash
                + task_options_hash
            )

    def __getstate__(self) -> dict:
        # Note: We specifically don't serialize several items (e.g., func and task_options_base)
        # that are created for us at module load time. These are extracted from the task
        # registry, instead.
        #
        # This needs to remain minimal. See class-level docs before adding anything to this state.
        return {
            "name": self.name,
            "namespace": self.namespace,
            "version": self.version,
            "hash": self.hash,
            "compat": self.compat,
            "script": self.script,
            # This key name is mismatched to avoid a trivial schema update.
            "task_options": self._task_options_override,
        }

    def __setstate__(self, state) -> None:
        self.name = state["name"]
        self.namespace = state["namespace"]
        self.version = state["version"]
        self.hash = state["hash"]
        self.compat = state.get("compat", [])
        self.script = state.get("script", False)
        # This key name is mismatched to avoid a trivial schema update.
        self._task_options_override = state.get("task_options", {})

        # Set func from TaskRegistry.
        registry = get_task_registry()
        _task = registry.get(self.fullname)
        if _task:
            self.func = _task.func
            self._task_options_base = _task._task_options_base
            self.source = _task.source or get_func_source(self.func)
            self._hash_includes = _task._hash_includes
        else:
            self.func = lambda *args, **kwargs: undefined_task(self.fullname, *args, **kwargs)
            self._task_options_base = {}
            self.source = ""
            self._hash_includes = None

        self._signature = None

        self._validate()

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

    @property
    def load_module(self) -> str:
        load_module = self.get_task_option("load_module")
        if load_module:
            return load_module
        else:
            return self.func.__module__


class SchedulerTask(Task[Func]):
    """
    A Task that executes within the scheduler to allow custom evaluation.
    """

    def _call(self: "SchedulerTask[Callable[..., Result]]", *args: Any, **kwargs: Any) -> Result:
        """
        Returns a lazy Expression of calling the task.
        """
        return cast(
            Result,
            SchedulerExpression(
                self.fullname,
                args,
                kwargs,
                task_options=self._task_options_override,
                length=self.nout,
            ),
        )

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


@overload
def task(
    func: Func,
) -> Task[Func]:
    ...


@overload
def task(
    *,
    name: Optional[str] = None,
    namespace: Optional[str] = None,
    version: Optional[str] = None,
    compat: Optional[List[str]] = None,
    script: bool = False,
    hash_includes: Optional[list] = None,
    source: Optional[str] = None,
    **task_options_base: Any,
) -> Callable[[Func], Task[Func]]:
    ...


def task(
    func: Optional[Func] = None,
    *,
    name: Optional[str] = None,
    namespace: Optional[str] = None,
    version: Optional[str] = None,
    compat: Optional[List[str]] = None,
    script: bool = False,
    hash_includes: Optional[list] = None,
    source: Optional[str] = None,
    **task_options_base: Any,
) -> Union[Task[Func], Callable[[Func], Task[Func]]]:
    """
    Decorator to register a function as a redun :class:`Task`.

    Parameters
    ----------
    func : Optional[Func]
        A python function to register as a redun Task. If not given, a
        parameterized decorator is returned.
    name : Optional[str]
        Name of task (Default: infer from function `func.__name__`)
    namespace : Optional[str]
        Namespace of task (Default: Infer from context if possible, else None. See
        `Task._compute_namespace`)
    version : Optional[str]
        Optional manual versioning for a task (Default: source code of task is
        hashed).
    compat : Optional[List[str]]
        Optional redun version compatibility. Not currently implemented.
    script : bool
        If True, this is a script-style task which returns a shell script string.
    hash_includes : Optional[list]
        If provided, extra data that should be hashed. That is, extra data that should be
        considered as part of cache invalidation. This list may be reordered without impacting
        the computation. Each list item must be hashable by `redun.value.TypeRegistry.get_hash`.
    source : Optional[str]
        If provided, task.source will be set to this string. It is the caller's responsibility
        to ensure that `source` matches the provided `func` for proper hashing.
    **task_options_base : Any
        Additional options for configuring a task or specifying behaviors of tasks. Since
        these are provided at task construction time (this is typically at Python module-load
        time), they are the "base" set. Example keys:
            load_module : Optional[str]
                The module to load to import this task. (Default: infer from `func.__module__`)
            wrapped_task : Optional[Task]
                If present, a reference to the task wrapped by this one.
    """

    def deco(func: Func) -> Task[Func]:
        nonlocal namespace

        _task: Task[Func] = Task(
            func,
            name=name,
            namespace=namespace,
            version=version,
            compat=compat,
            script=script,
            task_options_base=task_options_base,
            hash_includes=hash_includes,
            source=source,
        )
        get_task_registry().add(_task)
        return _task

    if func:
        # If this decorator is applied directly to a function, decorate it.
        return deco(func)
    else:
        # If a function is not given, just return the parameterized decorator.
        return deco


def scheduler_task(
    name: Optional[str] = None,
    namespace: Optional[str] = None,
    version: str = "1",
    **task_options_base: Any,
) -> Callable[[Callable[..., Promise[Result]]], SchedulerTask[Callable[..., Result]]]:
    """
    Decorator to register a function as a scheduler task.

    Unlike usual tasks, scheduler tasks are lower-level tasks that are evaluated
    within the :class:`Scheduler` and allow defining custom evaluation semantics.
    For example, one can implement `cond()`, `seq()` and `catch()` using
    scheduler tasks.

    When evaluated, scheduler tasks are called with a reference to the
    :class:`Scheduler`, the parent :class:`Job`, and the full :class:`SchedulerExpression`
    as it's first three arguments.
    It's remaining arguments are the same as those passed from the user, however,
    they are not evaluated and may contain :class:`Expression`s. It is the
    responsibility of the scheduler task to explicitly evaluate arguments
    by using `Scheduler.evaluate()` as needed. Overall, this allows the scheduler
    task to implement custom evaluation semantics. Lastly, the scheduler task
    must return a :class:`Promise` that resolves to the result of the task.

    This concept corresponds to fexpr in Lisp: https://en.wikipedia.org/wiki/Fexpr

    For example, one could implement a lazy if-statement called `cond` using
    this scheduler task:

    .. code-block:: python

        @scheduler_task()
        def cond(scheduler: Scheduler, parent_job: Job, scheduler_expr: SchedulerExpression,
                 pred_expr: Any, then_expr: Any, else_expr: Any) -> Promise:
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

        _task: SchedulerTask[Callable[..., Result]] = SchedulerTask(
            func,
            name=name,
            namespace=namespace,
            version=version,
            task_options_base=task_options_base,
        )
        get_task_registry().add(_task)
        return _task

    return deco


def wraps_task(
    wrapper_name: Optional[str] = None,
    wrapper_hash_includes: list = [],
    **wrapper_task_options_base: Any,
) -> Callable[[Callable[[Task[Func]], Func]], Callable[[Func], Task[Func]]]:
    """A helper for creating new decorators that can be used like `@task`, that allow us to
    wrap the task in another one. Conceptually inspired by `@functools.wraps`, which makes it
    easier to create decorators that enclose functions.

    Specifically, this helps us create a decorator that accepts a task and wraps it. The task
    passed in is moved into an inner namespace, hiding it. Then a new task is created from the
    wrapper implementation that assumes its identity; the wrapper is given access to both the
    run-time arguments and the hidden task definition. Since `Tasks` may be wrapped repeatedly,
    the hiding step is recursive. The `load_module` for all of the layers is dictated by the
    innermost concrete `Task` object.

    A simple usage example is a new decorator `@doubled_task`, which simply runs the original
    task and multiplies by two.

    .. code-block:: python

        def doubled_task() -> Callable[[Func], Task[Func]]:

            # The name of this inner function is used to create the nested namespace,
            # so idiomatically, use the same name as the decorator with a leading underscore.
            @wraps_task()
            def _doubled_task(inner_task: Task) -> Callable[[Task[Func]], Func]:

                # The behavior when the task is run. Note we have both the task and the
                # runtime args.
                def do_doubling(*task_args, **task_kwargs) -> Any:
                    return 2 * inner_task.func(*task_args, **task_kwargs)

                return do_doubling
            return _doubled_task

        # The simplest use is to wrap a task that is already created
        @doubled_task()
        @task()
        def value_task1(x: int):
            return 1 + x

        # We can skip the inner decorator and the task will get created implicitly
        # Use the explicit form if you need to pass arguments to task creation.
        @doubled_task()
        def value_task2(x: int):
            return 1 + x

        # We can keep going
        @doubled_task()
        @doubled_task()
        def value_task3(x: int):
            return 1 + x

    There is an additional subtlety if the wrapper itself accepts arguments. These must be passed
    along to the wrapper so they are visible to the scheduler. Needing to do this manually is
    the cost of the extra powers we have.

    .. code-block:: python

        # An example of arguments consumed by the wrapper
        def wrapper_with_args(wrapper_arg: int) -> Callable[[Func], Task[Func]]:

            # WARNING: Be sure to pass the extra data for hashing so it participates in the cache
            # evaluation
            @wraps_task(wrapper_hash_includes=[wrapper_arg])
            def _wrapper_with_args(inner_task: Task) -> Callable[[Task[Func]], Func]:

                def do_wrapper_with_args(*task_args, **task_kwargs) -> Any:
                    return wrapper_arg * inner_task.func(*task_args, **task_kwargs)

                return do_wrapper_with_args
            return _wrapper_with_args

    Parameters
    ----------
    wrapper_name : Optional[str]
        The name of the wrapper, which is used to create the inner namespace. (Default: infer
        from the wrapper `wrapper_func.__name__`)
    wrapper_task_options_base : Any
        Additional options for the wrapper task.
    wrapper_hash_includes : Optional[list]
        If provided, extra data that should be hashed. That is, extra data that should be
        considered as part of cache invalidation. This list may be reordered without impacting
        the computation. Each list item must be hashable by `redun.value.TypeRegistry.get_hash`
    """

    def transform_wrapper(wrapper_func: Callable[[Task], Func]) -> Callable[[Func], Task[Func]]:
        def create_tasks(inner_func_or_task: Union[Func, Task[Func]]) -> Task[Func]:

            # As a convenience, create the lowest level Task on the fly.
            if isinstance(inner_func_or_task, Task):
                hidden_inner_task = inner_func_or_task
            else:
                hidden_inner_task = task()(inner_func_or_task)

            visible_name = hidden_inner_task.name
            visible_namespace = hidden_inner_task.namespace

            def recursive_rename(task_: Task, suffix: str) -> str:
                # Recurse first, so we rename the innermost first, making room for renames
                # higher up the chain.
                if task_.get_task_option("wrapped_task", None) is not None:
                    task_._task_options_base["wrapped_task"] = recursive_rename(
                        get_task_registry().get(task_name=task_.get_task_option("wrapped_task")),
                        suffix,
                    )

                old_fullname = task_.fullname
                if task_.namespace != "":
                    new_namespace = f"{task_.namespace}.{suffix}"
                else:
                    new_namespace = suffix

                new_task = get_task_registry().rename(
                    old_fullname, new_name=task_.name, new_namespace=new_namespace
                )
                return new_task.fullname

            nonlocal wrapper_name
            if not wrapper_name:
                wrapper_name = wrapper_func.__name__

            # *Before* we create the new task, hide the old one
            recursive_rename(hidden_inner_task, wrapper_name)

            # Gather data to propagate the implementation of our children, so that cache
            # invalidation propagates. We don't know how wrapper functions will use the tasks,
            # so take the conservative approach and assume that any change invalidates.
            # For example, may call `func` directly, which would put a whole subtree of evaluation
            # out of view of the scheduler.
            #
            # Technically we don't need the name, since the hash has that already, but it's a lot
            # easier to understand this way.
            wrapped_hash_data = [hidden_inner_task]

            # We're definitely getting a task back
            wrapped_task: Task[Func] = task(
                name=visible_name,
                namespace=visible_namespace,
                wrapped_task=hidden_inner_task.fullname,
                load_module=hidden_inner_task.load_module,
                hash_includes=wrapper_hash_includes + wrapped_hash_data,
                **wrapper_task_options_base,
            )(wrapper_func(hidden_inner_task))

            return wrapped_task

        return create_tasks

    return transform_wrapper


class TaskRegistry:
    """
    A registry of currently registered Tasks.
    The @task() decorator registers tasks to the current registry.
    """

    def __init__(self):
        self._tasks: Dict[str, Task] = {}

        self.task_hashes = set()

    def add(self, task: Task) -> None:
        self._tasks[task.fullname] = task
        self.task_hashes.add(task.hash)

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

    def rename(self, old_name: str, new_namespace: str, new_name: str) -> Task:
        assert old_name in self._tasks
        task = self._tasks.pop(old_name)
        self.task_hashes.remove(task.hash)

        task.namespace = new_namespace
        task.name = new_name
        self.add(task)
        return task

    def __iter__(self) -> Iterable[Task]:
        return iter(self._tasks.values())


# Global singleton task registry.
_task_registry = TaskRegistry()


def hash_args_eval(
    type_registry: TypeRegistry, task: Task, args: tuple, kwargs: dict
) -> Tuple[str, str]:
    """
    Compute eval_hash and args_hash for a task call, filtering out config args before hashing.
    """
    # Filter out config args from args and kwargs.
    sig = task.signature
    config_args: List = task.get_task_option("config_args", [])

    # Determine the variadic parameter if it exists.
    var_param_name: typing.Optional[str] = None
    for param in sig.parameters.values():
        if param.kind == inspect.Parameter.VAR_POSITIONAL:
            var_param_name = param.name
            break

    # Filter args to remove config_args.
    args2 = [
        arg_value
        for arg_name, arg_value in zip(sig.parameters, args)
        if arg_name not in config_args
    ]

    # Additional arguments are assumed to be variadic arguments.
    args2.extend(
        arg_value for arg_value in args[len(sig.parameters) :] if var_param_name not in config_args
    )

    # Filter kwargs.
    kwargs2 = {
        arg_name: arg_value
        for arg_name, arg_value in kwargs.items()
        if arg_name not in config_args
    }

    return hash_eval(type_registry, task.hash, args2, kwargs2)
