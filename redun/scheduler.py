import datetime
import importlib
import inspect
import linecache
import logging
import os
import pprint
import queue
import shlex
import sys
import threading
import time
import traceback
import uuid
from collections import OrderedDict, defaultdict
from itertools import chain, takewhile
from traceback import FrameSummary
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
    overload,
)

from redun.backends.base import RedunBackend, TagEntity
from redun.backends.db import RedunBackendDb
from redun.config import Config, Section, SectionProxy, create_config_section
from redun.executors.base import Executor, get_executors_from_config
from redun.executors.local import LocalExecutor
from redun.expression import (
    AnyExpression,
    ApplyExpression,
    Expression,
    QuotedExpression,
    SchedulerExpression,
    SimpleExpression,
    TaskExpression,
    ValueExpression,
    derive_expression,
    get_lazy_operation,
    quote,
)
from redun.file import Dir
from redun.handle import Handle
from redun.hashing import hash_struct
from redun.logging import logger as _logger
from redun.promise import Promise, wait_promises
from redun.scheduler_config import REDUN_INI_FILE, postprocess_config
from redun.tags import parse_tag_value
from redun.task import (
    CacheCheckValid,
    CacheResult,
    CacheScope,
    Task,
    TaskRegistry,
    get_task_registry,
    hash_args_eval,
    scheduler_task,
    task,
)
from redun.utils import format_table, iter_nested_value, map_nested_value, str2bool, trim_string
from redun.value import Value, get_type_registry

# Globals.
_local = threading.local()

# Constants.
JOB_ACTION_WIDTH = 6  # Width of job action in logs.

Result = TypeVar("Result")
T = TypeVar("T")
S = TypeVar("S")


def settrace_patch(tracefunc: Any) -> None:
    """
    Monkey patch for recording whether debugger REPL is active or not.

    sys.settrace() is called by debuggers such as pdb whenever the debugger
    REPL is activated. We use a monkey patch in order to record tracing
    process-wide. Relying on `sys.gettrace()` is not enough because it is
    thread-specific.
    """
    global _is_debugger_active
    _is_debugger_active = bool(tracefunc)
    try:
        _original_settrace(tracefunc)
    except Exception:
        # IDEs, such as PyCharm, may ban calls to settrace().
        # http://pydev.blogspot.com/2007/06/why-cant-pydev-debugger-work-with.html
        # In such cases, do nothing.
        pass


def is_debugger_active() -> bool:
    """
    Returns True if debugger REPL is currently active.
    """
    global _is_debugger_active
    return _is_debugger_active


# Patch sys.settrace() in order to detect presence of debugger.
_original_settrace = sys.settrace
_is_debugger_active = False
sys.settrace = settrace_patch


class NoCurrentScheduler(Exception):
    def __init__(self):
        super().__init__("Scheduler is not running.")


class SchedulerError(Exception):
    pass


class DryRunResult(Exception):
    pass


def get_current_scheduler(required=True) -> Optional["Scheduler"]:
    """
    Returns the currently running Scheduler for this thread.
    """
    if not hasattr(_local, "_redun_scheduler"):
        if required:
            raise NoCurrentScheduler()
        else:
            return None
    return _local._redun_scheduler


def set_current_scheduler(scheduler: Optional["Scheduler"]) -> None:
    """
    Sets the current running Scheduler for this thread.
    """
    if scheduler:
        _local._redun_scheduler = scheduler
    else:
        try:
            del _local._redun_scheduler
        except AttributeError:
            pass


def set_arg_defaults(task: "Task", args: Tuple, kwargs: dict) -> Tuple[Tuple, dict]:
    """
    Set default arguments from Task signature.
    """
    # Start with given kwargs.
    kwargs2 = dict(kwargs)

    sig = task.signature
    for i, param in enumerate(sig.parameters.values()):
        if i < len(args):
            # User already specified this arg in args.
            continue

        elif param.name in kwargs2:
            # User already specified this arg in kwargs.
            continue

        elif param.default is not param.empty:
            # Default should be used.
            kwargs2[param.name] = param.default
    return args, kwargs2


def format_arg(arg_name: str, value: Any, max_length: int = 200) -> str:
    """
    Format a Task argument into a string.
    """
    return "{arg_name}={value}".format(
        arg_name=arg_name, value=trim_string(repr(value), max_length=max_length)
    )


def format_task_call(task: "Task", args: Tuple, kwargs: dict) -> str:
    """
    Format a Task call into a string.

    ```
    my_task(arg1=10, my_file=File('path/to/file.txt')
    ```
    """
    all_args = OrderedDict()
    sig = task.signature

    for i, param in enumerate(sig.parameters.values()):
        if i < len(args):
            # Positional argument.
            all_args[param.name] = args[i]

        else:
            # Keyword argument.
            all_args[param.name] = kwargs.get(param.name, param.default)

    args_text = ", ".join(format_arg(arg_name, value) for arg_name, value in all_args.items())
    return "{task}({args})".format(
        task=task.fullname,
        args=args_text,
    )


class Execution:
    """
    An Execution tracks the execution of a workflow of :class:`redun.task.Task`s.
    """

    def __init__(self, id: Optional[str] = None):
        self.id = id or str(uuid.uuid4())
        self.job: Optional[Job] = None

    def add_job(self, job: "Job") -> None:
        # Record first job as root job.
        if not self.job:
            self.job = job


class Job:
    """
    A Job tracks the execution of a :class:`redun.task.Task` through its various stages.
    """

    STATUSES = ["PENDING", "RUNNING", "FAILED", "CACHED", "DONE", "TOTAL"]

    def __init__(
        self,
        task: Task,
        expr: TaskExpression,
        id: Optional[str] = None,
        parent_job: Optional["Job"] = None,
        execution: Optional[Execution] = None,
    ):
        self.id = id or str(uuid.uuid4())

        # The Task used in expr.
        self.task = task

        # TaskExpression for which this job was created to evaluate.
        self.expr = expr

        # Execution representing the entire running workflow.
        self.execution: Optional[Execution] = execution

        # Execution state.

        # Job-level task option overrides.
        self.task_options: Dict[str, Any] = {}

        # The evaluated version of (self.expr.args, self.expr.kwargs)
        self.eval_args: Optional[Tuple[Tuple, Dict]] = None

        # The evaluated and preprocessed arguments.
        # These are the arguments (args, kwargs) submitted to executors.
        self.args: Optional[Tuple[Tuple, Dict]] = None

        # The hash of the evaluated and preprocessed arguments.
        self.args_hash: Optional[str] = None

        # The hash of (self.task.hash, self.arg_hash). This is used as the cache key.
        self.eval_hash: Optional[str] = None

        # This is true if we fetched the result from the cache.
        self.was_cached: bool = False

        # Hash of the CallNode associated with running this job. This hash requires knowledge
        # of the Job's result, hence is available after either computing or retrieving the result.
        self.call_hash: Optional[str] = None

        # Promise for evaluating self.expr.
        self.result_promise: Promise = Promise()

        # The child jobs created for task calls within self.expr.
        self.child_jobs: List[Job] = []

        # The parent job or None if this is the root job of the execution.
        self.parent_job: Optional[Job] = parent_job

        # Bookkeeping for handles created during this job.
        self.handle_forks: Dict[str, int] = defaultdict(int)

        # Tags (key-value pairs) that should be added to this job.
        self.job_tags: List[Tuple[str, Any]] = []

        # Tags (key-value pairs) that should be added to the execution.
        self.execution_tags: List[Tuple[str, Any]] = []

        # Tags (value_hash and key-value pairs) that should be added to values.
        self.value_tags: List[Tuple[str, List[Tuple[str, Any]]]] = []

        # A cached status of the job (PENDING, RUNNING, CACHED, DONE, FAILED).
        self._status: Optional[str] = None

        if parent_job:
            self.add_parent(parent_job)
        if execution:
            execution.add_job(self)

    def __repr__(self) -> str:
        return f"Job(id={self.id}, task_name={self.task.fullname})"

    @property
    def status(self) -> str:
        if self._status:
            return self._status
        elif self.eval_args is None:
            return "PENDING"
        elif self.result_promise.is_pending:
            return "RUNNING"
        elif self.result_promise.is_fulfilled:
            if self.was_cached:
                return "CACHED"
            else:
                return "DONE"
        elif self.result_promise.is_rejected:
            return "FAILED"
        else:
            raise ValueError("Unknown status")

    def get_option(self, key: str, default: Any = None, as_type: Optional[Type] = None) -> Any:
        """
        Returns a task option associated with a :class:`Job`.

        Precedence is given to task options defined at call-time
        (e.g. `task.options(option=foo)(arg1, arg2)`) over task definition-time
        (e.g. `@task(option=foo)`).
        """
        assert "task_expr_options" in self.expr.__dict__
        task = cast(Task, self.task)

        def get_raw():
            if key in self.task_options:
                return self.task_options[key]
            elif key in self.expr.task_expr_options:
                return self.expr.task_expr_options[key]
            elif task.has_task_option(key):
                return task.get_task_option(key)
            else:
                return default

        result = get_raw()
        return as_type(result) if as_type else result

    def get_options(self) -> dict:
        """
        Returns task options for this job.

        Precedence is given to task options defined at call-time
        (e.g. `task.options(option=foo)(arg1, arg2)`) over task definition-time
        (e.g. `@task(option=foo)`).
        """
        assert self.task
        task_options = {
            **self.task.get_task_options(),
            **self.expr.task_expr_options,
            **self.task_options,
        }
        return task_options

    def get_limits(self) -> Dict[str, int]:
        """
        Returns resource limits required for this job to run.
        """
        limits = self.get_option("limits", {}) if self.task else {}

        # We allow limits to be a list of resources. In that case, we default the required
        # resource count to 1 for each resource specified. We create the mapping of limit to
        # count here so that we always have a dict when constructing job_limits below.
        if isinstance(limits, list):
            limits = {limit_name: 1 for limit_name in limits}

        assert isinstance(limits, dict)

        job_limits: Dict[str, int] = defaultdict(int)
        job_limits.update(limits)
        return job_limits

    def add_parent(self, parent_job: "Job") -> None:
        """
        Maintains the Job tree but connecting the job with a parent job.
        """
        parent_job.child_jobs.append(self)

    def collapse(self, other_job: "Job") -> None:
        """
        Collapse this Job into `other_job`.

        This method is used when equivalent Jobs are detected, and we want to
        perform Common Subexpression Elimination (CSE).
        """

        # Inform parent job, to use other_job instead.
        parent_job = self.parent_job
        assert parent_job
        parent_job.child_jobs[parent_job.child_jobs.index(self)] = other_job

        # Make callbacks just as if we had gotten a cache hit.
        def then(result: Any) -> None:
            self.call_hash = other_job.call_hash
            scheduler = get_current_scheduler()
            assert scheduler
            self.was_cached = True

            scheduler.done_job(self, result)

        def fail(error: Any) -> None:
            self.call_hash = other_job.call_hash
            scheduler = get_current_scheduler()
            assert scheduler
            assert other_job.eval_args
            self.was_cached = True

            # It's important to use the main thread directly, because if we queue up the
            # event, the scheduler may shut down before processing it. This way, we can ensure
            # all the bookkeeping gets done synchronously.
            scheduler._reject_job_main_thread(self, error)

        other_job.result_promise.then(then, fail)

    def resolve(self, result: Any) -> None:
        """
        Resolves a Job with a final concrete value, `result`.
        """
        self.expr.call_hash = self.call_hash
        self.result_promise.do_resolve(result)
        self.clear()

    def reject(self, error: Any) -> None:
        """
        Rejects a Job with an error exception.
        """
        self.expr.call_hash = self.call_hash
        self.result_promise.do_reject(error)
        self.clear()

    def clear(self):
        """
        Free execution state from Job.
        """
        # Record final status before clearing execution state.
        self._status = self.status

        # Implementation note: we need to do this because the job holds
        # both parent and child jobs, creating a reference cycle that would
        # prevent garbage collection. Here, we break that cycle.
        self.expr = None
        self.eval_args = None
        self.args = None
        self.result_promise = None
        self.job_tags.clear()
        self.execution_tags.clear()
        self.value_tags.clear()
        self.child_jobs.clear()


def get_backend_from_config(backend_config: Optional[SectionProxy] = None) -> RedunBackend:
    """
    Parses a redun :class:`redun.backends.base.RedunBackend` from a config object.
    """
    if not backend_config:
        backend_config = create_config_section()
    backend_config = cast(SectionProxy, backend_config)
    if not backend_config.get("db_uri"):
        # By default, use in-memory db and autoload (create schemas).
        backend_config["db_uri"] = "sqlite:///:memory:"
        load = True
    else:
        load = False

    backend = RedunBackendDb(config=backend_config)
    if load:
        backend.load()
    return backend


def get_limits_from_config(limits_config: Optional[Section] = None) -> Dict[str, int]:
    """
    Parses resource limits from a config object.
    """
    return (
        {key: int(value) for key, value in cast(dict, limits_config).items()}
        if limits_config
        else {}
    )


def get_ignore_warnings_from_config(scheduler_config: Section) -> Set[str]:
    """
    Parses ignore warnings from config.
    """
    warnings_text = scheduler_config.get("ignore_warnings")
    if not warnings_text:
        return set()
    return set(warnings_text.strip().split())


def format_job_statuses(
    job_status_counts: Dict[str, Dict[str, int]],
    timestamp: Optional[datetime.datetime] = None,
) -> Iterator[str]:
    """
    Format job status table (Dict[task_name, Dict[status, count]]).
    """
    # Create counts table.
    task_names = sorted(job_status_counts.keys())
    table: List[List[str]] = (
        [["TASK"] + Job.STATUSES]
        + [
            ["ALL"]
            + [
                str(sum(job_status_counts[task_name][status] for task_name in task_names))
                for status in Job.STATUSES
            ]
        ]
        + [
            [task_name] + [str(job_status_counts[task_name][status]) for status in Job.STATUSES]
            for task_name in task_names
        ]
    )

    # Display job status table.
    if timestamp is None:
        timestamp = datetime.datetime.now()
    yield "| JOB STATUS {}".format(timestamp.strftime("%Y/%m/%d %H:%M:%S"))

    for line in format_table(table, "lrrrrrr", min_width=7):
        yield f"| {line}"
    yield ""


class Frame(FrameSummary, Value):
    """
    Frame of a :class:`Traceback` for :class:`Job` failure.
    """

    type_name = "redun.Frame"

    def __init__(
        self,
        filename: str,
        lineno: int,
        name: str,
        locals: Dict[str, Any],
        lookup_line: bool = True,
        line: Optional[str] = None,
        job: Optional[Job] = None,
    ):
        self.filename = filename
        self.lineno = lineno
        self.name = name
        self._line = line
        if lookup_line:
            self.line
        self.locals = {key: trim_string(repr(value)) for key, value in locals.items()}
        assert job
        self.job: Job = job

        # Advance past decorator.
        if self._line and self._line.strip().startswith("@"):
            while True:
                self.lineno += 1
                line = linecache.getline(self.filename, self.lineno).strip()
                if line.startswith("def "):
                    break
            self._line = None

    def __getstate__(self) -> dict:
        return {
            "filename": self.filename,
            "lineno": self.lineno,
            "name": self.name,
            "_line": self._line,
            "locals": self.locals,
            "job": self.job,
        }

    def __setstate__(self, state: dict) -> None:
        self.filename = state["filename"]
        self.lineno = state["lineno"]
        self.name = state["name"]
        self._line = state["_line"]
        self.locals = state["locals"]
        self.job = state["job"]


class Traceback(Value):
    """
    Traceback for :class:`Job` failure.
    """

    type_name = "redun.Traceback"

    def __init__(self, error: Any, frames: List[FrameSummary], logs: Optional[List[str]] = None):
        self.error: Any = error
        self.frames = frames
        self.logs: List[str] = logs or []

    def __getstate__(self) -> dict:
        return {
            "error": self.error,
            "frames": self.frames,
            "logs": self.logs,
        }

    def __setstate__(self, state: dict) -> None:
        self.error = state["error"]
        self.frames = state["frames"]
        self.logs = state["logs"]

    @classmethod
    def from_error(self, error: Exception, trim_frames=True) -> "Traceback":
        """
        Returns a new :class:`Traceback` derived from an Exception.

        Parameters
        ----------
        error : Exception
            Exception object from which to derive a Traceback.
        trim_frames : bool
            If True, do not include redun scheduler related frames in Traceback.
        """
        frames = list(traceback.extract_tb(error.__traceback__))
        if trim_frames:
            frames = self.trim_frames(frames)
        return Traceback(error, frames)

    @classmethod
    def trim_frames(self, frames: List[FrameSummary]) -> List[FrameSummary]:
        # As we walk back through the call stack, when we reach a frame from
        # redun's executors or cli, we know we have left the user's code.
        redun_path_prefixes = [
            os.path.join(os.path.dirname(__file__), "executors"),
            os.path.join(os.path.dirname(__file__), "cli.py"),
        ]

        return list(
            reversed(
                list(
                    takewhile(
                        lambda frame: not any(
                            frame.filename.startswith(prefix) for prefix in redun_path_prefixes
                        ),
                        reversed(frames),
                    )
                )
            )
        )

    def format(self) -> Iterator[str]:
        """
        Iterates through lines displaying the :class:`Traceback`.
        """
        for frame in self.frames:
            if isinstance(frame, Frame):
                yield '  Job {job}: File "{file}", line {lineno}, in {task}\n'.format(
                    job=frame.job.id[:8],
                    file=frame.filename,
                    lineno=frame.lineno,
                    task=frame.name,
                )
            else:
                yield '  File "{file}", line {lineno}, in {func}\n'.format(
                    file=frame.filename,
                    lineno=frame.lineno,
                    func=frame.name,
                )
            yield "    {line}\n".format(line=frame.line)
            if frame.locals:
                var_width = max(map(len, frame.locals.keys()))
                for key, value in sorted(frame.locals.items()):
                    yield "    {key} = {value}\n".format(key=key.ljust(var_width), value=value)

        yield "{}: {}".format(type(self.error).__name__, self.error)

        if self.logs:
            yield "\n"
            yield "Latest logs:\n"
            for line in self.logs:
                yield line


class ErrorValue(Value):
    """
    Value for wrapping Exceptions raised by Task.
    """

    type_name = "redun.ErrorValue"

    def __init__(self, error: Exception, traceback: Optional[Traceback] = None):
        assert isinstance(error, Exception)
        self.error = error
        self.traceback = traceback

    def __repr__(self) -> str:
        return "ErrorValue({})".format(repr(self.error))

    def get_hash(self, data: Optional[bytes] = None) -> str:
        registry = get_type_registry()
        return hash_struct(
            ["redun.ErrorValue", registry.get_hash(self.error), registry.get_hash(self.traceback)]
        )

    def __getstate__(self) -> dict:
        return {"error": self.error, "traceback": self.traceback}

    def __setstate__(self, state: dict) -> None:
        self.error = state["error"]
        self.traceback = state["traceback"]


@task(name="root_task", namespace="redun")
def root_task(expr: QuotedExpression[Result]) -> Result:
    """
    Default task used for a root job in an Execution.
    """
    return expr.eval()


def needs_root_task(task_registry: TaskRegistry, expr: Any) -> bool:
    """
    Returns True if expression `expr` needs to be wrapped in a root task.
    """
    # A root expr must be a TaskExpression.
    if not isinstance(expr, TaskExpression) or isinstance(expr, SchedulerExpression):
        return True

    # All arguments must be concrete.
    task = task_registry.get(expr.task_name)

    assert (
        task
    ), f"Could not find task `{expr.task_name}`, found options {list(task_registry._tasks.keys())}"

    args, kwargs = set_arg_defaults(task, expr.args, expr.kwargs)
    return any(isinstance(arg, Expression) for arg in iter_nested_value((args, kwargs)))


class Scheduler:
    """
    Scheduler for evaluating redun tasks.

    A thread may only have a single running scheduler at a time, and a scheduler can perform
    exactly one execution at a time. While running, the scheduler will register itself into a
    thread-local variable, see `get_current_scheduler` and `set_current_scheduler`.

    A scheduler may be reused for multiple executions, and makes every effort to be stateless
    between executions. That is, the scheduler clears its internal state before starting a new
    execution, providing isolation from any others we may have performed.

    Although the scheduler collaborates with executors that may use multiple threads during
    execution, the scheduler logic relies upon being executed from a single thread. Therefore,
    the main lifecycle methods used by executors defer back to the scheduler thread.
    The scheduler is implemented around an event loop and asynchronous callbacks, allowing it to
    coordinate many in-flight jobs in parallel.

    Scheduler tasks are generally considered "friends" of the scheduler and may need to access
    some private methods.
    """

    def __init__(
        self,
        config: Optional[Config] = None,
        backend: Optional[RedunBackend] = None,
        executor: Optional[Executor] = None,
        logger: Optional[Any] = None,
        use_task_traceback: bool = True,
        job_status_interval: Optional[int] = None,
        migrate: Optional[bool] = None,
    ):
        self.config = config or Config()
        self.logger = logger or _logger
        self.ignore_warnings = get_ignore_warnings_from_config(self.config.get("scheduler", {}))
        self.use_task_traceback = use_task_traceback
        self.traceback: Optional[Traceback] = None
        self.job_status_interval: Optional[int] = job_status_interval or int(
            self.config.get("scheduler", {}).get("job_status_interval", 20)
        )
        if self.job_status_interval <= 0:
            self.job_status_interval = None

        # Setup backend.
        if backend is None:
            backend = get_backend_from_config(self.config.get("backend"))

        self.backend: RedunBackend = backend

        # Setup executors.
        self.executors: Dict[str, Executor] = {}
        if executor:
            self.add_executor(executor)
        else:
            # Add default executors.
            self.add_executor(LocalExecutor("default", mode="thread"))
            self.add_executor(LocalExecutor("process", mode="process"))
        for executor in get_executors_from_config(self.config.get("executors", {})):
            self.add_executor(executor)

        # Setup limits.
        self.limits: Dict[str, int] = get_limits_from_config(self.config.get("limits"))
        self.limits_used: Dict[str, int] = defaultdict(int)
        self._jobs_pending_limits: List[Tuple[Job, Tuple[tuple, dict]]] = []

        # Setup execution tags.
        tags = self.config.get("tags", {})
        self._exec_tags = [(key, parse_tag_value(value)) for key, value in tags.items()]

        # Registries (aka environment).
        self.task_registry = get_task_registry()
        self.type_registry = get_type_registry()

        # Scheduler state.
        self.thread_id: Optional[int] = None
        self.events_queue: queue.Queue = queue.Queue()
        self.workflow_promise: Optional[Promise] = None
        self._current_execution: Optional[Execution] = None

        # Tracking for expression cycles. We store pending expressions by their parent_job
        # because the parent_job's lifetime corresponds to the expression's lifetime.
        # This makes for straightforward deallocation.
        self._pending_expr: Dict[
            Optional[Job], Dict[str, Tuple[Promise, Expression]]
        ] = defaultdict(dict)

        # Tracking for Common Subexpression Elimination (CSE) for pending Jobs.
        self._pending_jobs: Dict[Optional[str], Job] = {}

        # Currently pending/running jobs.
        self._jobs: Set[Job] = set()

        # Job status of finalized jobs.
        self._finalized_jobs: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))

        # Execution modes.
        self._dryrun = False
        self._use_cache = True

    @property
    def is_running(self) -> bool:
        return self._current_execution is not None

    def clear(self):
        """Release resources"""
        self._pending_expr.clear()
        self._pending_jobs.clear()
        self._jobs.clear()
        self._finalized_jobs.clear()

    def add_executor(self, executor: Executor) -> None:
        """
        Add executor to scheduler.
        """
        self.executors[executor.name] = executor
        executor.set_scheduler(self)

    def load(self, migrate: Optional[bool] = None) -> None:
        self.backend.load(migrate=migrate)

    def log(
        self, *messages: Any, indent: int = 0, multiline: bool = False, level: int = logging.INFO
    ) -> None:
        text = " ".join(map(str, messages))
        if not multiline:
            lines = text.split("\n")
        else:
            lines = [text]
        for line in lines:
            self.logger.log(level, (" " * indent) + line)

    def _validate_tasks(self) -> None:
        """
        Validate that tasks are properly configured.
        """
        invalid_tasks = [task for task in self.task_registry if not task.namespace]
        if invalid_tasks and "namespace" not in self.ignore_warnings:
            task_names = [
                "{}:{}".format(
                    os.path.basename(task.func.__code__.co_filename), task.func.__name__
                )
                for task in invalid_tasks
            ]
            self.logger.warning(
                "Tasks will require namespace soon. Either set namespace in the `@task` decorator "
                "or with the module-level variable `redun_namespace`.\n"
                "tasks needing namespace: {}".format(", ".join(task_names))
            )

    def _run(
        self,
        expr: TaskExpression[Result],
        parent_job: Optional[Job] = None,
        dryrun: bool = False,
        cache: bool = True,
    ) -> Promise[Result]:
        """
        Run the scheduler to evaluate an Expression `expr`.

        This is a private method that returns the original workflow Promise.
        """
        # Set scheduler and start executors.

        try:
            set_current_scheduler(self)
            self.clear()

            for executor in self.executors.values():
                executor.start()
            self._dryrun = dryrun
            self._use_cache = cache
            self.traceback = None

            self._validate_tasks()

            # Start event loop to evaluate expression.
            start_time = time.time()
            self.thread_id = threading.get_ident()
            result = self.evaluate(expr, parent_job=parent_job)
            self._process_events(result)

            # Log execution duration.
            duration = time.time() - start_time
            self.log(f"Execution duration: {duration:.2f} seconds")
        except Exception:
            raise
        finally:
            # Stop executors and unset scheduler.
            for executor in self.executors.values():
                executor.stop()

            self._current_execution = None
            set_current_scheduler(None)

        return result

    @overload
    def run(
        self,
        expr: Expression[Result],
        exec_argv: Optional[List[str]] = None,
        dryrun: bool = False,
        cache: bool = True,
        tags: Iterable[Tuple[str, Any]] = (),
        execution_id: Optional[str] = None,
    ) -> Result:
        pass

    @overload
    def run(
        self,
        expr: Result,
        exec_argv: Optional[List[str]] = None,
        dryrun: bool = False,
        cache: bool = True,
        tags: Iterable[Tuple[str, Any]] = (),
        execution_id: Optional[str] = None,
    ) -> Result:
        pass

    def run(
        self,
        expr: Union[Expression[Result], Result],
        exec_argv: Optional[List[str]] = None,
        dryrun: bool = False,
        cache: bool = True,
        tags: Iterable[Tuple[str, Any]] = (),
        execution_id: Optional[str] = None,
    ) -> Result:
        """
        Run the scheduler to evaluate an Expression `expr`.

        This is the primary user entry point to the scheduler. The scheduler will register
        itself as this thread's running scheduler for the duration of the execution.
        This function blocks, running the event loop until the result is ready and can be returned.

        The scheduler can only run one execution at a time.
        """
        if needs_root_task(self.task_registry, expr):
            # Ensure we always have one root-level job encompassing the whole execution.
            expr = root_task(quote(expr))

        # Initialize execution.
        parent_job: Optional[Job]
        if exec_argv is None:
            exec_argv = ["scheduler.run", trim_string(repr(expr))]
        self._current_execution = Execution(execution_id)
        self.backend.record_execution(self._current_execution.id, exec_argv)
        self.backend.record_tags(
            TagEntity.Execution, self._current_execution.id, chain(self._exec_tags, tags)
        )

        self.log(
            "Start Execution {exec_id}:  redun {argv}".format(
                exec_id=self._current_execution.id,
                argv=" ".join(map(shlex.quote, exec_argv[1:])),
            )
        )

        result: Promise[Result] = self._run(
            cast(TaskExpression[Result], expr), dryrun=dryrun, cache=cache
        )

        # Return or raise result depending on whether it succeeded.
        if result.is_fulfilled:
            return result.value

        elif result.is_rejected:
            if self.traceback:
                # Log traceback.
                self.log("*** Execution failed. Traceback (most recent task last):")
                for line in self.traceback.format():
                    self.log(line.rstrip("\n"))

            raise result.error

        elif result.is_pending and self._dryrun:
            self.log("Dryrun: Additional jobs would run.")
            raise DryRunResult()

        else:
            raise AssertionError("Unexpected state")

    def extend_run(
        self,
        expr: Union[Expression[Result], Result],
        parent_job_id: str,
        dryrun: bool = False,
        cache: bool = True,
    ) -> dict:
        """
        Extend an existing scheduler execution (run) to evaluate a Task or Expression.

        This is an alternative to the `run` method, and acts as a primary user entry point.
        """
        if needs_root_task(self.task_registry, expr):
            # Ensure we always have one root-level job encompassing the whole execution.
            expr = root_task(quote(expr))

        # Extend an existing execution.
        parent_job_details = self.backend.get_job(parent_job_id)
        if not parent_job_details:
            raise ValueError(f"Unknown parent_job_id: {parent_job_id}")
        self._current_execution = Execution(parent_job_details["execution_id"])
        # Create dummy parent_job to mimic job in the parent scheduler:
        # - We need to set its id to match the given parent_job_id.
        # - We set the same execution id as the original parent_job.
        # - Any expression will do, so we use a minimal root task.
        # - We also mimic the task and eval_args being set like they are in _exec_job().
        parent_job = Job(
            root_task,
            root_task(quote(None)),  # type: ignore
            id=parent_job_id,
            execution=self._current_execution,
        )
        parent_job.eval_args = ((quote(None),), {})

        result: Promise[Result] = self._run(
            cast(TaskExpression[Result], expr), parent_job=parent_job, dryrun=dryrun, cache=cache
        )

        # There will always be one new job id for this subexecution.
        [job] = parent_job.child_jobs

        # Return the result along with the job id to allow caller to stitch the job tree.
        if result.is_fulfilled:
            return {
                "result": result.value,
                "job_id": job.id,
                "call_hash": job.call_hash,
            }
        elif result.is_rejected:
            return {
                "error": result.error,
                "job_id": job.id,
                "call_hash": job.call_hash,
            }
        elif result.is_pending and self._dryrun:
            # Return an incomplete run due to dryrun being active.
            return {"dryrun": True, "job_id": job.id, "call_hash": job.call_hash}
        else:
            raise AssertionError("Unexpected state")

    def _process_events(self, workflow_promise: Promise) -> None:
        """
        Main scheduler event loop for evaluating the current expression. Loop over events,
        blocking until the provided `workflow_promise` is resolved.
        """
        self.workflow_promise = workflow_promise

        while self.workflow_promise.is_pending:
            if self._dryrun and self.events_queue.empty():
                # We have exhausted the completed part of the workflow.
                break

            try:
                event_func = self.events_queue.get(timeout=self.job_status_interval)
                event_func()
            except KeyboardInterrupt:
                self.log("Shutting down... Ctrl+C again to force shutdown.")
                sys.exit(1)
            except queue.Empty:
                if not is_debugger_active():
                    # Print job statuses periodically.
                    # For convenience, do not print statuses if the debugger is
                    # active since they interfere with the debugger REPL.
                    self.log_job_statuses()

        # Print final job statuses.
        self.log_job_statuses()

    def get_job_status_report(self) -> List[str]:
        # Gather job status information.
        status_counts: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
        for job in self._jobs:
            status_counts[job.task.fullname][job.status] += 1
            status_counts[job.task.fullname]["TOTAL"] += 1
        for task_name, task_status_counts in self._finalized_jobs.items():
            for status, count in task_status_counts.items():
                status_counts[task_name][status] += count
                status_counts[task_name]["TOTAL"] += count

        return list(format_job_statuses(status_counts))

    def log_job_statuses(self) -> None:
        """
        Display Job statuses.
        """
        self.log()
        for report_line in self.get_job_status_report():
            self.log(report_line)
        self.log()

    def evaluate(self, expr: AnyExpression, parent_job: Optional[Job] = None) -> Promise:
        """
        Begin an asynchronous evaluation of an expression (concrete value or Expression). Assumes
        this scheduler is currently running.

        This method is not a typical entry point for users, however, it is often used by
        `SchedulerTask`s, to trigger further computations.

        Returns a Promise that will resolve when the evaluation is complete.
        """

        assert self.is_running, "Scheduler is not currently running."

        def eval_term(value):
            # Evaluate one term of an expression.
            if isinstance(value, ValueExpression):
                return value.value
            elif isinstance(value, ApplyExpression):
                return self._evaluate_apply(value, parent_job=parent_job)
            else:
                return value

        def resolve_term(value):
            # Replace promises with their resolved values.
            if isinstance(value, Promise):
                return value.value
            else:
                return value

        pending_expr = map_nested_value(eval_term, expr)
        pending_terms = [
            arg for arg in iter_nested_value(pending_expr) if isinstance(arg, Promise)
        ]
        return Promise.all(pending_terms).then(
            lambda _: map_nested_value(resolve_term, pending_expr)
        )

    def _evaluate_apply(self, expr: ApplyExpression, parent_job: Optional[Job] = None) -> Promise:
        """
        Begin an evaluation of an ApplyExpression.

        This method is responsible for ensuring that each unique expression object is evaluated
        exactly once.

        Returns a Promise that will resolve/reject when the evaluation is complete.
        """

        # Detect expression cycles. Specifically, we look for previous expressions
        # being evaluated within the same parent_job. If we find one, we can
        # reuse its Promise and then copy over the evaluation bookkeeping
        # (call_hash, _upstreams) after its completion.
        # Note: We use `expr.get_hash()` to compensate for an Expression serialization
        # issue discussed in https://github.com/insitro/redun-private/pull/191
        promise_expr = self._pending_expr[parent_job].get(expr.get_hash())
        if promise_expr:
            pending_promise, expr2 = promise_expr

            def callback(result):
                # Copy the evaluation bookkeeping from the completed expression `expr2`
                # to our detected duplicate expression `expr`.
                if isinstance(expr2, TaskExpression):
                    expr.call_hash = expr2.call_hash
                elif isinstance(expr2, SimpleExpression):
                    expr._upstreams = expr2._upstreams
                else:
                    raise AssertionError(f"Unexpected expression: {expr2}")
                return result

            return pending_promise.then(callback)

        # Implementation note: we have to store this promise on the expression provided.
        # This happens at the end of the function, so do not return early!
        promise: Promise

        if isinstance(expr, SchedulerExpression):
            task = self.task_registry.get(expr.task_name)
            if not task:
                promise = Promise(
                    lambda resolve, reject: reject(
                        NotImplementedError(
                            "Scheduler task '{}' does not exist".format(expr.task_name)
                        )
                    )
                )
            else:
                # Scheduler tasks are executed with access to scheduler, parent_job, and
                # raw args (i.e. possibly unevaluated).
                promise = task.func(self, parent_job, expr, *expr.args, **expr.kwargs)

        elif isinstance(expr, TaskExpression):
            # Evaluate task_name to specific task.
            # TaskRegistry is like an environment.
            task = self.task_registry.get(expr.task_name)
            if not task:
                promise = Promise(
                    lambda resolve, reject: reject(
                        f"Task not found in registry: {expr.task_name}.  This can occur if the "
                        "script that defines a user task wasn't loaded or, in the case of redun "
                        "tasks, if an executor is loading a different version of redun that "
                        "fails to define the task."
                    )
                )
            else:
                # TaskExpressions need to be executed in a new Job.
                job = Job(task, expr, parent_job=parent_job, execution=self._current_execution)
                self._jobs.add(job)

                # Make default arguments explicit in case they need to be evaluated.
                expr_args = set_arg_defaults(task, expr.args, expr.kwargs)

                # Evaluate args then execute job.
                args_promise = self.evaluate(expr_args, parent_job=parent_job)
                promise = args_promise.then(lambda eval_args: self._exec_job(job, eval_args))

        elif isinstance(expr, SimpleExpression):
            # Simple Expressions can be executed synchronously.
            func = get_lazy_operation(expr.func_name)
            if not func:
                promise = Promise(
                    lambda resolve, reject: reject(
                        NotImplementedError(
                            "Expression function '{}' does not exist".format(expr.func_name)
                        )
                    )
                )
            else:
                # Evaluate args, apply func, evaluate result.
                args_promise = self.evaluate((expr.args, expr.kwargs), parent_job=parent_job)
                promise = args_promise.then(
                    lambda eval_args: self.evaluate(
                        cast(Callable, func)(*eval_args[0], **eval_args[1]), parent_job=parent_job
                    )
                )

        else:
            raise NotImplementedError("Unknown expression: {}".format(expr))

        # Recording pending expressions.
        self._pending_expr[parent_job][expr.get_hash()] = (promise, expr)
        return promise

    def _is_job_within_limits(self, job_limits: dict) -> bool:
        """
        Helper to determine if a job can be executed under allowed/used limits.
        """
        # Job limits not specified in the config will default to an available limit of 1.
        return all(
            self.limits.get(limit_name, 1) - self.limits_used[limit_name] - count >= 0
            for limit_name, count in job_limits.items()
        )

    def _consume_resources(self, job_limits: Dict[str, int]) -> None:
        """
        Increments the resource limits used with the supplied job limits.
        """
        for limit_name, count in job_limits.items():
            self.limits_used[limit_name] += count

    def _release_resources(self, job_limits: Dict[str, int]) -> None:
        """
        Decrements the resource limits used with the supplied job limits.
        """
        for limit_name, count in job_limits.items():
            self.limits_used[limit_name] -= count

    def _add_job_pending_limits(self, job: Job, eval_args: Tuple[Tuple, dict]) -> None:
        """
        Adds a job to the queue of jobs waiting to run once resources are available.
        """
        self._jobs_pending_limits.append((job, eval_args))

    def _check_jobs_pending_limits(self) -> None:
        """
        Checks whether Jobs pending due to limits can now satisfy limits and run.
        """

        def _add_limits(limits1, limits2):
            return {
                limit_name: limits1[limit_name] + limits2[limit_name]
                for limit_name in set(limits1) | set(limits2)
            }

        ready_jobs: List[Tuple[Job, Tuple[tuple, dict]]] = []
        not_ready_jobs: List[Tuple[Job, Tuple[tuple, dict]]] = []
        ready_job_limits: Dict[str, int] = defaultdict(int)

        # Resource limits that will be consumed by ready jobs.
        for job, eval_args in self._jobs_pending_limits:
            job_limits = job.get_limits()
            proposed_limits = _add_limits(job_limits, ready_job_limits)
            if self._is_job_within_limits(proposed_limits):
                ready_jobs.append((job, eval_args))
                ready_job_limits = proposed_limits
            else:
                not_ready_jobs.append((job, eval_args))

        self._jobs_pending_limits = not_ready_jobs

        # We're just nominating these jobs to start again, but their resource needs will be
        # checked again as part of restarting them. It's possible they'll get re-queued again.
        for job, eval_args in ready_jobs:
            self._exec_job(job, eval_args)

    def _check_pending_job(self, job: Job) -> Optional[Job]:
        """
        Checks whether an equivalent Job is already running.

        This check is necessary to avoid double evaluating an expression that is
        not in the cache yet since it is pending.

        This is a form of common subexpression elimination.
        https://en.wikipedia.org/wiki/Common_subexpression_elimination

        If there is another job we can rely on, return it. Otherwise, return `None`.
        """

        if (
            job.get_option("cache_scope", CacheScope.BACKEND, as_type=CacheScope)
            == CacheScope.NONE
        ):
            return None

        # Check the task has been configured to restrict CSE. This isn't typical, but it
        # is allowed.
        allowed_cache_results = job.get_option("allowed_cache_results", None)
        if allowed_cache_results is not None:
            assert isinstance(allowed_cache_results, set)
            if CacheResult.CSE not in allowed_cache_results:
                return None

        pending_job = self._pending_jobs.get(job.eval_hash)
        if pending_job:
            job.collapse(pending_job)
            return pending_job

        return None

    def _exec_job(self, job: Job, eval_args: Tuple[Tuple, dict]) -> Promise:
        """
        Execute a job that is ready using the fully evaluated arguments.
        """
        # Delay the execution to a new event so we don't interrupt the current event.
        self.events_queue.put(lambda: self._exec_job_main_thread(job, eval_args))
        return job.result_promise

    def _exec_job_main_thread(self, job: Job, eval_args: Tuple[Tuple, dict]) -> None:
        """
        Execute a job that is ready using the fully evaluated arguments.

        This function runs on the main scheduler thread.
        """

        # Downgrade the cache option, if needed.
        if not self._use_cache:
            job.task_options["cache_scope"] = CacheScope.NONE

        # Ensure we are on main scheduler thread.
        assert self.thread_id == threading.get_ident()
        assert job.task

        assert not job.was_cached, f"Job {job} was marked as cached, but we're evaluating it?"

        # Set eval_args on job.
        job.eval_args = eval_args

        # Preprocess arguments before sending them to task function.
        args, kwargs = job.eval_args
        args, kwargs = job.args = self._preprocess_args(job, args, kwargs)

        # Check cache using eval_hash as key.
        job.eval_hash, job.args_hash = hash_args_eval(self.type_registry, job.task, args, kwargs)
        assert job.eval_hash
        assert job.args_hash

        # Check whether a job of the same eval_hash is already running.
        if self._check_pending_job(job) is not None:
            # If we're trying to execute a job that is being re-triggered because we hit
            # resource limits on the first try, it's important we don't hit this exit,
            # because then the job will have been dropped.

            # There's no work to do, but be sure we consider it started.
            self.backend.record_job_start(job)

            return

        # Check cache for job.
        result, job.was_cached, call_hash = self._get_cache(job)

        if job.was_cached:
            # Evaluation was cached, so we proceed to done_job/resolve_job.
            self.log(
                "{action} Job {job_id}:  {task_call} (eval_hash={eval_hash}{check_valid}, "
                "call_hash={call_hash})".format(
                    job_id=job.id[:8],
                    action="Cached".ljust(JOB_ACTION_WIDTH),
                    task_call=format_task_call(job.task, args, kwargs),
                    eval_hash=job.eval_hash[:8],
                    call_hash=call_hash[:8] if call_hash else "None",
                    check_valid=", check_valid=shallow" if call_hash else "",
                )
            )

            # Record the call hash, if we recovered one.
            job.call_hash = call_hash

            # There's no work to do, but be sure we consider it started.
            self.backend.record_job_start(job)

            # Trigger downstream steps, just like an executor would, upon completing it.
            # One of the roles of `done_job` is to trigger evaluation on `result`, in case it is
            # an expression. In the case of an ultimate reduction cache hit, we know that's not
            # needed. However, we let this duplicate work happen anyway, because it keeps the
            # program flow the same and is inexpensive.
            if isinstance(result, ErrorValue):
                return self.reject_job(job, result.error, result.traceback)
            else:
                return self.done_job(job, result)

        if not self._dryrun:
            # Perform rollbacks due to Handles that conflict with past Handles.
            self._perform_rollbacks(args, kwargs)

            # Since the result was not cached, either run it now, or reschedule for whenever
            # the resources are available.
            job_limits = job.get_limits()
            if not self._is_job_within_limits(job_limits):
                self._add_job_pending_limits(job, eval_args)
                return
            self._consume_resources(job_limits)

        # Record that the job is actually starting.
        self.backend.record_job_start(job)

        # Determine executor.
        executor_name = job.get_option("executor") or "default"
        executor = self.executors.get(executor_name)
        if not executor:
            return self.reject_job(
                job, SchedulerError('Unknown executor "{}"'.format(executor_name))
            )

        self.log(
            "{action} Job {job_id}:  {task_call} on {executor}".format(
                job_id=job.id[:8],
                action=("Dryrun" if self._dryrun else "Run").ljust(JOB_ACTION_WIDTH),
                task_call=format_task_call(job.task, args, kwargs),
                executor=executor_name,
            )
        )

        # Stop short of submitting jobs during a _dryrun.
        if self._dryrun:
            return

        # Record the job as pending, since we're submitting it.
        # Note that if the CSE is disabled, this job might have the same `eval_hash` as a prior
        # one. We don't care about overwriting, however, since they're all equivalent.
        self._pending_jobs[job.eval_hash] = job

        # Submit job.
        if not job.task.script:
            executor.submit(job)
        else:
            executor.submit_script(job)

    def add_job_tags(self, job: Job, tags: List[Tuple[str, Any]]) -> None:
        """
        Callback for adding job tags during job execution.
        """
        # Perform database actions on main scheduler thread.
        self.events_queue.put(
            lambda: self.backend.record_tags(
                entity_type=TagEntity.Job, entity_id=job.id, tags=tags
            )
        )

    def done_job(self, job: Job, result: Any, job_tags: List[Tuple[str, Any]] = []) -> None:
        """
        Mark a :class:`Job` as successfully done with a `result`.

        A primary Executor lifecycle method, hence is thread safe.
        """
        self.events_queue.put(lambda: self._done_job_main_thread(job, result, job_tags=job_tags))

    def _done_job_main_thread(
        self, job: Job, result: Any, job_tags: List[Tuple[str, Any]] = []
    ) -> None:
        """
        Mark a :class:`Job` as successfully done with a `result`.

        Note that `result` might require additional evaluation.

        This function runs on the main scheduler thread. Use
        :method:`Scheduler.done_job()` if calling from another thread.
        """

        # Ensure we are on main scheduler thread.
        assert self.thread_id == threading.get_ident()

        # Cached jobs won't have used any resources.
        if not job.was_cached:
            self._release_resources(job.get_limits())
            self._check_jobs_pending_limits()

        assert job.task
        assert job.eval_hash
        assert job.eval_args
        assert job.args_hash

        job.job_tags.extend(job_tags)

        # Update cache. Note that cache scope dictates whether we *read* from the cache, but we
        # always *write* to the backend.
        if not job.was_cached:
            assert not self._dryrun
            result = self._postprocess_result(job, result, job.eval_hash)
            self.set_cache(job.eval_hash, job.task.hash, job.args_hash, result)

        # Eval result (child tasks), then resolve job.
        self.evaluate(result, parent_job=job).then(
            lambda result: self._resolve_job(job, result)
        ).catch(lambda error: self.reject_job(job, error))

    def _resolve_job(self, job: Job, result: Any) -> None:
        """
        Resolve a :class:`Job` with a fully evaluated result. Internal method; executors should
        call `done_job` or `reject_job`.

        This occurs after a job is done and the result is fully evaluated.
        """
        self.events_queue.put(lambda: self._resolve_job_main_thread(job, result))

    def _resolve_job_main_thread(self, job: Job, result: Any) -> None:
        """
        Resolve a :class:`Job` with a fully evaluated result.

        This occurs after a job is done and the result is fully evaluated.

        This function runs on the main scheduler thread. Use
        :method:`Scheduler._resolve_job()` if calling from another thread.
        """
        # Ensure we are on main scheduler thread.
        assert self.thread_id == threading.get_ident()

        assert job.task
        assert job.args_hash
        assert job.eval_args

        # If the call hash is already known, we must have retrieved it from the cache. Otherwise,
        # compute the hash and record the call node.
        if job.call_hash:
            assert job.was_cached
        else:
            # Ignore failed child jobs, which have no call_hash.
            child_call_hashes = [
                cast(str, child_job.call_hash)
                for child_job in job.child_jobs
                if child_job.call_hash
            ]

            # Compute final call_hash and record CallNode.
            # This is a no-op if job was cached.
            result_hash = self.backend.record_value(result)
            job.call_hash = self.backend.record_call_node(
                task_name=job.task.fullname,
                task_hash=job.task.hash,
                args_hash=job.args_hash,
                expr_args=(job.expr.args, job.expr.kwargs),
                eval_args=job.eval_args,
                result_hash=result_hash,
                child_call_hashes=child_call_hashes,
            )

        self._record_job_tags(job)

        self.backend.record_job_end(job)
        job.resolve(result)
        self._finalize_job(job)

    def _finalize_job(self, job: Job) -> None:
        """
        Clean up finalized job.
        """
        self._jobs.remove(job)
        self._finalized_jobs[job.task.fullname][job.status] += 1

        # By poping the job, we free up all expressions that we created within
        # the job. They no longer can be part of any more cycles.
        self._pending_expr.pop(job, None)

        # Once a job has been recorded to the cache, we don't need to keep around
        # the job, since if we see it again, we'll simply download the result.
        self._pending_jobs.pop(job.eval_hash, None)

    def _record_job_tags(self, job: Job) -> None:
        """
        Record tags acquired during Job.
        """
        assert job.task
        assert job.execution

        # Record value tags.
        for value_hash, tags in job.value_tags:
            self.backend.record_tags(entity_type=TagEntity.Value, entity_id=value_hash, tags=tags)

        # Record Job tags.
        job_tags = job.get_option("tags", []) + job.job_tags
        if job_tags:
            self.backend.record_tags(entity_type=TagEntity.Job, entity_id=job.id, tags=job_tags)

        # Record Execution tags.
        if job.execution_tags:
            self.backend.record_tags(
                entity_type=TagEntity.Execution,
                entity_id=job.execution.id,
                tags=job.execution_tags,
            )

        # Record Task tags.
        task_tags = job.task.get_task_option("tags")
        if task_tags:
            self.backend.record_tags(
                entity_type=TagEntity.Task, entity_id=job.task.hash, tags=task_tags
            )

    def reject_job(
        self,
        job: Optional[Job],
        error: Any,
        error_traceback: Optional[Traceback] = None,
        job_tags: List[Tuple[str, Any]] = [],
    ) -> None:
        """
        Reject a :class:`Job` that has failed with an `error`.

        A primary Executor lifecycle method, hence is thread safe.
        """
        self.events_queue.put(
            lambda: self._reject_job_main_thread(
                job, error, error_traceback=error_traceback, job_tags=job_tags
            )
        )

    def _reject_job_main_thread(
        self,
        job: Optional[Job],
        error: Any,
        error_traceback: Optional[Traceback] = None,
        job_tags: List[Tuple[str, Any]] = [],
    ) -> None:
        """
        Reject a :class:`Job` that has failed with an `error`.

        This function runs on the main scheduler thread. Use
        :method:`Scheduler.reject_job()` if calling from another thread.
        """
        # Ensure we are on main scheduler thread.
        assert self.thread_id == threading.get_ident()

        # Attach the traceback to the error so the redun traceback is always available on the error
        # itself. In some cases (like AWS batch), the error will be constructed by unpickling a
        # pickled Excetption(__traceback__ is not included when pickling). By attaching the
        # traceback here, we make sure the traceback is available for downstream handlers.
        error.redun_traceback = error_traceback

        if job:
            assert job.task
            assert job.args_hash
            assert job.eval_args

            job.job_tags.extend(job_tags)

            self.log(
                "*** {action} Job {job_id}:  {task_call}:".format(
                    job_id=job.id[:8],
                    action="Reject".ljust(JOB_ACTION_WIDTH),
                    task_call=format_task_call(job.task, job.eval_args[0], job.eval_args[1]),
                )
            )

            # Cached jobs won't have used any resources.
            if not job.was_cached:
                self._release_resources(job.get_limits())
                self._check_jobs_pending_limits()

            if self.use_task_traceback:
                self._set_task_traceback(job, error, error_traceback=error_traceback)

            child_call_hashes = [
                child_job.call_hash for child_job in job.child_jobs if child_job.call_hash
            ]

            # Compute final call_hash and record CallNode.
            error_value = ErrorValue(error, error_traceback or Traceback.from_error(error))
            try:
                error_hash = self.backend.record_value(error_value)
            except (TypeError, AttributeError):
                # Some errors cannot be serialized so record them as generic Exceptions.
                error2 = Exception(repr(error))
                error_value = ErrorValue(error2, error_traceback or Traceback.from_error(error2))
                error_hash = self.backend.record_value(error_value)
            job.call_hash = self.backend.record_call_node(
                task_name=job.task.fullname,
                task_hash=job.task.hash,
                args_hash=job.args_hash,
                expr_args=(job.expr.args, job.expr.kwargs),
                eval_args=job.eval_args,
                result_hash=error_hash,
                child_call_hashes=child_call_hashes,
            )
            self._record_job_tags(job)
            self.backend.record_job_end(job, status="FAILED")
            job.reject(error)
            self._finalize_job(job)

        else:
            # Error is not job specific. Often this could be a redun-internal error.
            # Stop whole workflow.
            self.log("*** Workflow error")
            if not error_traceback:
                # Since this is an unusual workflow error, lets see all the frames.
                error_traceback = Traceback.from_error(error, trim_frames=False)
            self.traceback = error_traceback
            assert self.workflow_promise
            self.workflow_promise.do_reject(error)

    def _set_task_traceback(
        self, job: Job, error: Any, error_traceback: Optional[Traceback] = None
    ) -> None:
        """
        Set traceback to be :class:`redun.task.Task` based.
        """
        if self.traceback:
            # The traceback has already been set.
            return

        # Get job stack.
        job_stack: List[Job] = []
        ptr: Optional[Job] = job
        while ptr:
            job_stack.append(ptr)
            ptr = ptr.parent_job
        job_stack.reverse()

        # Get task-based traceback.
        task_frames: List[FrameSummary] = []
        for frame_job in job_stack:
            assert frame_job.task
            assert frame_job.eval_args
            args, kwargs = frame_job.eval_args

            sig = inspect.signature(frame_job.task.func)
            task_frames.append(
                Frame(
                    frame_job.task.func.__code__.co_filename,
                    frame_job.task.func.__code__.co_firstlineno,
                    frame_job.task.func.__name__,
                    job=frame_job,
                    locals={
                        **dict(zip(sig.parameters, args)),
                        **kwargs,
                    },
                )
            )

        # Graft user function frames onto task traceback.
        if not error_traceback:
            error_traceback = Traceback.from_error(error)
        task_frames.extend(error_traceback.frames)

        # Set this execution's traceback.
        self.traceback = Traceback(error, task_frames, logs=error_traceback.logs)

    def _get_cache(self, job: Job) -> Tuple[Any, bool, Optional[str]]:
        """
        Attempt to find a cached value for an evaluation (`eval_hash`).

        Returns
        -------
        (result, is_cached, call_hash): Tuple[Any, bool, Optional[str]]
           is_cached is True if `result` was in the cache.
           Note that the result may be an `ErrorValue`, which the caller must unwrap.
        """
        check_valid = job.get_option("check_valid", CacheCheckValid.FULL, as_type=CacheCheckValid)

        # HACK: This is a workaround introduced by DE-4761. See the ticket for
        # notes on how script_task could be redesigned to remove the need for
        # this special-casing.
        is_script_task = job.task.fullname == "redun.script_task"

        # Determine whether we should use cache.
        cache_scope = (
            CacheScope.CSE
            if is_script_task
            else job.get_option("cache_scope", CacheScope.BACKEND, as_type=CacheScope)
        )

        allowed_cache_results = job.get_option("allowed_cache_results", None)
        assert allowed_cache_results is None or isinstance(allowed_cache_results, set), (
            "Should have gotten a set for option `allowed_cache_results`, got"
            f" {allowed_cache_results} instead."
        )

        # Check CSE and cache.
        assert job.task
        assert job.args_hash
        assert job.eval_hash
        assert job.execution
        result, call_hash, cache_type = self.backend.check_cache(
            job.task.hash,
            job.args_hash,
            job.eval_hash,
            job.execution.id,
            self.task_registry.task_hashes,
            cache_scope,
            check_valid,
            allowed_cache_results,
        )

        if cache_type == CacheResult.CSE:
            # If this is a CSE hit, we can use the result immediately. The result may be
            # an error wrapped as a `ErrorValue`, but we can still use it.
            return result, True, call_hash
        elif isinstance(result, ErrorValue):
            # Errors can't be used from the backend cache.
            return None, False, None
        elif cache_type == CacheResult.MISS:
            if self._dryrun:
                # In dryrun mode, log reason for cache miss.
                self._log_cache_miss(job, job.task.hash, job.args_hash)
            return None, False, None

        elif self._is_valid_value(result):
            # Result must still be valid to use.
            return result, True, call_hash

        else:
            self.log(
                "{action} Job {job}:  Cached result is no longer valid "
                "(result={result}, eval_hash={eval_hash}).".format(
                    action="Miss".ljust(JOB_ACTION_WIDTH),
                    job=job.id[:8],
                    result=trim_string(repr(result)),
                    eval_hash=job.eval_hash[:8],
                )
            )
            return None, False, None

    def _log_cache_miss(self, job: Job, task_hash: str, args_hash: str) -> None:
        """
        Log the reason for a cache miss.
        """
        task = self.task_registry.get(hash=task_hash)
        reason = self.backend.explain_cache_miss(task, args_hash)

        if not reason:
            # No reason could be determined.
            self.log(
                "{action} Job {job}:  Cannot determine reason for cache miss of "
                "'{task_name}()' (task_hash={task_hash}, args_hash={args_hash}).".format(
                    action="Miss".ljust(JOB_ACTION_WIDTH),
                    job=job.id[:8],
                    task_name=task.fullname,
                    task_hash=task_hash[:8],
                    args_hash=args_hash[:8],
                )
            )

        elif reason["reason"] == "new_task":
            self.log(
                "{action} Job {job}:  New task '{task_name}()' with previous arguments "
                "(task_hash={old_hash} --> {new_hash}, args_hash={args_hash}).".format(
                    action="Miss".ljust(JOB_ACTION_WIDTH),
                    job=job.id[:8],
                    task_name=task.fullname,
                    old_hash=reason["call_task_hash"][:8],
                    new_hash=task_hash[:8],
                    args_hash=args_hash[:8],
                )
            )

        elif reason["reason"] == "new_args":
            self.log(
                "{action} Job {job}:  Existing task '{task_name}()' is called with new arguments "
                "(task_hash={task_hash}, args_hash={old_hash} --> {new_hash}).".format(
                    action="Miss".ljust(JOB_ACTION_WIDTH),
                    job=job.id[:8],
                    task_name=task.fullname,
                    task_hash=task_hash[:8],
                    old_hash=reason["call_args_hash"][:8],
                    new_hash=args_hash[:8],
                )
            )

        elif reason["reason"] == "new_call":
            self.log(
                "{action} Job {job}:  New task '{task_name}()' is called with new arguments "
                "(task_hash={task_hash}, args_hash={args_hash}).".format(
                    action="Miss".ljust(JOB_ACTION_WIDTH),
                    job=job.id[:8],
                    task_name=task.fullname,
                    task_hash=task_hash[:8],
                    args_hash=args_hash[:8],
                )
            )

        else:
            raise NotImplementedError(reason)

    def set_cache(self, eval_hash: str, task_hash: str, args_hash: str, value: Any) -> None:
        """
        Set the cache for an evaluation (`eval_hash`) with result `value`.

        This method should only be used by the scheduler or `SchedulerTask`s
        """
        self.backend.set_eval_cache(eval_hash, task_hash, args_hash, value, value_hash=None)

    def _is_valid_value(self, value: Any) -> bool:
        """
        Returns True if the value is valid.

        Valid Nested Values must have all of their subvalues be valid.
        """
        return self.type_registry.is_valid_nested(value)

    def _perform_rollbacks(self, args: Tuple, kwargs: dict) -> None:
        """
        Perform any Handle rollbacks needed for the given Task arguments.
        """
        for value in iter_nested_value((args, kwargs)):
            if isinstance(value, Handle):
                self.backend.rollback_handle(value)

    def _preprocess_args(self, job: Job, args: Tuple, kwargs: dict) -> Any:
        """
        Preprocess arguments for a Task before execution.
        """

        def preprocess_value(value):
            if isinstance(value, Handle):
                if job.parent_job:
                    job.parent_job.handle_forks[value.get_hash()] += 1
                    call_order = job.parent_job.handle_forks[value.get_hash()]
                else:
                    call_order = 0
                preprocess_args = {"call_order": call_order}
            else:
                preprocess_args = {}

            value2 = self.type_registry.preprocess(value, preprocess_args)

            # Handles need extra recording for preprocessing.
            if isinstance(value, Handle):
                assert value2 != value
                self.backend.advance_handle([value], value2)

            return value2

        return map_nested_value(preprocess_value, (args, kwargs))

    def _postprocess_result(self, job: Job, result: Any, pre_call_hash: str) -> Any:
        """
        Postprocess a result from a Task before caching.
        """
        postprocess_args = {
            "pre_call_hash": pre_call_hash,
        }

        def postprocess_value(value):
            value2 = self.type_registry.postprocess(value, postprocess_args)

            if isinstance(value, Handle) and not self._dryrun:
                # Handles accumulate state change from jobs that emit them.
                assert value2 != value
                self.backend.advance_handle([value], value2)

            return value2

        return map_nested_value(postprocess_value, result)


@scheduler_task(namespace="redun")
def merge_handles(
    scheduler: Scheduler, parent_job: Job, sexpr: SchedulerExpression, handles_expr: List[Handle]
) -> Promise:
    """
    Merge multiple handles into one.
    """

    def then(handles):
        assert handles
        assert len({handle.__handle__.name for handle in handles}) == 1

        final_handle = handles[0]
        other_handles = handles[1:]

        scheduler.backend.advance_handle(other_handles, final_handle)
        return final_handle

    return scheduler.evaluate(handles_expr, parent_job=parent_job).then(then)


@scheduler_task(namespace="redun")
def cond(
    scheduler: Scheduler,
    parent_job: Job,
    sexpr: SchedulerExpression,
    cond_expr: Any,
    then_expr: Any,
    *rest: Any,
) -> Promise:
    """
    Conditionally execute expressions, i.e. a lazy if-statement.

    .. code-block:: python

        @task()
        def is_even(x):
            return x % 2 == 0

        @task()
        def task1():
            # ...

        @task()
        def task2():
            # ...

        @task()
        def main(x: int):
            return cond(
                is_even(x),
                task1(),
                task2()
            )
    """
    exprs = (cond_expr, then_expr) + rest

    def then(args):
        i, cond_value = args

        if cond_value:
            # Return 'then' clause.
            return scheduler.evaluate(exprs[i + 1], parent_job=parent_job)

        elif len(exprs) - i == 3:
            # No more expresses, so return 'otherwise' clause.
            return scheduler.evaluate(exprs[i + 2], parent_job=parent_job)

        else:
            # Recurse to next conditional clause.
            return scheduler.evaluate((i + 2, exprs[i + 2]), parent_job=parent_job).then(then)

    # Evaluate conditional clause.
    return scheduler.evaluate((0, cond_expr), parent_job=parent_job).then(then)


@task(namespace="redun", version="1")
def throw(error: Exception) -> None:
    """
    Raises an exception.

    This is task is useful in cases where raising needs to be done lazily.
    """
    raise error


@scheduler_task(namespace="redun")
def catch(
    scheduler: Scheduler, parent_job: Job, sexpr: SchedulerExpression, expr: Any, *catch_args: Any
) -> Promise:
    """
    Catch exceptions `error` of class `error_class` from `expr` and evaluate `recover(error)`.

    The following example catches the `ZeroDivisionError` raised by `divider(0)`
    and returns 0.0 instead.

    .. code-block:: python

        @task()
        def divider(denom):
            return 1.0 / denom

        @task()
        def recover(error):
            return 0.0

        @task()
        def main():
            return catch(
                divider(0),
                ZeroDivisionError,
                recover
            )

    This is equivalent to the regular python code:

    .. code-block:: python

        def divider(denom):
            return 1.0 / denom

        def main():
            try:
                return denom(0)
            except ZeroDivisionError as error:
                return 0.0

    `catch()` can also handle multiple Exception classes as well as multiple
    recover expressions.

    .. code-block:: python

        @task()
        def main():
            return catch(
                task1(),
                (ZeroDivisionError, ValueError),
                recover1,
                KeyError,
                recover2,
            )

    which is equivalent to the regular python code:

    .. code-block:: python

        try:
            return task1()
        except (ZeroDivisionError, ValueError) as error:
            return recover1(error)
        except KeyError as error:
            return recover2(error)

    Implementation/behavior note: Usually, we don't cache errors, since this is rarely productive.
    Also recall that we typically cache each round of evaluation separately, allowing
    the scheduler to retrace the call graph and detect if any subtasks have changed
    (i.e. hash change) or if any Values in the subworkflow are now invalid (e.g. File hash has
    changed). To correctly cache a caught expression, we need to cache both the fact that
    `expr` resolved to an `Exception`, then that the `recover_expr` produced a non-error result.
    However, we only want to cache the exception if it is successfully handled, so if the
    recovery re-raises the exception or issues another error, the error should not be cached.

    In order to create this behavior, we have to implement custom caching behavior that delays
    caching the error until we see that it has been successfully handled.


    Parameters
    ----------
    expr: Expression
        Main expression to evaluate.
    error_recover_pairs:
        A list of alternating error_class and recover Tasks.
    """
    errors: List[Union[Type[Exception], Tuple[Type[Exception]]]] = list(catch_args[::2])
    recovers: List[Callable] = list(catch_args[1::2])
    error_recover_pairs = list(zip(errors, recovers))
    eval_hash: str
    args_hash: str
    recover_expr: Any = None

    def on_success(result: Result) -> Result:
        # Cache `expr` if catch is ultimately successful.
        scheduler.backend.record_value(catch)
        scheduler.set_cache(eval_hash, catch.hash, args_hash, expr)
        return result

    def on_recover(result: Result) -> Result:
        # Cache `recover_expr` if exception was thrown.
        scheduler.backend.record_value(catch)
        scheduler.set_cache(eval_hash, catch.hash, args_hash, recover_expr)
        return result

    def promise_catch(error: Any) -> Promise:
        nonlocal recover_expr

        for error_class, recover in error_recover_pairs:
            if isinstance(error, error_class):
                # Record dataflow for caught error:
                #   expr --> error --> recover(error) --> sexpr
                error_expr = derive_expression(expr, error)
                recover_expr = recover(error_expr)
                derive_expression(recover_expr, sexpr)

                # Error matches the class, evaluate the recover task.
                return scheduler.evaluate(recover_expr, parent_job=parent_job).then(on_recover)

        # Error did not match any classes, reraise the error.
        raise error

    # Check cache.
    catch_args = (expr,) + catch_args
    eval_hash, args_hash = hash_args_eval(scheduler.type_registry, catch, catch_args, {})
    # We haven't set an option on the catch task, so we just have to look for one at runtime.
    cache_scope = CacheScope(sexpr.task_expr_options.get("cache_scope", CacheScope.BACKEND))
    if scheduler._use_cache and cache_scope != CacheScope.NONE:
        assert parent_job.execution
        cached_expr, call_hash, cache_type = scheduler.backend.check_cache(
            task_hash=catch.hash,
            args_hash=args_hash,
            eval_hash=eval_hash,
            execution_id=parent_job.execution.id,
            cache_scope=cache_scope,
            check_valid=CacheCheckValid.FULL,
            scheduler_task_hashes=scheduler.task_registry.task_hashes,
            allowed_cache_results={CacheResult.SINGLE},
        )
        if cache_type != CacheResult.MISS:
            return scheduler.evaluate(cached_expr, parent_job=parent_job).catch(promise_catch)

    return scheduler.evaluate(expr, parent_job=parent_job).then(on_success, promise_catch)


@scheduler_task(namespace="redun", version="1")
def catch_all(
    scheduler: Scheduler,
    parent_job: Job,
    sexpr: SchedulerExpression,
    exprs: T,
    error_class: Union[None, Exception, Tuple[Exception, ...]] = None,
    recover: Optional[Task[Callable[..., S]]] = None,
) -> Promise[Union[T, S]]:
    """
    Catch all exceptions that occur in the nested value `exprs`.

    This task works similar to `catch`, except it takes multiple
    expressions within a nested value (e.g. a list, set, dict, etc).
    Any exceptions raised by the expressions are caught and processed only after
    all expressions succeed or fail. For example, this can be useful in large
    fanouts where the user wants to avoid one small error stopping the whole
    workflow immediately. Consider the following code:

    .. code-block:: python

        @task
        def divider(denom):
            return 1.0 / denom

        @task
        def recover(values):
            nerrors = sum(1 for value in values if isinstance(value, Exception))
            raise Exception(f"{nerrors} error(s) occurred.")

        @task
        def main():
            result catch_all([divider(1), divider(0), divider(2)], ZeroDivisionError, recover)

    Each of the expressions in the list `[divider(1), divider(0), divider(2)]`
    will be allowed to finish before the exceptions are processed by `recover`. If
    there are no exceptions, the evaluated list is returned. If there are any
    exceptions, the list is passed to `recover`, where it will contain each
    successful result or Exception. `recover` then has the opportunity to process
    all errors together. In the example above, the total number of errors is
    reraised.

    Parameters
    ----------
    exprs:
        A nested value (list, set, dict, tuple, NamedTuple) of expressions to evaluate.
    error_class: Union[Exception, Tuple[Expression, ...]]
        An Exception or Exceptions to catch.
    recover: Task
        A task to call if any errors occurs. It will be called with the nested
        value containing both successful results and errors.
    """

    def postprocess(pending_terms):
        # Gather all errors.
        errors = [promise.error for promise in pending_terms if promise.is_rejected]

        if errors:
            if not recover:
                # By default, just reraise the first error.
                raise errors[0]
            else:

                def do_recover(error_class_recover):
                    error_class, recover = error_class_recover
                    if all(isinstance(error, error_class) for error in errors):
                        return scheduler.evaluate(
                            recover(map_nested_value(resolve_term, pending_expr)),
                            parent_job=parent_job,
                        )
                    else:
                        # By default, just reraise first non-matching error.
                        raise next(error for error in errors if not isinstance(error, error_class))

                # Evaluate error_class and recover in case they're Expressions.
                return scheduler.evaluate((error_class, recover), parent_job=parent_job).then(
                    do_recover
                )
        else:
            # Return the evaluated nested value.
            return map_nested_value(resolve_term, pending_expr)

    def resolve_term(value):
        # Replace a Promise with its return value or its error.
        if isinstance(value, Promise):
            if value.is_fulfilled:
                return value.value
            else:
                return value.error
        else:
            # Regular values (non-Promises) pass through unchanged.
            return value

    pending_terms = []

    def eval_term(value):
        # Evaluate one term and track its promise in pending_terms.
        promise = scheduler.evaluate(value, parent_job=parent_job)
        pending_terms.append(promise)
        return promise

    pending_expr = map_nested_value(eval_term, exprs)
    return wait_promises(pending_terms).then(postprocess)


@scheduler_task(namespace="redun")
def apply_tags(
    scheduler: Scheduler,
    parent_job: Job,
    sexpr: SchedulerExpression,
    value: Any,
    tags: List[Tuple[str, Any]] = [],
    job_tags: List[Tuple[str, Any]] = [],
    execution_tags: List[Tuple[str, Any]] = [],
) -> Promise:
    """
    Apply tags to a value, job, or execution.

    Returns the original value.

    .. code-block:: python

        @task
        def task1():
            x = 10
            # Apply tags on the value 10.
            return apply_tags(x, [("env": "prod", "project": "acme")])

        @task
        def task2():
            x = 10
            # Apply tags on the current job.
            return apply_tags(x, job_tags=[("env": "prod", "project": "acme")])

        @task
        def task3():
            x = 10
            # Apply tags on the current execution.
            return apply_tags(x, execution_tags=[("env": "prod", "project": "acme")])
    """

    def then(args):
        value, tags, job_tags, execution_tags = args
        if tags:
            value_hash = scheduler.backend.record_value(value)
            parent_job.value_tags.append((value_hash, tags))
        parent_job.job_tags.extend(job_tags)
        parent_job.execution_tags.extend(execution_tags)
        return value

    return scheduler.evaluate((value, tags, job_tags, execution_tags), parent_job=parent_job).then(
        then
    )


@task(
    namespace="redun",
    name="subrun_root_task",
    version="1",
    config_args=["config", "config_dir", "load_modules", "run_config"],
    cache_scope=CacheScope.CSE,
    check_valid=CacheCheckValid.SHALLOW,
)
def _subrun_root_task(
    expr: QuotedExpression,
    config: Dict[str, Dict],
    config_dir: Optional[str],
    load_modules: List[str],
    run_config: Dict[str, Any],
) -> Any:
    """
    Launches a sub-scheduler and runs the provided expression by first "unwrapping" it.
    The evaluated result is returned within a dict alongside other sub-scheduler-related
    state to the caller.

    This task is limited to execution cache scope because it is difficult for the caller to
    be reactive to the details of the subrun. When retrieving a cache value for this task,
    the parent scheduler only sees the final values, not the whole call tree, so it can only
    perform shallow checking. Selecting execution scope means that validity checking is not
    required. We also set shallow checking as a reminder of how it behaves.

    In the case that there is no execution-scope cache hit, then the sub-scheduler has to
    be started. However, the sub-scheduler is free to perform any cache logic it desires. It
    has the complete context to be fully reactive, or not, as configured. So, if there is a
    backend-scope cache hit available, the sub-scheduler may return almost instantly.

    The parent scheduler may or may not have access to the code, which also makes full reactivity
    difficult. It is a mild assumption that the code does not change during an execution.

    There is one particular case we have not implemented, where the parent scheduler could bypass
    the need to start the sub-scheduler. If 1) the user only wants shallow validity checking
    and 2) the parent scheduler has all access all the code (to compute task hashes), then
    the parent scheduler could correctly identify a backend-scope cache hit using information
    stored from the prior call node. This is left to future work.

    Parameters
    ----------
    expr: QuotedExpression
        QuotedExpression to be run by sub-scheduler.
    config
        A two-level python dict to configure the sub-scheduler.  (Will be used to initialize a
        :class:`Config` object via the `config_dict` kwarg.) Used if a `config_dir` is not
        provided.
    config_dir : Optional[str]
        If supplied, this config is loaded and used instead of the supplied `config`
    load_modules
        List of modules that must be imported before starting the sub-scheduler. Before
        launching the sub-scheduler, the modules that define the user tasks must be imported.
    run_config
        Sub-scheduler run() kwargs. These are typically the local-scheduler run() kwargs that
        should also apply to the sub-scheduler such as `dryrun` and `cache` settings.

    Returns
    -------
    Dict[str, Any]
        Returns a dict result from running the sub-scheduler on `expr`.  The dict contains extra
        information pertaining to the sub-scheduler as follows:
        {
          'result': sub-scheduler evaluation of `expr`, present if successful
          'error': exception raised by sub-scheduler, present if error was encountered
          'dryrun': present if dryrun was requested and workflow can't complete

          'job_id': Job id of root Job in sub-scheduler,
                    present if parent_job_id given in run_config
          'call_hash': call_hash of root CallNode in sub-scheduler,
                       present if parent_job_id given in run_config

          'status': sub-scheduler final job status poll (list of str)
          'config': sub-scheduler config dict (useful for confirming exactly what config
            settings were used).
          'run_config': a dict of run configuration used by the sub-scheduler
        }

        parent_job_id is present in run_config, only results are return. Errors are raised.
    """
    assert not config or config_dir is None, "Only one of config and config_dir can be used"

    # A lazily-loaded config directory takes precedence.
    if config_dir is not None:
        # Load config file.
        config_obj = Config()
        config_obj.read_path(Dir(config_dir).file(REDUN_INI_FILE).path)

        # Postprocess config.
        config_obj = postprocess_config(config_obj, config_dir)
    else:
        config_obj = Config(config_dict=config)

    sub_scheduler = Scheduler(config=config_obj)
    sub_scheduler.load()

    # Import user modules.  The user is responsible for ensuring that the Executor that runs this
    # task has access to all these modules' code.  Additionally, any other user modules imported
    # by these modules must also be accessible.
    if load_modules:
        for module in load_modules:
            importlib.import_module(module)

    subrun_result: dict = {
        "config": sub_scheduler.config.get_config_dict(),
    }

    # Extend an existing Execution if the parent_job_id is given. Otherwise, start
    # a new Execution.
    if "parent_job_id" in run_config:
        result = sub_scheduler.extend_run(expr.eval(), **run_config)

        # If extending an Execution, supplement result value with additional
        # information such as job id and call_hash.
        if not isinstance(result, dict):
            raise AssertionError(f"Unknown scheduler result: {result}")
        subrun_result.update(result)

    else:
        execution_id = run_config.get("execution_id", None)
        result = sub_scheduler.run(expr=expr.eval(), execution_id=execution_id, **run_config)
        subrun_result["result"] = result

    subrun_result.update(
        {
            "run_config": {
                "dryrun": sub_scheduler._dryrun,
                "cache": sub_scheduler._use_cache,
            },
            "status": sub_scheduler.get_job_status_report(),
        }
    )
    return subrun_result


@scheduler_task(
    namespace="redun",
    version="1",
    cache_scope=CacheScope.BACKEND,
    check_valid=CacheCheckValid.SHALLOW,
)
def subrun(
    scheduler: Scheduler,
    parent_job: Job,
    sexpr: SchedulerExpression,
    expr: Any,
    executor: str,
    config: Optional[Dict[str, Any]] = None,
    config_dir: Optional[str] = None,
    new_execution: bool = False,
    load_modules: Optional[List[str]] = None,
    **task_options: dict,
) -> Promise:
    """
    Evaluates an expression `expr` in a sub-scheduler running within Executor `executor`.

    `executor` and optional `task_options` are used to configure the special redun task (
    _subrun_root_task) that starts the sub-scheduler. For example, you can configure the task
    with a batch executor to run the sub-scheduler on AWS Batch.

    `config`: To ease configuration management of the sub-scheduler, you can either pass a `config`
    dict which contains configuration that would otherwise require a redun.ini file in the
    sub-scheduler environment, or you can provide a `config_dir` to be loaded when the subrun
    starts. If you do not pass either, the local scheduler's config will be
    forwarded to the sub-scheduler (replacing the local `config_dir` with "."). In practice,
    the sub-scheduler's `config_dir` should be less important as you probably want to log both
    local and sub-scheduler call graphs to a common database. You can also obtain a copy of the
    local scheduler's config and customize it as needed. Instantiate the scheduler directly instead
    of calling `redun run`.  Then access its config via
    `scheduler.py::get_scheduler_config_dict()`.

    Note on code packaging: The user is responsible for ensuring that the chosen Executor for
    invoking the sub-scheduler copies over all user-task scripts.  E.g. the local scheduler may
    be launched on local tasks defined in workflow1.py but subrun(executor="batch) is invoked on
    taskX defined in workflow2.py.  In this case, the user must ensure workflow2.py is copied to
    the batch node by placing it within the same directory tree as workflow1.py.

    The cache behavior of `subrun` is customized. Since recursive reductions are supposed to
    happen inside the subrun, we can't accept a single reduction. If the user wants full
    validity checking, that requires using single reductions, which means we have to actually
    start the sub-scheduler and let it handle the caching logic. In contrast, if a CSE or
    ultimate reduction is available, then we won't need to start the sub-scheduler.

    Most scheduler tasks don't accept the cache options `cache_scope` and `check_valid`,
    but `subrun` does, so we can forward them to the underlying `Task` that implements the subrun.
    Note that we also set the default `check_valid` value to be `CacheCheckValid.SHALLOW`, unlike
    its usual value.

    Parameters
    ----------
    expr : Any
        Expression to be run by sub-scheduler.
    executor : str
        Executor name for the special redun task that launches the sub-scheduler
        (_subrun_root_task). E.g. `batch` to launch the sub-scheduler in AWS Batch. Note
        that this is a config key in the  *local* scheduler's config.
    config : dict
        Optional sub-scheduler config dict. Must be a two-level dict that can be used to
        initialize a :class:`Config` object (see :method:`Config.get_config_dict()`).  If None or
        empty, the local Scheduler's config will be passed to the sub-scheduler and any values
        with local config_dir will be replaced with ".".  Do not include database credentials as
        they will be logged as clear text in the call graph.
    config_dir : str
        Optional path to load a config from. Must be available in the execution context of the
        subrun.
    new_execution : bool
        If True, record the provenance of the evaluation of `expr` as a new Execution, otherwise
        extend the current Execution with new Jobs.
    load_modules: Optional[List[str]]
        If provided, an explicit list of the modules to load to prepare the subrun. If not
        supplied, the `load_module` for every task in the task registry will be included.
    **task_options : Any
        Task options for _subrun_root_task.  E.g. when running the sub-scheduler via Batch
        executor, you can pass ECS configuration (e.g. `memory`, `vcpus`, `batch_tags`).

    Returns
    -------
    Dict[str, Any]
        Returns a dict result of evaluating the _subrun_root_task
    """
    assert not config or config_dir is None, "Only one of config and config_dir can be used"

    # If a config wasn't provided, forward the parent scheduler's backend config to the
    # sub-scheduler, replacing the local config_dir with "."
    if config_dir is not None:
        config = {}
    elif not config:
        config = scheduler.config.get_config_dict(replace_config_dir=".")

    # Collect a list of all modules that define user task.  This list will include the initial
    # task(s) to be subrun() as these are registered in the local task registry during subrun()
    # argument formulation.
    registry = get_task_registry()
    load_modules_set = set()
    if load_modules is not None:
        load_modules_set = set(load_modules)
    else:
        for _task in registry:
            module_name = _task.load_module
            if module_name.startswith("redun.") and not module_name.startswith("redun.tests."):
                continue
            load_modules_set.add(module_name)

    # Prepare child task call for subrun.
    run_config: Dict[str, Any] = {
        "dryrun": scheduler._dryrun,
        "cache": scheduler._use_cache,
    }
    if not new_execution:
        run_config["parent_job_id"] = parent_job.id

    # Extract the cache options.
    all_options: Dict[str, Any] = {
        "cache_scope": CacheScope(
            sexpr.task_expr_options.get("cache_scope", subrun.get_task_option("cache_scope"))
        ),
        "check_valid": CacheCheckValid(
            sexpr.task_expr_options.get("check_valid", subrun.get_task_option("check_valid"))
        ),
        "allowed_cache_results": {CacheResult.CSE, CacheResult.ULTIMATE},
    }
    all_options.update(task_options)

    subrun_root_task_expr = _subrun_root_task.options(executor=executor, **all_options)(
        expr=quote(expr),
        config=config,
        config_dir=config_dir,
        load_modules=sorted(load_modules_set),
        run_config=run_config,
    )

    def log_banner(msg):
        scheduler.log("-" * 79)
        scheduler.log(msg)
        scheduler.log("-" * 79)

    def then(subrun_result):
        if "job_id" in subrun_result:
            # Create stub-job representing the job in the subscheduler.
            # The call_hash is needed to compute the right call_hash of parent_job.
            job = Job(
                root_task,
                root_task(quote(None)),
                id=subrun_result["job_id"],
                parent_job=parent_job,
                execution=scheduler._current_execution,
            )
            job.call_hash = subrun_result["call_hash"]

        # Echo sub-scheduler report.
        scheduler.log()
        log_banner(
            f"BEGIN: Sub-scheduler report (executor='{executor}'), {trim_string(repr(expr))}"
        )

        scheduler.log("Sub-scheduler config:")
        scheduler.log(pprint.pformat(subrun_result["config"]))

        scheduler.log()
        scheduler.log("Sub-scheduler run_config:")
        scheduler.log(pprint.pformat(subrun_result["run_config"]))

        scheduler.log()
        scheduler.log("Sub-scheduler final job status:")
        for report_line in subrun_result["status"]:
            scheduler.log(report_line)

        log_banner("END: Sub-scheduler report")
        scheduler.log()

        if "result" in subrun_result:
            # Return the user's result.
            return subrun_result["result"]

        elif "error" in subrun_result:
            # Reraise the error.
            raise subrun_result["error"]

        elif "dryrun" in subrun_result:
            # Return a promise that never resolves.
            return Promise()

    return scheduler.evaluate(subrun_root_task_expr, parent_job=parent_job).then(then)


@scheduler_task(namespace="redun")
def federated_task(
    scheduler: Scheduler,
    parent_job: Job,
    sexpr: SchedulerExpression,
    entrypoint: str,
    *task_args,
    **task_kwargs,
) -> Promise:
    """Execute a task that has been indirectly specified in the scheduler config file by providing
    a `federated_task` and its executor. This allows us to invoke code we do not have locally.

    Since the code isn't visible, the cache_scope for the subrun is always set to CSE.

    Parameters
    ----------
    entrypoint: str
        The name of the `federated_task` section in the config that identifies the task to perform.
    task_args: Optional[List[Any]]
        Positional arguments for the task
    task_kwargs: Any
        Keyword arguments for the task
    """

    # Load the federated config and find the specified entrypoint within it.
    federated_task_configs = scheduler.config.get("federated_tasks")

    assert entrypoint in federated_task_configs, (
        f"Could not find the entrypoint `{entrypoint}` "
        f"in the provided federated tasks. Found `{list(federated_task_configs.keys())}`"
    )

    entrypoint_config: Dict[str, Any] = dict(federated_task_configs[entrypoint])

    required_keys = {"namespace", "task_name", "load_module", "executor", "config_dir"}
    available_keys = set(entrypoint_config.keys())
    assert required_keys.issubset(available_keys), (
        f"Federated task entry `{entrypoint}` does not have the required keys, missing "
        f"`{required_keys.difference(available_keys)}`"
    )

    # The description is optional, but it shouldn't be in the hash. Just hide it from the
    # downstream subrun, so we don't have to deal with merging a user's `config_args`.
    entrypoint_config.pop("description", None)

    # Extract the keys relevant to hashing
    hash_config = {}
    for key in required_keys:
        hash_config[key] = entrypoint_config[key]

    # This argument needs to be parsed.
    if "new_execution" in entrypoint_config:
        entrypoint_config["new_execution"] = str2bool(entrypoint_config["new_execution"])

    # Since we require a namespace, we can simply dot format this ourselves.
    wrapper_task_expr: TaskExpression = TaskExpression(
        task_name=f'{entrypoint_config.pop("namespace")}.{entrypoint_config.pop("task_name")}',
        args=task_args,
        kwargs=task_kwargs,
    )

    subrun_task = subrun(
        expr=wrapper_task_expr,
        executor=entrypoint_config.pop("executor"),
        load_modules=[entrypoint_config.pop("load_module")],
        # Since the code isn't visible, only accept CSE hits.
        cache_scope=CacheScope.CSE,
        **entrypoint_config,
    )

    return scheduler.evaluate(subrun_task, parent_job=parent_job)
