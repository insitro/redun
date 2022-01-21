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

from redun.backends.base import RedunBackend, TagEntityType
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
from redun.handle import Handle
from redun.hashing import hash_eval, hash_struct
from redun.logging import logger as _logger
from redun.promise import Promise
from redun.tags import parse_tag_value
from redun.task import Task, TaskRegistry, get_task_registry, scheduler_task, task
from redun.utils import format_table, iter_nested_value, map_nested_value, trim_string
from redun.value import TypeError as RedunTypeError
from redun.value import Value, get_type_registry

# Globals.
_local = threading.local()

# Constants.
JOB_ACTION_WIDTH = 6  # Width of job action in logs.

Result = TypeVar("Result")


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
sys.settrace = settrace_patch  # type: ignore


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


def get_current_job_namespace(required=True) -> str:
    """
    Returns the namespace of the current running job or ''.
    """
    scheduler = get_current_scheduler(required=required)
    if scheduler:
        job = scheduler.get_current_job()
        if job:
            return job.task.namespace
    return ""


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

    def __init__(self, id: str):
        self.id = id
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
        expr: TaskExpression,
        parent_job: Optional["Job"] = None,
        execution: Optional[Execution] = None,
    ):
        self.id = str(uuid.uuid4())
        self.expr = expr
        self.args = expr.args
        self.kwargs = expr.kwargs

        # Job-level task option overrides.
        self.task_options: Dict[str, Any] = {}

        # Execution state.
        self.execution: Optional[Execution] = execution
        self.task_name: str = self.expr.task_name
        self.task: Optional[Task] = None
        self.args_hash: Optional[str] = None
        self.eval_args: Optional[Tuple[Tuple, Dict]] = None
        self.eval_hash: Optional[str] = None
        self.was_cached: bool = False
        self.call_hash: Optional[str] = None
        self.result_promise: Promise = Promise()
        self.result: Any = None
        self.child_jobs: List[Job] = []
        self.parent_job: Optional[Job] = parent_job
        self.handle_forks: Dict[str, int] = defaultdict(int)
        self.job_tags: List[Tuple[str, Any]] = []
        self.value_tags: List[Tuple[str, List[Tuple[str, Any]]]] = []
        self._status: Optional[str] = None

        if parent_job:
            self.notify_parent(parent_job)
        if execution:
            execution.add_job(self)

    def __repr__(self) -> str:
        return f"Job(id={self.id}, task_name={self.task_name})"

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

    def get_option(self, key: str, default: Any = None) -> Any:
        """
        Returns a task option associated with a :class:`Job`.

        Precedence is given to task options defined at call-time
        (e.g. `task.options(option=foo)(arg1, arg2)`) over task definition-time
        (e.g. `@task(option=foo)`).
        """
        assert "task_expr_options" in self.expr.__dict__
        task = cast(Task, self.task)

        if key in self.task_options:
            return self.task_options[key]
        elif key in self.expr.task_expr_options:
            return self.expr.task_expr_options[key]
        elif task.has_task_option(key):
            return task.get_task_option(key)
        else:
            return default

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

    def notify_parent(self, parent_job: "Job") -> None:
        """
        Maintains the Job tree but connecting the job with a parent job.
        """
        parent_job.child_jobs.append(self)

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

        self.expr = None
        self.args = None
        self.kwargs = None
        self.eval_args = None
        self.result_promise = None
        self.result = None
        self.job_tags.clear()
        self.value_tags.clear()

        for child_job in self.child_jobs:
            child_job.parent_job = None
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
        if self.line.strip().startswith("@"):
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
    assert task
    args, kwargs = set_arg_defaults(task, expr.args, expr.kwargs)
    return any(isinstance(arg, Expression) for arg in iter_nested_value((args, kwargs)))


class Scheduler:
    """
    Scheduler for evaluating redun tasks.
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
        self._pending_expr: Dict[str, Tuple[Promise, Job]] = {}
        self._jobs: Set[Job] = set()
        self._finalized_jobs: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
        self.dryrun = False
        self.use_cache = True

    def add_executor(self, executor: Executor) -> None:
        """
        Add executor to scheduler.
        """
        self.executors[executor.name] = executor
        executor.scheduler = self

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

    @overload
    def run(
        self,
        expr: Expression[Result],
        exec_argv: Optional[List[str]] = None,
        dryrun: bool = False,
        cache: bool = True,
        tags: Iterable[Tuple[str, Any]] = (),
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
    ) -> Result:
        pass

    def run(
        self,
        expr: Union[Expression[Result], Result],
        exec_argv: Optional[List[str]] = None,
        dryrun: bool = False,
        cache: bool = True,
        tags: Iterable[Tuple[str, Any]] = (),
    ) -> Result:
        """
        Run the scheduler to evaluate a Task or Expression.
        """
        if needs_root_task(self.task_registry, expr):
            # Ensure we always have one root-level job encompassing the whole execution.
            expr = root_task(quote(expr))

        self._validate_tasks()
        self.backend.calc_current_nodes({task.hash for task in self.task_registry})

        # Set scheduler and start executors.
        set_current_scheduler(self)
        for executor in self.executors.values():
            executor.start()
        self.dryrun = dryrun
        self.use_cache = cache
        self.traceback = None

        # Start execution.
        if exec_argv is None:
            exec_argv = ["scheduler.run", trim_string(repr(expr))]
        self._current_execution = Execution(self.backend.record_execution(exec_argv))
        self.backend.record_tags(
            TagEntityType.Execution, self._current_execution.id, chain(self._exec_tags, tags)
        )

        self.log(
            "Start Execution {exec_id}:  redun {argv}".format(
                exec_id=self._current_execution.id,
                argv=" ".join(map(shlex.quote, exec_argv[1:])),
            )
        )

        # Start event loop to evaluate expression.
        start_time = time.time()
        self.thread_id = threading.get_ident()
        self._pending_expr.clear()
        self._jobs.clear()
        result = self.evaluate(expr)
        self.process_events(result)

        # Log execution duration.
        duration = time.time() - start_time
        self.log(f"Execution duration: {duration:.2f} seconds")

        # Stop executors and unset scheduler.
        for executor in self.executors.values():
            executor.stop()
        set_current_scheduler(None)

        # Return or raise result depending on whether it succeeded.
        if result.is_fulfilled:
            return result.value

        elif result.is_rejected:
            if self.traceback:
                # Log traceback.
                self.log("*** Execution failed. Traceback (most recent task last):")
                for line in self.traceback.format():
                    self.log(line.rstrip("\n"))

            raise result.value

        elif result.is_pending and self.dryrun:
            self.log("Dryrun: Additional jobs would run.")
            raise DryRunResult()

        else:
            raise AssertionError("Unexpected state")

    def process_events(self, workflow_promise: Promise) -> None:
        """
        Main scheduler event loop for evaluating the current expression.
        """
        self.workflow_promise = workflow_promise

        while self.workflow_promise.is_pending:
            if self.dryrun and self.events_queue.empty():
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
            status_counts[job.task_name][job.status] += 1
            status_counts[job.task_name]["TOTAL"] += 1
        for task_name, task_status_counts in self._finalized_jobs.items():
            for status, count in task_status_counts.items():
                status_counts[task_name][status] += count
                status_counts[task_name]["TOTAL"] += count

        # Create counts table.
        task_names = sorted(status_counts.keys())
        table: List[List[str]] = (
            [["TASK"] + Job.STATUSES]
            + [
                ["ALL"]
                + [
                    str(sum(status_counts[task_name][status] for task_name in task_names))
                    for status in Job.STATUSES
                ]
            ]
            + [
                [task_name] + [str(status_counts[task_name][status]) for status in Job.STATUSES]
                for task_name in task_names
            ]
        )

        now = datetime.datetime.now()
        report_lines: List[str] = []
        report_lines.append("| JOB STATUS {}".format(now.strftime("%Y/%m/%d %H:%M:%S")))

        for line in format_table(table, "lrrrrrr", min_width=7):
            report_lines.append("| " + line)
        return report_lines

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
        Begin an evaluation of an expression (concrete value or Expression).

        Returns a Promise that will resolve when the evaluation is complete.
        """

        def eval_term(value):
            # Evaluate one term of an expression.
            if isinstance(value, ValueExpression):
                return value.value
            elif isinstance(value, ApplyExpression):
                return self.evaluate_apply(value, parent_job=parent_job)
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

    def _check_pending_expr(
        self, expr: TaskExpression, parent_job: Optional[Job]
    ) -> Optional[Promise]:
        """
        Returns a promise for an expression currently pending evaluation.

        This check is necessary to avoid double evaluating an expression that is
        not in the cache yet since it is pending.

        This is a form of common subexpression elimination.
        https://en.wikipedia.org/wiki/Common_subexpression_elimination
        """

        # Check pending evaluation cache for whether this expression is being evaluated already.
        promise_job = self._pending_expr.get(expr.get_hash())
        if not promise_job:
            return None

        # We need to notify parent_job of this new child job.
        # This is used to calculate proper call_hash.
        # TODO: Decide whether to record multiple parents. Jobs can form a DAG too.
        promise, job2 = promise_job
        if parent_job:
            job2.notify_parent(parent_job)

        # Wait for other job to finish, and then copy call_hash to this expression.
        def callback(result: Any) -> Any:
            expr.call_hash = job2.call_hash
            return result

        return promise.then(callback)

    def evaluate_apply(self, expr: ApplyExpression, parent_job: Optional[Job] = None) -> Promise:
        """
        Begin an evaluation of an ApplyExpression.

        Returns a Promise that will resolve/reject when the evaluation is complete.
        """
        if isinstance(expr, SchedulerExpression):
            task = self.task_registry.get(expr.task_name)
            if not task:
                return Promise(
                    lambda resolve, reject: reject(
                        NotImplementedError(
                            "Scheduler task '{}' does not exist".format(expr.task_name)
                        )
                    )
                )

            # Scheduler tasks are executed with access to scheduler, parent_job, and
            # raw args (i.e. possibly unevaluated).
            return task.func(self, parent_job, expr, *expr.args, **expr.kwargs)

        elif isinstance(expr, TaskExpression):
            # Reuse promise if expression is already pending evaluation.
            pending_promise = self._check_pending_expr(expr, parent_job)
            if pending_promise:
                return pending_promise

            # TaskExpressions need to be executed in a new Job.
            job = Job(expr, parent_job=parent_job, execution=self._current_execution)
            self._jobs.add(job)

            # Evaluate task_name to specific task.
            # TaskRegistry is like an environment.
            job.task = self.task_registry.get(expr.task_name)
            if not job.task:
                raise AssertionError(
                    f"Task not found in registry: {expr.task_name}.  This can occur if the "
                    "script that defines a user task wasn't loaded or, in the case of redun "
                    "tasks, if an executor is loading a different version of redun that fails to "
                    "define the task"
                )

            # Make default arguments explicit in case they need to be evaluated.
            expr_args = set_arg_defaults(job.task, expr.args, expr.kwargs)

            # Evaluate args then execute job.
            args_promise = self.evaluate(expr_args, parent_job=parent_job)
            promise = args_promise.then(lambda eval_args: self.exec_job(job, eval_args))

            # Record pending expression.
            self._pending_expr[expr.get_hash()] = (promise, job)
            return promise

        elif isinstance(expr, SimpleExpression):
            # Simple Expressions can be executed synchronously.
            func = get_lazy_operation(expr.func_name)
            if not func:
                return Promise(
                    lambda resolve, reject: reject(
                        NotImplementedError(
                            "Expression function '{}' does not exist".format(expr.func_name)
                        )
                    )
                )

            # Evaluate args, apply func, evaluate result.
            args_promise = self.evaluate((expr.args, expr.kwargs), parent_job=parent_job)
            return args_promise.then(
                lambda eval_args: self.evaluate(
                    cast(Callable, func)(*eval_args[0], **eval_args[1]), parent_job=parent_job
                )
            )

        else:
            raise NotImplementedError("Unknown expression: {}".format(expr))

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

        for job, eval_args in ready_jobs:
            self.exec_job(job, eval_args)

    def exec_job(self, job: Job, eval_args: Tuple[Tuple, dict]) -> Promise:
        """
        Execute a job that is ready using the fully evaluated arguments.
        """
        self.events_queue.put(lambda: self._exec_job(job, eval_args))
        return job.result_promise

    def _exec_job(self, job: Job, eval_args: Tuple[Tuple, dict]) -> None:
        """
        Execute a job that is ready using the fully evaluated arguments.

        This function runs on the main scheduler thread. Use
        :method:`Scheduler.exec_job()` if calling from another thread.
        """

        # Ensure we are on main scheduler thread.
        assert self.thread_id == threading.get_ident()
        assert job.task

        # Make sure the job can "fit" within the available resource limits.
        job_limits = job.get_limits()
        if not self._is_job_within_limits(job_limits):
            self._add_job_pending_limits(job, eval_args)
            return
        self._consume_resources(job_limits)

        # Set eval_args on job.
        job.eval_args = eval_args

        # Set job caching preference.
        if not self.use_cache:
            job.task_options["cache"] = False

        self.backend.record_job_start(job)

        # Preprocess arguments before sending them to task function.
        args, kwargs = job.eval_args
        args, kwargs = self.preprocess_args(job, args, kwargs)

        # Check cache using eval_hash as key.
        job.eval_hash, job.args_hash = self.get_eval_hash(job.task, args, kwargs)

        call_hash: Optional[str]
        if self.use_cache and job.get_option("cache", True):
            result, job.was_cached, call_hash = self.get_cache(
                job,
                job.eval_hash,
                job.task.hash,
                job.args_hash,
                check_valid=job.get_option("check_valid", "full"),
            )
        else:
            result, job.was_cached, call_hash = None, False, None
        if job.was_cached:
            # Evaluation was cached, so we proceed to done_job/resolve_job.
            self.log(
                "{action} Job {job_id}:  {task_call} (eval_hash={eval_hash}{check_valid})".format(
                    job_id=job.id[:8],
                    action="Cached".ljust(JOB_ACTION_WIDTH),
                    task_call=format_task_call(job.task, args, kwargs),
                    eval_hash=job.eval_hash[:8],
                    check_valid=", check_valid=shallow" if call_hash else "",
                )
            )
            if call_hash:
                # We have a fully resolved result.
                return self._resolve_job(job, result, call_hash)
            else:
                # We have a result that needs further evaluation.
                return self._done_job(job, result)

        # Perform rollbacks due to Handles that conflict with past Handles.
        if not self.dryrun:
            self.perform_rollbacks(args, kwargs)

        # Determine executor.
        executor_name = job.get_option("executor") or "default"
        executor = self.executors.get(executor_name)
        if not executor:
            return self._reject_job(
                job, SchedulerError('Unknown executor "{}"'.format(executor_name))
            )

        self.log(
            "{action} Job {job_id}:  {task_call} on {executor}".format(
                job_id=job.id[:8],
                action=("Dryrun" if self.dryrun else "Run").ljust(JOB_ACTION_WIDTH),
                task_call=format_task_call(job.task, args, kwargs),
                executor=executor_name,
            )
        )

        # Stop short of submitting jobs during a dryrun.
        if self.dryrun:
            return

        # Submit job.
        if not job.task.script:
            executor.submit(job, args, kwargs)
        else:
            executor.submit_script(job, args, kwargs)

    def done_job(self, job: Job, result: Any, job_tags: List[Tuple[str, Any]] = []) -> None:
        """
        Mark a :class:`Job` as successfully done with a result.
        """
        self.events_queue.put(lambda: self._done_job(job, result, job_tags=job_tags))

    def _done_job(self, job: Job, result: Any, job_tags: List[Tuple[str, Any]] = []) -> None:
        """
        Mark a :class:`Job` as successfully done with a `result`.

        result might require additional evaluation.

        This function runs on the main scheduler thread. Use
        :method:`Scheduler.done_job()` if calling from another thread.
        """

        # Ensure we are on main scheduler thread.
        assert self.thread_id == threading.get_ident()

        self._release_resources(job.get_limits())
        self._check_jobs_pending_limits()

        assert job.task
        assert job.eval_hash
        assert job.eval_args
        assert job.args_hash

        job.job_tags.extend(job_tags)

        # Update cache.
        if not job.was_cached:
            assert not self.dryrun
            result = self.postprocess_result(job, result, job.eval_hash)
            self.set_cache(job.eval_hash, job.task.hash, job.args_hash, result)

        # Clear pending expression lookup.
        if job.expr.get_hash() in self._pending_expr:
            del self._pending_expr[job.expr.get_hash()]

        # Eval result (child tasks), then resolve job.
        self.evaluate(result, parent_job=job).then(
            lambda result: self.resolve_job(job, result)
        ).catch(lambda error: self.reject_job(job, error))

    def resolve_job(self, job: Job, result: Any, call_hash: Optional[str] = None) -> None:
        """
        Resolve a :class:`Job` with a fully evaluated result.

        This occurs after a job is done and the result is fully evaluated.
        """
        self.events_queue.put(lambda: self._resolve_job(job, result, call_hash=call_hash))

    def _resolve_job(self, job: Job, result: Any, call_hash: Optional[str] = None) -> None:
        """
        Resolve a :class:`Job` with a fully evaluated result.

        This occurs after a job is done and the result is fully evaluated.
        This function runs on the main scheduler thread. Use
        :method:`Scheduler.resolve_job()` if calling from another thread.
        """
        # Ensure we are on main scheduler thread.
        assert self.thread_id == threading.get_ident()

        assert job.task
        assert job.args_hash
        assert job.eval_args

        if call_hash:
            # CallNode is already recorded, use it again for this job.
            job.call_hash = call_hash
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
        self._finalized_jobs[job.task_name][job.status] += 1

    def _record_job_tags(self, job: Job) -> None:
        """
        Record tags acquired during Job.
        """
        assert job.task

        # Record value tags.
        for value_hash, tags in job.value_tags:
            self.backend.record_tags(
                entity_type=TagEntityType.Value, entity_id=value_hash, tags=tags
            )

        # Record Job tags.
        job_tags = job.get_option("tags", []) + job.job_tags
        if job_tags:
            self.backend.record_tags(
                entity_type=TagEntityType.Job, entity_id=job.id, tags=job_tags
            )

        # Record Task tags.
        task_tags = job.task.get_task_option("tags")
        if task_tags:
            self.backend.record_tags(
                entity_type=TagEntityType.Task, entity_id=job.task.hash, tags=task_tags
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
        """
        self.events_queue.put(
            lambda: self._reject_job(
                job, error, error_traceback=error_traceback, job_tags=job_tags
            )
        )

    def _reject_job(
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

    def get_eval_hash(self, task: Task, args: tuple, kwargs: dict) -> Tuple[str, str]:
        """
        Compute eval_hash and args_hash for a task call, filtering out config args before hashing.
        """
        # Filter out config args from args and kwargs.
        sig = task.signature
        config_args: List = task.get_task_option("config_args", [])

        # Determine the variadic parameter if it exists.
        var_param_name: Optional[str] = None
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
            arg_value
            for arg_value in args[len(sig.parameters) :]
            if var_param_name not in config_args
        )

        # Filter kwargs.
        kwargs2 = {
            arg_name: arg_value
            for arg_name, arg_value in kwargs.items()
            if arg_name not in config_args
        }

        return hash_eval(self.type_registry, task.hash, args2, kwargs2)

    def get_cache(
        self, job: Job, eval_hash: str, task_hash: str, args_hash: str, check_valid: str = "full"
    ) -> Tuple[Any, bool, Optional[str]]:
        """
        Attempt to find a cached value for an evaluation (`eval_hash`).

        Returns
        -------
        (result, is_cached, call_hash): Tuple[Any, bool, Optional[str]]
           is_cached is True if `result` was in the cache.
        """
        assert check_valid in {"full", "shallow"}

        if check_valid == "full":
            result, is_cached = self.backend.get_eval_cache(eval_hash)
            call_hash: Optional[str] = None

        else:
            # See if the full call is cached and valid.
            call_hash = self.backend.get_call_hash(task_hash, args_hash)
            if call_hash:
                try:
                    result, is_cached = self.backend.get_cache(call_hash)
                except RedunTypeError:
                    # Type no longer exists, we can't use shallow checking.
                    result, is_cached = self.backend.get_eval_cache(eval_hash)
            else:
                result, is_cached = self.backend.get_eval_cache(eval_hash)

        if isinstance(result, ErrorValue):
            # Errors can't be used from the cache.
            result, is_cached = None, False

        if self.dryrun and not is_cached:
            # In dryrun mode, log reason for cache miss.
            self._log_cache_miss(job, task_hash, args_hash)

        if self.is_valid_value(result):
            return result, is_cached, call_hash
        else:
            self.log(
                "{action} Job {job}:  Cached result is no longer valid "
                "(result={result}, eval_hash={eval_hash}).".format(
                    action="Miss".ljust(JOB_ACTION_WIDTH),
                    job=job.id[:8],
                    result=trim_string(repr(result)),
                    eval_hash=eval_hash[:8],
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
        """
        self.backend.set_eval_cache(eval_hash, task_hash, args_hash, value, value_hash=None)

    def is_valid_value(self, value: Any) -> bool:
        """
        Returns True if the value is valid.

        Valid Nested Values must have all of their subvalues be valid.
        """
        return self.type_registry.is_valid_nested(value)

    def perform_rollbacks(self, args: Tuple, kwargs: dict) -> None:
        """
        Perform any Handle rollbacks needed for the given Task arguments.
        """
        for value in iter_nested_value((args, kwargs)):
            if isinstance(value, Handle):
                self.backend.rollback_handle(value)

    def preprocess_args(self, job: Job, args: Tuple, kwargs: dict) -> Any:
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

    def postprocess_result(self, job: Job, result: Any, pre_call_hash: str) -> Any:
        """
        Postprocess a result from a Task before caching.
        """
        postprocess_args = {
            "pre_call_hash": pre_call_hash,
        }

        def postprocess_value(value):
            value2 = self.type_registry.postprocess(value, postprocess_args)

            if isinstance(value, Handle) and not self.dryrun:
                # Handles accumulate state change from jobs that emit them.
                assert value2 != value
                self.backend.advance_handle([value], value2)

            return value2

        return map_nested_value(postprocess_value, result)

    def get_current_job(self):
        # TODO
        return None


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

    # Note: In redun, we cache only one round of evaluation, `expr` and `recover_expr`,
    # instead of the final result, `result`. This approach allows the scheduler
    # to retrace the call graph and detect if any subtasks have changed (i.e. hash change)
    # or if any Values in the subworkflow are now invalid (e.g. File hash has changed).

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
    eval_hash, args_hash = scheduler.get_eval_hash(catch, catch_args, {})
    if scheduler.use_cache:
        cached_expr, is_cached = scheduler.backend.get_eval_cache(eval_hash)
        if is_cached:
            return scheduler.evaluate(cached_expr, parent_job=parent_job).catch(promise_catch)

    return scheduler.evaluate(expr, parent_job=parent_job).then(on_success, promise_catch)


@scheduler_task(namespace="redun")
def apply_tags(
    scheduler: Scheduler,
    parent_job: Job,
    sexpr: SchedulerExpression,
    value: Any,
    tags: List[Tuple[str, Any]],
) -> Promise:
    """
    Apply tags to a value.

    Returns the original value.
    """

    def then(args):
        value, tags = args
        value_hash = scheduler.backend.record_value(value)
        parent_job.value_tags.append((value_hash, tags))
        return value

    return scheduler.evaluate((value, tags), parent_job=parent_job).then(then)


@task(
    namespace="redun",
    name="subrun_root_task",
    version="1",
    config_args=["config", "load_modules", "run_config"],
)
def _subrun_root_task(
    expr: Any,
    config: Dict[str, Dict],
    load_modules: List[str],
    run_config: Dict[str, Any],
) -> Any:
    """
    Launches a sub-scheduler and runs the provided expression by first "unwrapping" it.
    The evaluated result is returned within a dict alongside other sub-scheduler-related
    state to the caller.

    Parameters
    ----------
    expr: TaskExpression
        TaskExpression to be run by sub-scheduler.
    config
        A two-level python dict to configure the sub-scheduler.  (Will be used to initialize a
        :class:`Config` object via the `config_dict` kwarg.)
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
          'result': sub-scheduler evaluation of `expr`
          'status': sub-scheduler final job status poll (list of str)
          'config': sub-scheduler config dict (useful for confirming exactly what config
            settings were used).
        }
    """
    from redun.functools import force

    sub_scheduler = Scheduler(config=Config(config_dict=config))
    sub_scheduler.load()

    # Import user modules.  The user is responsible for ensuring that the Executor that runs this
    # task has access to all these modules' code.  Additionally, any other user modules imported
    # by these modules must also be accessible.
    if load_modules:
        for module in load_modules:
            importlib.import_module(module)

    result = sub_scheduler.run(force(expr), **run_config)
    return {
        "result": result,
        "status": sub_scheduler.get_job_status_report(),
        "config": sub_scheduler.config.get_config_dict(),
        "run_config": {
            "dryrun": sub_scheduler.dryrun,
            "cache": sub_scheduler.use_cache,
        },
    }


@scheduler_task(namespace="redun", version="1")
def subrun(
    scheduler: Scheduler,
    parent_job: Job,
    sexpr: SchedulerExpression,
    expr: Any,
    executor: str,
    config: Optional[Dict[str, Any]] = None,
    **task_options: dict,
) -> Promise:
    """
    Evaluate an expression `expr` in a sub-scheduler.

    subrun() is a scheduler_task that evaluates a `_subrun_root_task`, which in turn launches the
    sub-scheduler.

    `expr` is the TaskExpression that the sub-scheduler will evaluate.

    `executor` and optional `task_options` are used to configure the special redun task (
    _subrun_root_task) that starts the sub-scheduler. For example, you can configure the task
    with a batch executor to run the sub-scheduler on AWS Batch.

    `config`: To ease configuration management of the sub-scheduler, you can pass a `config`
    dict which contains configuration that would otherwise require a redun.ini file in the
    sub-scheduler environment.
        WARNING: Do not include database credentials in this config as they will be
    logged as clear text in the redun call graph.  You should instead specify a database secret
    (see `db_aws_secret_name`).
        If you do not pass a config, the local scheduler's config will be forwarded to the
    sub-scheduler (replacing the local `config_dir` with "."). In practice, the sub-scheduler's
    `config_dir` should be less important as you probably want to log both local and sub-scheduler
    call graphs to a common database.
        You can also obtain a copy of the local scheduler's config and customize it as needed.
    Instantiate the scheduler directly instead of calling `redun run`.  Then access its
    connfig via `scheduler.py::get_scheduler_config_dict()`.

    Note on code packaging: The user is responsible for ensuring that the chosen Executor for
    invoking the sub-scheduler copies over all user-task scripts.  E.g. the local scheduler may
    be launched on local tasks defined in workflow1.py but subrun(executor="batch) is invoked on
    taskX defined in workflow2.py.  In this case, the user must ensure workflow2.py is copied to
    the batch node by placing it within the same directory tree as workflow1.py.

    Parameters
    ----------
    expr
        Expression to be run by sub-scheduler.
    executor
        Executor name for the special redun task that launches the sub-scheduler
        (_subrun_root_task). E.g. `batch` to launch the sub-scheduler in AWS Batch. Note
        that this is a config key in the  *local* scheduler's config.
    config
        Optional sub-scheduler config dict. Must be a two-level dict that can be used to
        initialize a :class:`Config` object [see :method:`Config.get_config_dict()`.  If None or
        empty, the local Scheduler's config will be passed to the sub-scheduler and any values
        with local config_dir will be replaced with ".".  Do not include database credentials as
        they will be logged as clear text in the call graph.
    task_options
        Task options for _subrun_root_task.  E.g. when running the sub-scheduler via Batch
        executor, you can pass ECS configuration (e.g. `memory`, `vcpus`, `batch_tags`).

    Returns
    -------
    Dict[str, Any]
        Returns a dict result of evaluating the _subrun_root_task
    """
    from redun.functools import delay

    # If a config wasn't provided, forward the parent scheduler's backend config to the
    # sub-scheduler, replacing the local config_dir with "."
    if not config:
        config = scheduler.config.get_config_dict(replace_config_dir=".")

    # Collect a list of all modules that define user task.  This list will include the initial
    # task(s) to be subrun() as these are registered in the local task registry during subrun()
    # argument formulation.
    registry = get_task_registry()
    load_modules = set()
    for _task in registry:
        module_name = _task.load_module
        if module_name.startswith("redun.") and not module_name.startswith("redun.tests."):
            continue
        load_modules.add(module_name)

    subrun_root_task_expr = _subrun_root_task.options(executor=executor, **task_options)(
        expr=delay(expr),
        config=config,
        load_modules=sorted(load_modules),
        run_config={
            "dryrun": scheduler.dryrun,
            "cache": scheduler.use_cache,
        },
    )

    def log_banner(msg):
        scheduler.log("-" * 79)
        scheduler.log(msg)
        scheduler.log("-" * 79)

    def then(subrun_result):
        # Echo subs-scheduler report.
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

        # Return the user's result.
        return subrun_result["result"]

    return scheduler.evaluate(subrun_root_task_expr, parent_job=parent_job).then(then)
