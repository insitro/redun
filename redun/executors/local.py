import sys
import typing
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from multiprocessing import get_context, get_start_method, set_start_method
from typing import Any, Callable, Optional, Tuple

from redun.config import create_config_section
from redun.executors.base import Executor, load_task_module, register_executor
from redun.scripting import exec_script, get_task_command
from redun.task import get_task_registry

if typing.TYPE_CHECKING:
    from redun.scheduler import Job, Scheduler


# Modes of local execution.
THREAD_MODE = "thread"
PROCESS_MODE = "process"


def exec_task(mode: str, module_name: str, task_fullname: str, args: Tuple, kwargs: dict) -> Any:
    """
    Execute a task in the new process.
    """
    load_task_module(module_name, task_fullname)
    task = get_task_registry().get(task_fullname)
    return task.func(*args, **kwargs)


def exec_script_task(
    mode: str, module_name: str, task_fullname: str, args: Tuple, kwargs: dict
) -> bytes:
    """
    Execute a script task from the task registry.
    """
    load_task_module(module_name, task_fullname)
    task = get_task_registry().get(task_fullname)
    command = get_task_command(task, args, kwargs)
    return exec_script(command)


@register_executor("local")
class LocalExecutor(Executor):
    """
    A redun Executor for running jobs locally using a thread or process pool.
    """

    # Available local executor modes.
    MODES = [THREAD_MODE, PROCESS_MODE]
    _OLD2NEW_MODES = {
        "threads": THREAD_MODE,
        "processes": PROCESS_MODE,
    }
    START_METHODS = ["fork", "spawn", "forkserver"]
    # start_method fork is not reliable on Mac OS X. So we use forkserver as
    # a safe common default.
    # https://bugs.python.org/issue33725
    DEFAULT_START_METHOD = "forkserver"

    def __init__(
        self,
        name: str,
        scheduler: Optional["Scheduler"] = None,
        config=None,
        mode: str = THREAD_MODE,
    ):
        super().__init__(name, scheduler=scheduler)

        # Parse config.
        if not config:
            config = create_config_section()

        self.max_workers = config.getint("max_workers", 20)
        self.mode = config.get("mode", mode)
        # Autoconvert deprecated modes.
        self.mode = self._OLD2NEW_MODES.get(self.mode, self.mode)
        self._scratch_root = config.get("scratch", "/tmp/redun")
        assert self.mode in self.MODES, f"Unknown mode: {self.mode}"

        self.start_method = config.get("start_method", self.DEFAULT_START_METHOD)
        assert (
            self.start_method in self.START_METHODS
        ), f"Unknown start_method: {self.start_method}"

        # Pools.
        self._thread_executor: Optional[ThreadPoolExecutor] = None
        self._process_executor: Optional[ProcessPoolExecutor] = None

    def _start(self, mode: str) -> None:
        """
        Start pool on first Job submission.
        """
        if mode == THREAD_MODE and not self._thread_executor:
            self._thread_executor = ThreadPoolExecutor(max_workers=self.max_workers)

        if mode == PROCESS_MODE and not self._process_executor:
            if sys.version_info < (3, 7):
                # https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ProcessPoolExecutor
                # Changed in version 3.7: The mp_context argument was added to allow users
                # to control the start_method for worker processes created by the pool.
                # https://github.com/python/cpython/blob/3.6/Lib/multiprocessing/context.py#L240
                if get_start_method(allow_none=True) is None:
                    set_start_method(self.start_method)
                self._process_executor = ProcessPoolExecutor(max_workers=self.max_workers)
            else:
                self._process_executor = ProcessPoolExecutor(
                    max_workers=self.max_workers, mp_context=get_context(self.start_method)
                )

    def stop(self) -> None:
        """
        Stop Executor pools.
        """
        if self._thread_executor:
            self._thread_executor.shutdown()
            self._thread_executor = None

        if self._process_executor:
            # Shutdown causes problems on python3.8
            # https://bugs.python.org/issue39995
            if (sys.version_info.major, sys.version_info.minor) != (3, 8):
                self._process_executor.shutdown()
            self._process_executor = None

    def _submit(self, exec_func: Callable, job: "Job") -> None:
        mode = job.get_option("mode", self.mode)
        if mode not in (THREAD_MODE, PROCESS_MODE):
            raise ValueError(f"Unknown mode: {mode}")

        # Ensure pool are started.
        self._start(mode)

        # Determine pool executor.
        executor = (
            self._thread_executor
            if mode == THREAD_MODE
            else self._process_executor
            if mode == PROCESS_MODE
            else None
        )
        if not executor:
            raise ValueError('Unknown LocalExecutor.mode "{}"'.format(mode))

        # Run job in a new thread or process.

        def on_done(future):
            try:
                self._scheduler.done_job(job, future.result())
            except Exception as error:
                self._scheduler.reject_job(job, error)

        assert job.args
        args, kwargs = job.args
        executor.submit(
            exec_func, mode, job.task.load_module, job.task.fullname, args, kwargs
        ).add_done_callback(on_done)

    def submit(self, job: "Job") -> None:
        assert not job.task.script
        self._submit(exec_task, job)

    def submit_script(self, job: "Job") -> None:
        assert job.task.script
        self._submit(exec_script_task, job)

    def scratch_root(self) -> str:
        return self._scratch_root
