import importlib
import typing
from typing import Any, Callable, Dict, Iterator, Optional, Type, cast

if typing.TYPE_CHECKING:
    from redun.scheduler import Job, Scheduler


class ExecutorError(Exception):
    pass


class Executor:
    """Note that most Executors should Register themselves with the method `register_executor`,
    below."""

    def __init__(
        self,
        name: str,
        scheduler: Optional["Scheduler"] = None,
        config=None,
    ):
        self.name = name
        self._scheduler = scheduler

    def set_scheduler(self, scheduler: "Scheduler") -> None:
        self._scheduler = scheduler

    def log(self, *messages: Any, **kwargs) -> None:
        """
        Display log message through Scheduler.
        """
        assert self._scheduler
        self._scheduler.log(f"Executor[{self.name}]:", *messages, **kwargs)

    def submit(self, job: "Job") -> None:
        """Execute the provided job.

        Implementations must provide results back to the scheduler by either calling `done_job` or
        `reject_job`."""
        assert self._scheduler
        self._scheduler.reject_job(
            job, ExecutorError("Executor {} does not support submitting tasks.".format(type(self)))
        )

    def submit_script(self, job: "Job") -> None:
        """Execute the provided script job.

        Implementations must provide results back to the scheduler by either calling `done_job` or
        `reject_job`."""

        assert self._scheduler
        self._scheduler.reject_job(
            job,
            ExecutorError(
                "Executor {} does not support submitting script tasks.".format(type(self))
            ),
        )

    def start(self) -> None:
        pass

    def stop(self) -> None:
        pass

    def scratch_root(self) -> str:
        raise NotImplementedError()


# Singleton executor registry.
_executor_classes: Dict[str, Type[Executor]] = {}


def get_executor_class(executor_name: str, required: bool = True) -> Optional[Type[Executor]]:
    """
    Get an Executor by name from the executor registry.

    Parameters
    ----------
    executor_name : str
        Name of executor class to retrieve.
    required : bool
        If True, raises error if executor is not registered.
        If False, None is returned for unknown executor name.
    """
    executor_class = _executor_classes.get(executor_name)
    if required and not executor_class:
        raise ExecutorError("Unknown executor {}".format(executor_name))
    return executor_class


def _register_executor(executor_name: str, executor_class: Type[Executor]) -> None:
    """
    Register an Executor class to be used by the scheduler.
    """
    _executor_classes[executor_name] = executor_class


def register_executor(executor_name: str) -> Callable:
    """
    Register an Executor class to be used by the scheduler.

    Note that registered classes are responsible for ensuring their modules get loaded, so this
    decorator is actually run. For example, this can be done by adding them to the redun
    module `__init__.py`.
    """

    def deco(executor_class: Type[Executor]):
        _register_executor(executor_name, executor_class)
        return executor_class

    return deco


def get_executors_from_config(executors_config: dict) -> Iterator[Executor]:
    """
    Instantiate executors defined in an executors config section.
    """
    for executor_name, executor_config in executors_config.items():
        executor_class = cast(Type[Executor], get_executor_class(executor_config["type"]))
        executor = executor_class(executor_name, config=executor_config)
        yield executor


def get_executor_from_config(executors_config: dict, executor_name: str) -> Executor:
    """Create and return the executor by name. Raise an error if it is not present."""
    for name, executor_config in executors_config.items():
        if name != executor_name:
            continue

        executor_class = cast(Type[Executor], get_executor_class(executor_config["type"]))
        executor = executor_class(executor_name, config=executor_config)
        return executor
    raise RuntimeError(f"Unknown Executor {executor_name}.")


def load_task_module(module_name: str, task_name: str) -> None:
    """
    Helper method that Executors may call to load a task's module code.

    Presently, the main benefit is this method provides better error handling.

    Args:
          module_name: Task module name.
          task_name: Task name
    """
    try:
        importlib.import_module(module_name)
    except ModuleNotFoundError as exc:
        # Append user-friendly error to original exception
        raise ModuleNotFoundError(
            f"Failed to find module {module_name} for task {task_name}. "
            "If this is remotely executed, ensure module is included in code packaging."
        ) from exc
