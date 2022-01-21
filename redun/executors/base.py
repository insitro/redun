import importlib
import typing
from typing import Any, Callable, Dict, Iterator, Optional, Tuple, Type, cast

if typing.TYPE_CHECKING:
    from redun.scheduler import Job, Scheduler


class ExecutorError(Exception):
    pass


class Executor:
    def __init__(
        self,
        name: str,
        scheduler: Optional["Scheduler"] = None,
        config=None,
    ):
        self.name = name
        self.scheduler = scheduler

    def log(self, *messages: Any, **kwargs) -> None:
        """
        Display log message through Scheduler.
        """
        assert self.scheduler
        self.scheduler.log(f"Executor[{self.name}]:", *messages, **kwargs)

    def submit(self, job: "Job", args: Tuple, kwargs: dict) -> None:
        assert self.scheduler
        self.scheduler.reject_job(
            job, ExecutorError("Executor {} does not support submitting tasks.".format(type(self)))
        )

    def submit_script(self, job: "Job", args: Tuple, kwargs: dict) -> None:
        assert self.scheduler
        self.scheduler.reject_job(
            job,
            ExecutorError(
                "Executor {} does not support submitting script tasks.".format(type(self))
            ),
        )

    def start(self) -> None:
        pass

    def stop(self) -> None:
        pass


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
