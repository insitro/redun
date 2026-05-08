import importlib
import typing
from collections.abc import Callable, Iterator
from typing import Any, Optional, cast

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

    def supports_async(self) -> bool:
        """
        Returns True if Executor supports async tasks.
        """
        return False

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
            job,
            ExecutorError("Executor {} does not support submitting tasks.".format(type(self))),
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


class _ExecutorProvider:
    """Pointer to an executor that we don't want to import until it's actually used."""

    _executor_class: type[Executor] | str

    def __init__(self, executor_class: type[Executor] | str):
        self._executor_class = executor_class

    @property
    def executor_class(self) -> type[Executor]:
        if isinstance(self._executor_class, str):
            module_name, class_name = self._executor_class.rsplit(".", 1)
            module = importlib.import_module(module_name)
            executor_class = getattr(module, class_name)
            if not issubclass(executor_class, Executor):
                raise ExecutorError(
                    f"Class '{module_name}.{class_name}' is not a subclass of Executor."
                )
            self._executor_class = executor_class
        return cast(type[Executor], self._executor_class)

    def __call__(self, *args, **kwargs) -> Executor:
        return self.executor_class(*args, **kwargs)


# Singleton executor registry.
_executor_providers: dict[str, _ExecutorProvider] = {}


def get_executor_class(
    executor_name: str, required: bool = True, class_path: Optional[str] = None
) -> Optional[type[Executor]]:
    """
    Get an Executor by name from the executor registry.

    Parameters
    ----------
    executor_name : str
        Name of executor class to retrieve.
    required : bool
        If True, raises error if executor is not registered.
        If False, None is returned for unknown executor name.
    class_path : str, optional
        Fully-qualified class name (e.g. 'mypackage.executors.MyExecutor').
        Used as a fallback to lazily register the executor if it is not already
        in the registry.
    """
    executor_provider = _executor_providers.get(executor_name)

    if not executor_provider and class_path:
        _register_executor(executor_name, class_path)
        executor_provider = _executor_providers.get(executor_name)

    if not executor_provider:
        if required:
            raise ExecutorError(
                f"Unknown executor '{executor_name}'. "
                f"Custom executors must be registered before the Scheduler is created. "
                f"Either add 'class = your.module.ClassName' to the executor config, "
                f"or import the executor module in your setup_scheduler function."
            )
        return None

    try:
        return executor_provider.executor_class
    except (ImportError, AttributeError) as e:
        if class_path:
            raise ExecutorError(
                f"Failed to load executor '{executor_name}' from class path '{class_path}': {e}"
            ) from e
        raise


def _register_executor(
    executor_name: str, executor_class: type[Executor] | str
) -> _ExecutorProvider:
    """
    Register an Executor class to be used by the scheduler.
    """
    provider = _ExecutorProvider(executor_class)
    _executor_providers[executor_name] = provider
    return provider


def register_executor(executor_name: str, executor_class_name: Optional[str] = None) -> Callable:
    """
    Register an Executor, either by decorating a class or passing a fully-specified class name.
    The executor will be available to the scheduler under the provided name.

    Note that registered classes are responsible for ensuring their modules get loaded, so this
    decorator is actually run. For example, this can be done by adding them to the redun
    module `__init__.py`.

    Usage:
    >>> @register_executor("my_executor")
    ... class MyExecutor(Executor):
    ...     ...
    or
    >>> register_executor("my_executor", "my_module.MyExecutor")
    """
    if executor_class_name:
        return _register_executor(executor_name, executor_class_name)

    def deco(executor_class: type[Executor]):
        _register_executor(executor_name, executor_class)
        return executor_class

    return deco


def get_executors_from_config(executors_config: dict) -> Iterator[Executor]:
    """
    Instantiate executors defined in an executors config section.
    """
    for executor_name, executor_config in executors_config.items():
        executor_class = cast(
            type[Executor],
            get_executor_class(executor_config["type"], class_path=executor_config.get("class")),
        )
        executor = executor_class(executor_name, config=executor_config)
        yield executor


def get_executor_from_config(executors_config: dict, executor_name: str) -> Executor:
    """Create and return the executor by name. Raise an error if it is not present."""
    for name, executor_config in executors_config.items():
        if name != executor_name:
            continue

        executor_class = cast(
            type[Executor],
            get_executor_class(executor_config["type"], class_path=executor_config.get("class")),
        )
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
