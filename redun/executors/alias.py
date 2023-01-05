from configparser import SectionProxy
from typing import Optional

from redun.executors.base import Executor, ExecutorError, register_executor
from redun.scheduler import Job, Scheduler


@register_executor("alias")
class AliasExecutor(Executor):
    """A simple executor that lazily defers to another one by name. This can be useful when tasks
    are defined with differing executors, allowing them to be configured separately, but
    sometimes need to share the underlying executor; this can be important to enforce per-executor
    resource limits.

    Unfortunately, this implementation isn't able to check validity of the link at init time."""

    def __init__(
        self,
        name: str,
        scheduler: Optional["Scheduler"] = None,
        config: Optional[SectionProxy] = None,
        target: Optional[Executor] = None,
    ):
        """The alias may be created by either providing the `target` to wrap or a `config` object.

        Parameters
        ----------
        name : str
            The name of this executor
        scheduler : Optional["Scheduler"]
            The scheduler to attach to, or may be omitted and provided later.
        config : Optional[SectionProxy] = None
            The config object specifying the name of the target.
        target : Optional[Executor] = None
            The target to alias. We only grab the name, and the alias is still lazily resolved by
            name, so the caller is responsible for ensuring the executor is attached to the
            scheduler.
        """

        super().__init__(name=name, scheduler=scheduler)

        assert (config is not None) ^ (
            target is not None
        ), "Exactly one of `target` or `config` should be provided"

        if config is not None:
            assert "target" in config, "Alias executors must provide the `target` config"
            self.target_name = config["target"]
        else:
            assert target is not None
            self.target_name = target.name

    def _get_target_executor(self) -> Executor:
        assert self._scheduler
        if self.target_name not in self._scheduler.executors:
            raise ExecutorError(
                f"Could not find executor `{self.target_name}` from options: "
                f"{self._scheduler.executors.keys()}"
            )
        return self._scheduler.executors[self.target_name]

    def submit(self, job: Job) -> None:
        self._get_target_executor().submit(job)

    def submit_script(self, job: Job) -> None:
        self._get_target_executor().submit_script(job)

    def start(self) -> None:
        # Do not invoke the aliased method, since the underlying scheduler is responsible for
        # ensuring the target is in the correct state and we do not want to duplicate lifecycle
        # calls.
        pass

    def stop(self) -> None:
        # Do not invoke the aliased method, since the underlying scheduler is responsible for
        # ensuring the target is in the correct state and we do not want to duplicate lifecycle
        # calls.
        pass
