import subprocess
import threading
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple, cast

from redun.executors import aws_utils
from redun.executors.base import Executor, register_executor
from redun.scheduler import Job, Scheduler, Traceback

@register_executor("k8s")
class K8SExecutor(Executor):
    def __init__(self, name: str, scheduler: Optional["Scheduler"] = None, config=None):
        super().__init__(name, scheduler=scheduler)
        if config is None:
            raise ValueError("K8SExecutor requires config.")
        self.is_running = False


    def _monitor(self) -> None:
        pass

    def log(self, *messages: Any, **kwargs) -> None:
        """
        Display log message through Scheduler.
        """
        print("log")
        assert self.scheduler
        self.scheduler.log(f"Executor[{self.name}]:", *messages, **kwargs)

    def _start(self) -> None:
        """
        Start monitoring thread.
        """
        if not self.is_running:
            self._aws_user = aws_utils.get_aws_user()

            self.is_running = True
            self._thread = threading.Thread(target=self._monitor, daemon=False)
            self._thread.start()

    def stop(self) -> None:
        """
        Stop Executor and monitoring thread.
        """
        print("stop")
        self.is_running = False

    def _submit(self, job: Job, args: Tuple, kwargs: dict) -> None:
        """
        Submit Job to executor.
        """
        assert self.scheduler
        assert job.task

    def submit(self, job: Job, args: Tuple, kwargs: dict) -> None:
        """
        Submit Job to executor.
        """
        print("submit")
        return self._submit(job, args, kwargs)

    def submit_script(self, job: Job, args: Tuple, kwargs: dict) -> None:
        """
        Submit Job for script task to executor.
        """
        return self._submit(job, args, kwargs)