from configparser import SectionProxy
from typing import Dict, List, Optional, Tuple

from redun.executors import gcp_utils
from redun.executors.base import Executor, register_executor
from redun.executors.command import get_script_task_command
from redun.scheduler import Job, Scheduler
from redun.scripting import get_task_command


@register_executor("gcp_batch")
class GCPBatchExecutor(Executor):
    """
    A redun Executor for running jobs on GCP Batch.
    """

    def __init__(
        self,
        name: str,
        scheduler: Optional["Scheduler"] = None,
        config: Optional[SectionProxy] = None,
    ):
        super().__init__(name, scheduler=scheduler)
        if config is None:
            raise ValueError("GCPBatchExecutor requires config.")

        self.debug = config.getboolean("debug", fallback=False)

        # Required config.
        self.project = config["project"]
        self.region = config["region"]
        self.gcs_scratch_prefix = config["gcs_scratch"]

    def submit(self, job: Job, args: Tuple, kwargs: dict) -> None:
        """
        Submit Job to executor.
        """
        self._submit(job, args, kwargs)

    def submit_script(self, job: Job, args: Tuple, kwargs: dict) -> None:
        """
        Submit Job for script task to executor.
        """
        self._submit(job, args, kwargs)

    def _submit(self, job: Job, args: Tuple, kwargs: dict) -> None:
        """
        Submit Job to executor.
        """

        assert self._scheduler
        assert job.task
        assert not self.debug

        # TODO use job array instead of direct call to submit single job
        self._submit_single_job(job, args, kwargs)

    def _submit_array_job(
        self, jobs: List[Job], all_args: List[Tuple], all_kwargs: List[Dict]
    ) -> str:
        """
        Submits an array job, returning job name uuid

        TODO- This will be necessary if we want the JobArrayer to be able to use
        GCPBatchExecutor./
        """
        pass

    def _submit_single_job(self, job: Job, args: Tuple, kwargs: dict) -> None:
        """
        Actually submits a job. Caching detects if it should be part
        of an array job
        """
        assert job.task
        assert self.project
        assert self.region

        # Submit a new Batch job.
        if not job.task.script:
            gcp_utils.batch_submit(
                self.project,
                self.region,
                job_name=job.task_name,
                image="gcr.io/google-containers/busybox",
            )
        else:
            task_command = get_task_command(job.task, args, kwargs)
            script_command = get_script_task_command(
                self.gcs_scratch_prefix,
                job,
                task_command,
                exit_command="exit 1",
            )
            gcp_utils.batch_submit(
                self.project, self.region, job_name=job.id, command=" ".join(script_command)
            )
