import threading
from configparser import SectionProxy
from typing import Any, Dict, List, Optional, Tuple

from redun.executors import gcp_utils
from redun.executors.base import Executor, register_executor
from redun.executors.code_packaging import package_code, parse_code_package_config
from redun.executors.command import get_oneshot_command, get_script_task_command
from redun.executors.scratch import get_job_scratch_dir
from redun.file import File
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
        self.image = config["image"]

        # Optional config
        self.code_package = parse_code_package_config(config)
        self.code_file: Optional[File] = None

        # Monitoring internals
        self.is_running = False
        self._thread: Optional[threading.Thread] = None

        # Default task options.
        self.default_task_options: Dict[str, Any] = {
            "mount_path": config.get("mount_path", fallback="/mnt/share"),
            "machine_type": config.get("machine_type", fallback="e2-standard-4"),
            "vcpus": config.getint("vcpus", fallback=2),
            "memory": config.getint("memory", fallback=16),
            "task_count": config.getint("task_count", fallback=1),
            "max_duration": config.get("max_duration", "60s"),
            "retries": config.getint("retries", fallback=2),
            "priority": config.getint("priority", fallback=30),
            # "job_name_prefix": config.get("job_name_prefix", fallback="redun-job"),
        }

    def submit(self, job: Job, args: Tuple, kwargs: dict) -> None:
        """
        Submit Job to executor.
        """
        assert job.task
        assert not job.task.script

        self._submit(job, args, kwargs)

    def submit_script(self, job: Job, args: Tuple, kwargs: dict) -> None:
        """
        Submit Job for script task to executor.
        """
        assert job.task
        assert job.task.script
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
        # self._start()

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
        assert self.gcs_scratch_prefix

        # Submit a new Batch job.
        gcp_job = None
        if not job.task.script:
            # Package code if necessary and we have not already done so. If code_package
            # is False, then we can skip this step. Additionally, if we have already
            # packaged and set code_file, then we do not need to repackage.
            if self.code_package is not False and self.code_file is None:
                code_package = self.code_package or {}
                assert isinstance(code_package, dict)
                self.code_file = package_code(self.gcs_scratch_prefix, code_package)

            command = get_oneshot_command(
                self.gcs_scratch_prefix, job, job.task, args, kwargs, code_file=self.code_file
            )

            gcp_job = gcp_utils.batch_submit(
                job_name=f"redun-{job.id}",
                project=self.project,
                region=self.region,
                image=self.image,
                command=" ".join(command),
                gcs_bucket=self.gcs_scratch_prefix,
                **self.default_task_options,
            )
        else:
            task_command = get_task_command(job.task, args, kwargs)
            script_command = get_script_task_command(
                self.gcs_scratch_prefix,
                job,
                task_command,
                exit_command="exit 1",
            )
            gcp_job = gcp_utils.batch_submit(
                job_name=f"redun-{job.id}",
                project=self.project,
                region=self.region,
                gcs_bucket=self.gcs_scratch_prefix,
                command=" ".join(script_command),
                **self.default_task_options,
            )

        job_dir = get_job_scratch_dir(self.gcs_scratch_prefix, job)
        job_type = "GCP Batch job"
        self.log(
            "submit redun job {redun_job} as {job_type} {batch_job}:\n"
            "  job_id          = {batch_job}\n"
            "  job_name        = {job_name}\n"
            "  gcs_scratch_path = {job_dir}\n".format(
                redun_job=job.id,
                job_type=job_type,
                batch_job=gcp_job.uid,
                job_name=gcp_job.name,
                job_dir=job_dir,
            )
        )

    def _start(self) -> None:
        """
        Start monitoring thread.
        """
        if not self._thread or not self._thread.is_alive():
            self._thread = threading.Thread(target=self._monitor, daemon=False)
            self._thread.start()

    def stop(self) -> None:
        """
        Stop Executor and monitoring thread.
        """
        self.is_running = False

        # Stop monitor thread.
        if (
            self._thread
            and self._thread.is_alive()
            and threading.get_ident() != self._thread.ident
        ):
            self._thread.join()

    def _monitor(self) -> None:
        """
        Thread for monitoring running GCP Batch jobs.
        """
        assert self._scheduler

        self.is_running = True
