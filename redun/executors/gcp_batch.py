import json
import logging
import re
import threading
import time
from collections import OrderedDict
from configparser import SectionProxy
from typing import Any, Dict, List, Optional

from google.cloud.batch_v1 import Job as BatchJob
from google.cloud.batch_v1 import JobStatus

from redun.executors import gcp_utils
from redun.executors.base import Executor, register_executor
from redun.executors.code_packaging import package_code, parse_code_package_config
from redun.executors.command import get_oneshot_command, get_script_task_command
from redun.executors.docker import DockerExecutor, get_docker_executor_config
from redun.executors.scratch import (
    SCRATCH_OUTPUT,
    get_job_scratch_dir,
    get_job_scratch_file,
    parse_job_error,
    parse_job_result,
)
from redun.file import File
from redun.scheduler import Job as RedunJob
from redun.scheduler import Scheduler
from redun.scripting import DEFAULT_SHELL, get_task_command

REDUN_HASH_LABEL_KEY = "redun_hash"
REDUN_JOB_TYPE_LABEL_KEY = "redun_job_type"

DEBUG_SCRATCH = "scratch"


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

        # Use DockerExecutor for local debug mode.
        docker_config = get_docker_executor_config(config)
        docker_config["scratch"] = config.get("debug_scratch", fallback=DEBUG_SCRATCH)
        self._docker_executor = DockerExecutor(name + "_debug", scheduler, config=docker_config)

        self.gcp_client = gcp_utils.get_gcp_client()

        # Required config.
        self.project = config["project"]
        self.region = config["region"]
        self.gcs_scratch_prefix = config["gcs_scratch"]
        self.image = config["image"]

        # Optional config
        self.code_package = parse_code_package_config(config)
        self.code_file: Optional[File] = None

        # Monitoring internals
        self.preexisting_batch_jobs: Dict[str, str] = {}  # RedunJob hash -> BatchJob name
        # We use an OrderedDict in order to retain submission order.
        self.pending_batch_jobs: Dict[str, "RedunJob"] = OrderedDict()
        self.is_running = False
        self.interval = config.getfloat("job_monitor_interval", fallback=5.0)
        self._thread: Optional[threading.Thread] = None

        # Default task options.
        self.default_task_options: Dict[str, Any] = {
            "mount_path": config.get("mount_path", fallback="/mnt/share"),
            "machine_type": config.get("machine_type"),
            "provisioning_model": config.get("provisioning_model", fallback="standard"),
            "vcpus": config.getint("vcpus", fallback=2),
            "gpus": config.getint("gpus", fallback=0),
            "memory": config.getint("memory", fallback=16),
            "task_count": config.getint("task_count", fallback=1),
            "max_duration": config.get("max_duration", "259200s"),  # 3 days in seconds
            "retries": config.getint("retries", fallback=2),
            "priority": config.getint("priority", fallback=30),
            "service_account_email": config.get("service_account_email", fallback=""),
        }
        if config.get("labels"):
            self.default_task_options["labels"] = json.loads(config.get("labels"))
        if config.get("boot_disk_size_gib"):
            self.default_task_options["boot_disk_size_gib"] = config.getint("boot_disk_size_gib")

    def set_scheduler(self, scheduler: Scheduler) -> None:
        super().set_scheduler(scheduler)
        self._docker_executor.set_scheduler(scheduler)

    def _is_debug_job(self, job: RedunJob) -> bool:
        """
        Returns True if job should be sent to debugging DockerExecutor.
        """
        return self.debug or job.get_option("debug", False)

    def gather_inflight_jobs(self) -> None:
        batch_jobs = gcp_utils.list_jobs(
            client=self.gcp_client, project_id=self.project, region=self.region
        )

        for job in batch_jobs:
            # Skip job if it is not in one of the 'inflight' states
            if job.status.state not in [
                JobStatus.State.SCHEDULED,
                JobStatus.State.QUEUED,
                JobStatus.State.RUNNING,
            ]:
                continue

            # If a job is in the inflight state but is not labelled with a redun hash
            # then it is not a redun job
            if REDUN_HASH_LABEL_KEY not in job.labels:
                continue

            job_hash = job.labels[REDUN_HASH_LABEL_KEY]
            self.preexisting_batch_jobs[job_hash] = job.name

    def submit(self, job: RedunJob) -> None:
        """
        Submit RedunJob to executor.
        """
        assert job.task
        assert not job.task.script

        if self._is_debug_job(job):
            return self._docker_executor.submit(job)
        else:
            return self._submit(job)

    def submit_script(self, job: RedunJob) -> None:
        """
        Submit RedunJob for script task to executor.
        """
        assert job.task
        assert job.task.script

        if self._is_debug_job(job):
            return self._docker_executor.submit_script(job)
        else:
            return self._submit(job)

    def _submit(self, job: RedunJob) -> None:
        """
        Submit Job to executor.
        """

        assert self._scheduler
        assert job.task
        assert not self.debug

        # We check is_running here as a way of determining whether this is the first submission
        # or not. If we are already running, then we know we have already had jobs submitted
        # and done the inflight check so no reason to do that again here.
        if not self.is_running:
            # Precompute existing inflight jobs for job reuniting.
            self.gather_inflight_jobs()

        if job.eval_hash in self.preexisting_batch_jobs:
            job_dir = get_job_scratch_dir(self.gcs_scratch_prefix, job)
            job_name = self.preexisting_batch_jobs[job.eval_hash]
            existing_job = gcp_utils.get_job(client=self.gcp_client, job_name=job_name)

            self.log(
                "reunite redun job {redun_job} with {job_type} {batch_job}:\n"
                "  gcs_scratch_path = {job_dir}".format(
                    redun_job=job.id,
                    job_type="GCP Batch Job",
                    batch_job=existing_job.uid,
                    job_dir=job_dir,
                )
            )
            self.pending_batch_jobs[existing_job.name] = job
        else:
            # TODO support array jobs instead of just single jobs
            self._submit_single_job(job)

        self._start()

    def _submit_array_job(self, jobs: List[RedunJob]) -> str:
        """
        Submits an array job, returning job name uuid
        """
        all_args = []
        all_kwargs = []
        for job in jobs:
            assert job.args
            all_args.append(job.args[0])
            all_kwargs.append(job.args[1])

        return ""

    def _get_job_options(self, job: RedunJob) -> dict:
        """
        Determine the task options for a job.

        Task options can be specified at the job-level have precedence over
        the executor-level (within `redun.ini`):
        """
        assert job.eval_hash

        job_options = job.get_options()

        task_options = {
            **self.default_task_options,
            **job_options,
        }

        default_tags = {
            REDUN_HASH_LABEL_KEY: job.eval_hash,
            REDUN_JOB_TYPE_LABEL_KEY: "script" if job.task.script else "container",
        }

        # Merge labels if needed.
        labels: Dict[str, str] = {
            **self.default_task_options.get("labels", {}),
            **job_options.get("labels", {}),
            **default_tags,
        }

        task_options["labels"] = labels

        return task_options

    def _submit_single_job(self, job: RedunJob) -> None:
        """
        Actually submits a job. Caching detects if it should be part
        of an array job
        """
        assert job.task
        assert job.eval_hash
        assert self.project
        assert self.region
        assert self.gcs_scratch_prefix

        assert job.args
        args, kwargs = job.args

        task_options = self._get_job_options(job)
        image = task_options.pop("image", self.image)
        project = task_options.pop("project", self.project)
        region = task_options.pop("region", self.region)

        # Submit a new Batch job.
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
                client=self.gcp_client,
                job_name=f"redun-{job.id}",
                project=project,
                region=region,
                image=image,
                commands=command,
                gcs_scratch_prefix=self.gcs_scratch_prefix,
                **task_options,
            )
        else:
            task_command = get_task_command(job.task, args, kwargs)
            script_command = get_script_task_command(
                self.gcs_scratch_prefix,
                job,
                task_command,
                exit_command="exit 1",
                as_mount=True,
            )

            # Get buckets - This can probably be improved.
            mount_buckets = set(
                re.findall(r"/mnt/disks/share/([^\/]+)/", script_command[-1] + task_command)
            )

            # Convert start command to script.
            START_SCRIPT = ".redun_job.sh"
            script_path = get_job_scratch_file(self.gcs_scratch_prefix, job, START_SCRIPT)

            File(script_path).write(DEFAULT_SHELL + script_command[-1])

            script_path = script_path.replace("gs://", "/mnt/disks/share/")

            # GCP Batch takes script as a string and requires quoting of -c argument
            script_command[-1] = script_path
            gcp_job = gcp_utils.batch_submit(
                client=self.gcp_client,
                job_name=f"redun-{job.id}",
                project=project,
                region=region,
                image=image,
                commands=script_command,
                gcs_scratch_prefix=self.gcs_scratch_prefix,
                mount_buckets=mount_buckets,
                **task_options,
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
        self.pending_batch_jobs[gcp_job.name] = job

    def _start(self) -> None:
        """
        Start monitoring thread.
        """
        if not self._thread or not self._thread.is_alive():
            self.is_running = True
            self._thread = threading.Thread(target=self._monitor, daemon=False)
            self._thread.start()

    def stop(self) -> None:
        """
        Stop Executor and monitoring thread.
        """
        self.is_running = False

        self._docker_executor.stop()

        # Stop monitor thread.
        if (
            self._thread
            and self._thread.is_alive()
            and threading.get_ident() != self._thread.ident
        ):
            self._thread.join()

    def _monitor(self) -> None:
        """
        Thread for monitoring running K8S jobs.
        """
        assert self._scheduler

        # Need new client for thread safety
        gcp_client = gcp_utils.get_gcp_client()

        try:
            while self.is_running and (self.pending_batch_jobs):
                if self._scheduler.logger.level >= logging.DEBUG:
                    self.log(
                        f"Waiting on {len(self.pending_batch_jobs)} Batch job(s):\n\t"
                        + "\n\t".join(sorted(self.pending_batch_jobs.keys())),
                        level=logging.DEBUG,
                    )

                # Copy pending_batch_jobs.keys() since it can change due to new submissions.
                job_names = list(self.pending_batch_jobs.keys())
                for name in job_names:
                    job = gcp_utils.get_job(client=gcp_client, job_name=name)
                    self._process_job_status(job)

                time.sleep(self.interval)

        except Exception as error:
            # Since we run this is method at the top-level of a thread, we need
            # to catch all exceptions so we can properly report them to the
            # scheduler.
            self.log("_monitor got exception", level=logging.INFO)
            self._scheduler.reject_job(None, error)

        self.log("Shutting down executor...", level=logging.DEBUG)
        self.stop()

    def _process_job_status(self, job: BatchJob) -> None:
        """
        Process GCP Batch job statuses.
        """
        assert self._scheduler
        state = job.status.state

        if state == JobStatus.State.STATE_UNSPECIFIED:
            return
        elif state == JobStatus.State.QUEUED:
            return
        elif state == JobStatus.State.SCHEDULED:
            return
        elif state == JobStatus.State.RUNNING:
            return
        elif state == JobStatus.State.SUCCEEDED:
            redun_job = self.pending_batch_jobs.pop(job.name)
            result, exists = parse_job_result(self.gcs_scratch_prefix, redun_job)
            if exists:
                self._scheduler.done_job(redun_job, result)
            else:
                # This can happen if job ended in an inconsistent state.
                self._scheduler.reject_job(
                    redun_job,
                    FileNotFoundError(
                        get_job_scratch_file(self.gcs_scratch_prefix, redun_job, SCRATCH_OUTPUT)
                    ),
                )
        elif state == JobStatus.State.FAILED:
            redun_job = self.pending_batch_jobs.pop(job.name)
            error, error_traceback = parse_job_error(self.gcs_scratch_prefix, redun_job)

            self._scheduler.reject_job(redun_job, error, error_traceback=error_traceback)

        elif state == JobStatus.State.DELETION_IN_PROGRESS:
            return
