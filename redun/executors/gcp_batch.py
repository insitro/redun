import json
import logging
import re
import threading
import time
import uuid
from collections import OrderedDict
from configparser import SectionProxy
from typing import Any, Dict, List, Optional, cast

from google.api_core.exceptions import NotFound
from google.cloud.batch_v1 import JobStatus, Task, TaskStatus

from redun.executors import gcp_utils
from redun.executors.base import Executor, register_executor
from redun.executors.code_packaging import package_code, parse_code_package_config
from redun.executors.command import get_oneshot_command, get_script_task_command
from redun.executors.docker import DockerExecutor, get_docker_executor_config
from redun.executors.scratch import (
    SCRATCH_ERROR,
    SCRATCH_HASHES,
    SCRATCH_INPUT,
    SCRATCH_OUTPUT,
    get_array_scratch_file,
    get_job_scratch_dir,
    get_job_scratch_file,
    parse_job_error,
    parse_job_result,
)
from redun.file import File
from redun.job_array import JobArrayer
from redun.scheduler import Job as RedunJob
from redun.scheduler import Scheduler
from redun.scripting import DEFAULT_SHELL, get_task_command
from redun.task import CacheScope
from redun.utils import pickle_dump

REDUN_ARRAY_JOB_PREFIX = "redun-array-"
REDUN_JOB_PREFIX = "redun-"

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
        docker_config["include_aws_env"] = "False"
        self._docker_executor = DockerExecutor(name + "_debug", scheduler, config=docker_config)

        self.gcp_batch_client = gcp_utils.get_gcp_batch_client()
        self.gcp_compute_client = gcp_utils.get_gcp_compute_client()

        # Required config.
        self.project = config["project"]
        self.region = config["region"]
        self.gcs_scratch_prefix = config["gcs_scratch"]
        self.image = config["image"]

        # Optional config
        self.code_package = parse_code_package_config(config)
        self.code_file: Optional[File] = None

        # Monitoring internals
        self.preexisting_batch_tasks: Dict[str, str] = {}  # RedunJob hash -> BatchJob task name
        # We use an OrderedDict in order to retain submission order.
        self.pending_batch_tasks: Dict[str, "RedunJob"] = OrderedDict()
        self.is_running = False
        self.interval = config.getfloat("job_monitor_interval", fallback=5.0)
        self._thread: Optional[threading.Thread] = None
        self.arrayer = JobArrayer(
            self._submit_jobs,
            self._on_error,
            submit_interval=self.interval,
            stale_time=config.getfloat("job_stale_time", fallback=3.0),
            min_array_size=config.getint("min_array_size", fallback=2),
            max_array_size=config.getint("max_array_size", fallback=5000),
        )

        # Default task options.
        self.default_task_options: Dict[str, Any] = {
            "machine_type": config.get("machine_type", fallback="e2-standard-4"),
            "task_count": config.getint("task_count", fallback=1),
            "retries": config.getint("retries", fallback=2),
            "priority": config.getint("priority", fallback=30),
            "service_account_email": config.get("service_account_email", fallback=""),
        }
        if config.get("labels"):
            self.default_task_options["labels"] = json.loads(config.get("labels"))

    def _on_error(self, error: Exception) -> None:
        """
        Callback for JobArrayer to report scheduler-level errors.
        """
        assert self._scheduler
        self._scheduler.reject_job(None, error)

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
            client=self.gcp_batch_client, project_id=self.project, region=self.region
        )
        inflight_job_statuses = [
            JobStatus.State.SCHEDULED,
            JobStatus.State.QUEUED,
            JobStatus.State.RUNNING,
        ]
        inflight_task_statuses = [
            TaskStatus.State.ASSIGNED,
            TaskStatus.State.PENDING,
            TaskStatus.State.RUNNING,
        ]

        for job in batch_jobs:
            # If a job is not labelled with a redun hash then it is not a redun job
            if REDUN_HASH_LABEL_KEY not in job.labels:
                continue

            if job.status.state not in inflight_job_statuses:
                continue

            job_hash = job.labels[REDUN_HASH_LABEL_KEY]

            # Single jobs have only one task in their task group and can be simply added to dict of
            # pre-existing jobs.
            if job.task_groups[0].task_count == 1:
                task_name = f"{job.task_groups[0].name}/tasks/0"
                self.preexisting_batch_tasks[job_hash] = task_name

            # Otherwise we need to read the eval file to reunite with batch tasks in the batch job
            else:
                eval_file = File(
                    get_array_scratch_file(self.gcs_scratch_prefix, job_hash, SCRATCH_HASHES)
                )
                if not eval_file.exists():
                    # Eval file does not exist, so we cannot reunite with this array job.
                    continue

                # Get eval_hash for all jobs that were part of the array
                eval_hashes = cast(str, eval_file.read("r")).splitlines()

                batch_tasks = gcp_utils.list_tasks(
                    client=self.gcp_batch_client, group_name=job.task_groups[0].name
                )
                for array_index, task in enumerate(batch_tasks):
                    # Skip job if it is not in one of the 'inflight' states
                    if task.status.state in inflight_task_statuses:
                        array_job_hash = eval_hashes[array_index]
                        self.preexisting_batch_tasks[array_job_hash] = task.name

    def _get_job_options(self, job: RedunJob, array_uuid: Optional[str] = None) -> dict:
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
            REDUN_HASH_LABEL_KEY: array_uuid if array_uuid else job.eval_hash,
            REDUN_JOB_TYPE_LABEL_KEY: "script" if job.task.script else "container",
        }

        project = self.project
        region = self.region
        machine_type = task_options.get("machine_type")

        # If either memory or vCPUS are not provided use the maximum available based on machine
        # type otherwise use the min of what is available and what is requested
        compute_machine_type = gcp_utils.get_compute_machine_type(
            self.gcp_compute_client, project, region, machine_type
        )

        requested_memory = task_options.get("memory", 0)
        # MiB to GB
        max_memory = compute_machine_type.memory_mb / 1024
        if requested_memory <= 0:
            task_options["memory"] = max_memory
        else:
            task_options["memory"] = min(requested_memory, max_memory)

        requested_vcpus = task_options.get("vcpus", 0)
        max_vcpus = compute_machine_type.guest_cpus
        if requested_vcpus <= 0:
            task_options["vcpus"] = max_vcpus
        else:
            task_options["vcpus"] = min(requested_vcpus, max_vcpus)

        # Merge labels if needed.
        labels: Dict[str, str] = {
            **self.default_task_options.get("labels", {}),
            **job_options.get("labels", {}),
            **default_tags,
        }

        task_options["labels"] = labels

        return task_options

    def submit(self, job: RedunJob) -> None:
        """
        Submit RedunJob to executor.
        """
        assert job.task
        assert not job.task.script

        if self._is_debug_job(job):
            return self._docker_executor.submit(job)
        else:
            self._submit(job)

    def submit_script(self, job: RedunJob) -> None:
        """
        Submit RedunJob for script task to executor.
        """
        assert job.task
        assert job.task.script

        if self._is_debug_job(job):
            return self._docker_executor.submit_script(job)
        else:
            self._submit(job)

    def _submit(self, job: RedunJob) -> None:
        """
        Submit RedunJob to executor.
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

        # Package code if necessary and we have not already done so. If code_package
        # is False, then we can skip this step. Additionally, if we have already
        # packaged and set code_file, then we do not need to repackage.
        if self.code_package is not False and self.code_file is None:
            code_package = self.code_package or {}
            assert isinstance(code_package, dict)
            self.code_file = package_code(self.gcs_scratch_prefix, code_package)

        task_options = self._get_job_options(job)
        cache_scope = CacheScope(task_options.get("cache_scope", CacheScope.BACKEND))

        # Determine if we can reunite with a previous Batch output or job.
        batch_task_name: Optional[str] = None
        if cache_scope == CacheScope.BACKEND and job.eval_hash in self.preexisting_batch_tasks:
            batch_task_name = self.preexisting_batch_tasks.pop(job.eval_hash)

            job_dir = get_job_scratch_dir(self.gcs_scratch_prefix, job)
            existing_task = gcp_utils.get_task(
                client=self.gcp_batch_client, task_name=batch_task_name
            )

            if existing_task:
                self.log(
                    "reunite redun job {redun_job} with {job_type}:\n"
                    "  task_name        = {batch_task_name}\n"
                    "  gcs_scratch_path = {job_dir}".format(
                        redun_job=job.id,
                        job_type="GCP Batch Job Task",
                        batch_task_name=batch_task_name,
                        job_dir=job_dir,
                    )
                )
                self.pending_batch_tasks[existing_task.name] = job
        if batch_task_name is None:
            self.arrayer.add_job(job)

        self._start()

    def _submit_jobs(self, jobs: List[RedunJob]) -> None:
        """
        Callback for JobArrayer to return arrays of Jobs.
        """
        if len(jobs) == 1:
            # Singleton jobs are given as single element lists.
            self._submit_single_job(jobs[0])
        else:
            self._submit_array_job(jobs)

    def _submit_array_job(self, jobs: List[RedunJob]) -> str:
        """
        Submits an array job, returning job name uuid
        """
        array_size = len(jobs)
        all_args = []
        all_kwargs = []
        for job in jobs:
            assert job.args
            all_args.append(job.args[0])
            all_kwargs.append(job.args[1])

        # All jobs identical so just grab the first one
        job = jobs[0]
        assert job.task
        if job.task.script:
            raise NotImplementedError("Array jobs not supported for scripts")

        # Generate a unique name for job with no '-' to simplify job name parsing.
        array_uuid = str(uuid.uuid4())

        task_options = self._get_job_options(job, array_uuid=array_uuid)

        # Setup input, output and error path files. Input file is a pickled list
        # of args, and kwargs, for each child job.
        input_file = get_array_scratch_file(self.gcs_scratch_prefix, array_uuid, SCRATCH_INPUT)
        with File(input_file).open("wb") as out:
            pickle_dump([all_args, all_kwargs], out)

        # Output file is a plaintext list of output paths, for each child job.
        output_file = get_array_scratch_file(self.gcs_scratch_prefix, array_uuid, SCRATCH_OUTPUT)
        output_paths = [
            get_job_scratch_file(self.gcs_scratch_prefix, job, SCRATCH_OUTPUT) for job in jobs
        ]
        with File(output_file).open("w") as ofile:
            json.dump(output_paths, ofile)

        # Error file is a plaintext list of error paths, one for each child job.
        error_file = get_array_scratch_file(self.gcs_scratch_prefix, array_uuid, SCRATCH_ERROR)
        error_paths = [
            get_job_scratch_file(self.gcs_scratch_prefix, job, SCRATCH_ERROR) for job in jobs
        ]
        with File(error_file).open("w") as efile:
            json.dump(error_paths, efile)

        # Eval hash file is plaintext hashes of child jobs for matching for job
        # reuniting.
        eval_file = get_array_scratch_file(self.gcs_scratch_prefix, array_uuid, SCRATCH_HASHES)
        with File(eval_file).open("w") as eval_f:
            eval_f.write("\n".join([job.eval_hash for job in jobs]))  # type: ignore

        image = task_options.pop("image", self.image)
        project = task_options.pop("project", self.project)
        region = task_options.pop("region", self.region)

        # Drop task_count, since we can infer it from the number of jobs.
        task_options.pop("task_count", None)
        command = get_oneshot_command(
            self.gcs_scratch_prefix, job, job.task, code_file=self.code_file, array_uuid=array_uuid
        )

        gcp_job = gcp_utils.batch_submit(
            client=self.gcp_batch_client,
            job_name=f"{REDUN_ARRAY_JOB_PREFIX}{array_uuid}",
            project=project,
            region=region,
            image=image,
            commands=command,
            gcs_scratch_prefix=self.gcs_scratch_prefix,
            task_count=array_size,
            **task_options,
        )

        gcp_job_task_count = gcp_job.task_groups[0].task_count
        if gcp_job_task_count != array_size:
            raise AssertionError(
                f"Batch job task group has {gcp_job_task_count} tasks "
                f"instead of requested array size {array_size}."
            )

        array_task_group_name = gcp_job.task_groups[0].name
        for i in range(gcp_job_task_count):
            self.pending_batch_tasks[f"{array_task_group_name}/tasks/{i}"] = jobs[i]

        self.log(
            "submit {array_size} redun job(s) as {job_type} {array_job_id}:\n"
            "  array_job_id          = {array_job_id}\n"
            "  array_task_group_name = {array_task_group_name}\n"
            "  array_size            = {array_size}\n"
            "  gcs_scratch_path      = {job_dir}\n".format(
                array_size=array_size,
                job_type="GCP Batch Job",
                array_job_id=gcp_job.uid,
                array_task_group_name=array_task_group_name,
                job_dir=get_array_scratch_file(self.gcs_scratch_prefix, array_uuid, ""),
            )
        )

        return array_uuid

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
        gcp_job = None
        if not job.task.script:
            command = get_oneshot_command(
                self.gcs_scratch_prefix,
                job,
                job.task,
                args=args,
                kwargs=kwargs,
                code_file=self.code_file,
            )

            gcp_job = gcp_utils.batch_submit(
                client=self.gcp_batch_client,
                job_name=f"{REDUN_JOB_PREFIX}{job.id}",
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
            mount_buckets: List[str] = list(
                set(re.findall(r"/mnt/disks/([^ \/]+)/", script_command[-1] + task_command))
            )

            # Convert start command to script.
            script_path = get_job_scratch_file(self.gcs_scratch_prefix, job, ".redun_job.sh")

            File(script_path).write(DEFAULT_SHELL + "\n" + script_command[-1])

            script_path = script_path.replace("gs://", "/mnt/disks/")

            # GCP Batch takes script as a string and requires quoting of -c argument
            script_command = ["bash", script_path]
            gcp_job = gcp_utils.batch_submit(
                client=self.gcp_batch_client,
                job_name=f"redun-{job.id}",
                project=project,
                region=region,
                image=image,
                commands=script_command,
                mount_buckets=mount_buckets,
                **task_options,
            )

        job_dir = get_job_scratch_dir(self.gcs_scratch_prefix, job)
        self.log(
            "submit redun job {redun_job} as {job_type}:\n"
            "  task_name        = {task_name}\n"
            "  gcs_scratch_path = {job_dir}\n".format(
                redun_job=job.id,
                job_type="GCP Batch Job Task",
                task_name=gcp_job.task_groups[0].name,
                job_dir=job_dir,
            )
        )
        task_name = gcp_job.task_groups[0].name
        # For a single submitted task we can set task name directly
        self.pending_batch_tasks[f"{task_name}/tasks/0"] = job

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
        self.arrayer.stop()

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

        # Need new client for thread safety
        gcp_batch_client = gcp_utils.get_gcp_batch_client()

        try:
            while self.is_running and (self.pending_batch_tasks or self.arrayer.num_pending):
                if self._scheduler.logger.level >= logging.DEBUG:
                    self.log(
                        f"Preparing {self.arrayer.num_pending} job(s) for Job Arrays.",
                        level=logging.DEBUG,
                    )
                    self.log(
                        f"Waiting on {len(self.pending_batch_tasks)} Batch tasks(s):\n\t"
                        + "\n\t".join(sorted(self.pending_batch_tasks.keys())),
                        level=logging.DEBUG,
                    )

                # Copy pending_batch_tasks.keys() since it can change due to new submissions.
                task_names = list(self.pending_batch_tasks.keys())
                for name in task_names:
                    try:
                        task = gcp_utils.get_task(client=gcp_batch_client, task_name=name)
                        self._process_task_status(task)
                    except NotFound:
                        # Batch Job has not instantiated tasks yet so ignore this NotFound error
                        continue

                time.sleep(self.interval)

        except Exception as error:
            # Since we run this is method at the top-level of a thread, we
            # need to catch all exceptions so we can properly report them to
            # the scheduler.
            self._scheduler.reject_job(None, error)

        self.log("Shutting down executor...", level=logging.DEBUG)
        self.stop()

    def _process_task_status(self, task: Task) -> None:
        assert self._scheduler
        state = task.status.state
        if state == TaskStatus.State.STATE_UNSPECIFIED:
            return
        elif state == TaskStatus.State.RUNNING:
            return
        elif state == TaskStatus.State.ASSIGNED:
            return
        elif state == TaskStatus.State.SUCCEEDED:
            redun_job = self.pending_batch_tasks.pop(task.name)
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
        elif state == TaskStatus.State.FAILED:
            redun_job = self.pending_batch_tasks.pop(task.name)
            error, error_traceback = parse_job_error(self.gcs_scratch_prefix, redun_job)

            self._scheduler.reject_job(redun_job, error, error_traceback=error_traceback)
