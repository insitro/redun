import logging
import threading
import time
from collections import OrderedDict
from configparser import SectionProxy
from itertools import islice
from typing import Any, Dict, Iterator, List, Optional, Tuple

from google.cloud import batch_v1
from google.cloud import logging as gcp_logging

from redun.config import create_config_section
from redun.executors import aws_utils
from redun.executors.base import Executor, register_executor
from redun.executors.code_packaging import package_code, parse_code_package_config
from redun.executors.command import get_oneshot_command, get_script_task_command
from redun.executors.docker import DockerExecutor
from redun.executors.scratch import (
    SCRATCH_OUTPUT,
    get_job_scratch_dir,
    get_job_scratch_file,
    parse_job_error,
    parse_job_result,
)
from redun.file import File
from redun.job_array import JobArrayer
from redun.scheduler import Job, Scheduler
from redun.scripting import get_task_command
from redun.task import Task

STATE_UNSPECIFIED = "STATE_UNSPECIFIED"
QUEUED = "QUEUED"
SCHEDULED = "SCHEDULED"
RUNNING = "RUNNING"
SUCCEEDED = "SUCCEEDED"
FAILED = "FAILED"
DELETION_IN_PROGRESS = "DELETION_IN_PROGRESS"

# GCP Batch job statuses.
BATCH_JOB_STATUSES = aws_utils.JobStatus(
    all=[
        STATE_UNSPECIFIED,
        QUEUED,
        SCHEDULED,
        RUNNING,
        RUNNING,
        SUCCEEDED,
        FAILED,
        DELETION_IN_PROGRESS,
    ],
    inflight=[QUEUED, SCHEDULED, RUNNING],
    pending=[QUEUED, SCHEDULED],
    success=[SUCCEEDED],
    failure=[FAILED],
    stopped=[],
    timeout=[],
)

DEBUG_SCRATCH = "scratch"

# Constants.
DEFAULT_GCP_REGION = "us-west1"
ARRAY_JOB_SUFFIX = "array"


def get_batch_job_name(prefix: str, job_hash: str, array: bool = False) -> str:
    """
    Return a GCP Batch Job name by either job or job hash.
    """
    return "{}-{}{}".format(prefix, job_hash, f"-{ARRAY_JOB_SUFFIX}" if array else "")[:63]


def batch_submit(
    command: List[str],
    image: str,
    job_def_name: Optional[str] = None,
    job_def_suffix: str = "-jd",
    job_name: str = "batch-job",
    array_size: int = 0,
    memory: int = 4,
    vcpus: int = 1,
    gpus: int = 0,
    boot_disk: int = 100,
    num_nodes: Optional[int] = None,
    shared_memory: Optional[int] = None,
    retries: int = 1,
    role: Optional[str] = None,
    gcp_region: str = DEFAULT_GCP_REGION,
    privileged: bool = False,
    autocreate_job: bool = True,
    timeout: Optional[int] = None,
    batch_tags: Optional[Dict[str, str]] = None,
    propagate_tags: bool = True,
) -> Dict[str, Any]:
    """Actually perform job submission to GCP batch. Create or retrieve the job definition, then
    use it to submit the job.

    batch_job_args : dict
        These are passed as kwargs to the `submit_job` API. Generally, it must configure the
        commands to be run by setting node or container overrides. This function will modify
        the provided dictionary, at a minimum applying overrides for the resource requirements
        stated by other arguments to this function.
    """
    batch_client = batch_v1.BatchServiceClient()

    job_def = {"project": "retro-cloud"}

    # Switch units, redun configs are in GB but GCP batch uses Mebibytes.
    memory_mib = int(memory * 953.7)
    boot_disk_mib = int(boot_disk * 953.7)
    cpu_milli = int(vcpus * 1000)

    job_parent = batch_client.common_location_path(project=job_def["project"], location=gcp_region)
    request = batch_v1.CreateJobRequest(
        parent=job_parent,
        job_id=job_name,
        job=batch_v1.Job(
            task_groups=[
                batch_v1.TaskGroup(
                    task_spec=batch_v1.TaskSpec(
                        runnables=[
                            batch_v1.Runnable(
                                container=batch_v1.Runnable.Container(
                                    image_uri=image,
                                    entrypoint="/bin/sh",
                                    commands=[
                                        "-c",
                                        "ls -al && " + " ".join(command),
                                    ],
                                )
                            )
                        ],
                        compute_resource=batch_v1.ComputeResource(
                            cpu_milli=cpu_milli,
                            memory_mib=memory_mib,
                            boot_disk_mib=boot_disk_mib,
                        ),
                    ),
                    task_count=1,
                    parallelism=1,
                )
            ],
            allocation_policy=batch_v1.AllocationPolicy(
                instances=[
                    batch_v1.AllocationPolicy.InstancePolicyOrTemplate(
                        policy=batch_v1.AllocationPolicy.InstancePolicy(
                            provisioning_model=batch_v1.AllocationPolicy.ProvisioningModel.STANDARD
                        )
                    )
                ]
            ),
            logs_policy=batch_v1.LogsPolicy(
                destination=batch_v1.LogsPolicy.Destination.CLOUD_LOGGING
            ),
        ),
    )
    response = batch_client.create_job(request=request)

    return response


def submit_task(
    image: str,
    scratch_prefix: str,
    job: Job,
    a_task: Task,
    args: Tuple = (),
    kwargs: Dict[str, Any] = {},
    job_options: dict = {},
    array_uuid: Optional[str] = None,
    array_size: int = 0,
    code_file: Optional[File] = None,
    gcp_region: str = DEFAULT_GCP_REGION,
) -> Dict[str, Any]:
    """
    Submit a redun Task to GCP Batch.
    """
    command = get_oneshot_command(
        scratch_prefix,
        job,
        a_task,
        args,
        kwargs,
        job_options=job_options,
        code_file=code_file,
        array_uuid=array_uuid,
    )
    # command_worker = get_oneshot_command(
    #     s3_scratch_prefix,
    #     job,
    #     a_task,
    #     args,
    #     kwargs,
    #     # Suppress cache checking since output is discarded.
    #     job_options={**job_options, "cache": False},
    #     code_file=code_file,
    #     array_uuid=array_uuid,
    #     output_path="/dev/null",  # Let main command write to scratch file.
    # )

    if array_uuid:
        job_hash = array_uuid
    else:
        assert job.eval_hash
        job_hash = job.eval_hash

    # Submit to GCP Batch.
    job_name = get_batch_job_name(
        job_options.get("job_name_prefix", "batch-job"), job_hash, array=bool(array_size)
    )

    # job_batch_options = get_batch_job_options(job_options)
    # job_batch_args = create_job_override_command(
    #     command=command,
    #     command_worker=command_worker,
    #     num_nodes=job_batch_options.get("num_nodes", None),
    # )

    result = batch_submit(
        command,
        image=image,
        job_name=job_name,
        job_def_suffix="-redun-jd",
        gcp_region=gcp_region,
        array_size=array_size,
        # **job_batch_options,
    )
    return result


def submit_command(
    image: str,
    scratch_prefix: str,
    job: Job,
    command: str,
    job_options: dict = {},
    gcp_region: str = DEFAULT_GCP_REGION,
) -> dict:
    """
    Submit a shell command to GCP Batch.
    """
    shell_command = get_script_task_command(
        scratch_prefix,
        job,
        command,
        exit_command="exit 1",
    )
    # first element is entrypoint
    shell_command = shell_command[1:]

    # # Submit to GCP Batch.
    assert job.eval_hash
    job_name = get_batch_job_name(job_options.get("job_name_prefix", "batch-job"), job.eval_hash)

    # job_batch_options = get_batch_job_options(job_options)
    # job_batch_args = create_job_override_command(
    #     command=shell_command,
    #     num_nodes=job_batch_options.get("num_nodes", None),
    # )

    # Submit to GCP Batch.
    return batch_submit(
        shell_command,
        image=image,
        job_name=job_name,
        job_def_suffix="-redun-jd",
        gcp_region=gcp_region,
        # **job_batch_options,
    )


def iter_batch_job_status(job_ids: List[str], pending_truncate: int = 10) -> Iterator[dict]:
    """
    Yields GCP Batch jobs statuses.

    Parameters
    ----------
    job_ids : List[str]
        Batch job ids that should be in order of submission.
    pending_truncate : int
        After seeing `pending_truncate` number of pending jobs, assume the rest are pending.
        Use a negative int to disable this optimization.
    """
    pending_run = 0

    client = batch_v1.BatchServiceClient()
    import pdb

    pdb.set_trace()

    for job_id in job_ids:
        job = client.get_job(name=job_id)
        yield job

        if job.status.state.name in BATCH_JOB_STATUSES.pending:
            pending_run += 1
        else:
            pending_run = 0

        if pending_truncate > 0 and pending_run > pending_truncate:
            break


def parse_job_logs(
    batch_job_id: str,
    max_lines: int = 1000,
) -> Iterator[str]:
    """
    Iterates through most recent CloudWatch logs of an GCP Batch Job.
    """
    log_client = gcp_logging.Client()
    logger = log_client.logger("batch_task_logs")

    lines_iter = logger.list_entries(filter_=f"labels.job_uid={batch_job_id}")
    lines = [entry.payload for entry in reversed(list(islice(lines_iter, 0, max_lines)))]

    if next(lines_iter, None) is not None:
        yield "\n*** Earlier logs are truncated ***\n"
    yield from lines


def get_docker_executor_config(config: SectionProxy) -> SectionProxy:
    """
    Returns a config for DockerExecutor.
    """
    keys = ["image", "scratch", "job_monitor_interval", "vcpus", "gpus", "memory"]
    return create_config_section({key: config[key] for key in keys if key in config})


@register_executor("gcp_batch")
class GCPBatchExecutor(Executor):
    """
    A redun Executor for running jobs on GCP Batch.
    """

    def __init__(self, name: str, scheduler: Optional["Scheduler"] = None, config=None):
        super().__init__(name, scheduler, config)
        if config is None:
            raise ValueError("GCPBatchExecutor requires config.")

        # Use DockerExecutor for local debug mode.
        docker_config = get_docker_executor_config(config)
        docker_config["scratch"] = config.get("debug_scratch", fallback=DEBUG_SCRATCH)
        self._docker_executor = DockerExecutor(name + "_debug", scheduler, config=docker_config)

        self.image = config["image"]
        self.scratch_prefix = config["scratch"]

        self.gcp_region = config.get("gcp_region", DEFAULT_GCP_REGION)
        self.code_package = parse_code_package_config(config)
        self.code_file: Optional[File] = None
        self.debug = config.getboolean("debug", fallback=False)

        # Default task options.
        self.default_task_options: Dict[str, Any] = {
            "vcpus": config.getint("vcpus", fallback=1),
            "gpus": config.getint("gpus", fallback=0),
            "memory": config.getint("memory", fallback=4),
            "shared_memory": config.getint("shared_memory", fallback=None),
            "retries": config.getint("retries", fallback=1),
            "job_name_prefix": config.get("job_name_prefix", fallback="redun-job"),
            "num_nodes": config.getint("num_nodes", fallback=None),
        }

        self.is_running = False
        # We use an OrderedDict in order to retain submission order.
        self.pending_batch_jobs: Dict[str, "Job"] = OrderedDict()
        self.preexisting_batch_jobs: Dict[str, str] = {}  # Job hash -> Job ID
        self.interval = config.getfloat("job_monitor_interval", fallback=5.0)

        self._thread: Optional[threading.Thread] = None
        self.arrayer = JobArrayer(
            submit_interval=self.interval,
            stale_time=config.getfloat("job_stale_time", fallback=3.0),
            min_array_size=config.getint("min_array_size", fallback=5),
            max_array_size=config.getint("max_array_size", fallback=1000),
        )

    def set_scheduler(self, scheduler: Scheduler) -> None:
        super().set_scheduler(scheduler)
        self._docker_executor.set_scheduler(scheduler)

    def _start(self) -> None:
        """
        Start monitoring thread.
        """
        if not self.is_running:
            self.is_running = True
            self._thread = threading.Thread(target=self._monitor, daemon=False)
            self._thread.start()

    def stop(self) -> None:
        """
        Stop Executor and monitoring thread.
        """
        self.arrayer.stop()
        self.is_running = False

    def _monitor(self) -> None:
        """
        Thread for monitoring running K8S jobs.
        """
        assert self._scheduler

        try:
            while self.is_running and (self.pending_batch_jobs or self.arrayer.num_pending):
                if self._scheduler.logger.level >= logging.DEBUG:
                    self.log(
                        f"Preparing {self.arrayer.num_pending} job(s) for Job Arrays.",
                        level=logging.DEBUG,
                    )
                    self.log(
                        f"Waiting on {len(self.pending_batch_jobs)} K8S job(s): "
                        + " ".join(sorted(self.pending_batch_jobs.keys())),
                        level=logging.DEBUG,
                    )

                # Copy pending_batch_jobs.keys() since it can change due to new
                # submissions.
                pending_jobs = list(self.pending_batch_jobs.keys())
                jobs = iter_batch_job_status(
                    job_ids=pending_jobs,
                )
                for job in jobs:
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

    def _process_job_status(self, job: dict) -> None:
        """
        Process GCP Batch job statuses.
        """
        assert self._scheduler
        job_status: Optional[str] = None

        # Determine job status.
        if job.status.state.name == SUCCEEDED:
            job_status = SUCCEEDED

        elif job.status.state.name == FAILED:
            # can_override, container_reason = self._can_override_failed(job)
            # TODO: add
            can_override = False
            container_reason = "Failed"
            if can_override:
                job_status = SUCCEEDED
                self._scheduler.log(
                    "NOTE: Overriding GCP Batch error: {}".format(container_reason)
                )
            else:
                job_status = FAILED
        else:
            return

        # Determine redun Job and job_tags.
        redun_job = self.pending_batch_jobs.pop(job.name)
        job_tags = []
        job_tags.append(("gcp_batch_job", job.name))
        job_tags.append(("gcp_log_stream", job.uid))

        if job_status == SUCCEEDED:
            # Assume a recently completed job has valid results.
            result, exists = parse_job_result(self.scratch_prefix, redun_job)
            if exists:
                self._scheduler.done_job(redun_job, result, job_tags=job_tags)
            else:
                # This can happen if job ended in an inconsistent state.
                self._scheduler.reject_job(
                    redun_job,
                    FileNotFoundError(
                        get_job_scratch_file(self.scratch_prefix, redun_job, SCRATCH_OUTPUT)
                    ),
                    job_tags=job_tags,
                )
        elif job_status == FAILED:
            error, error_traceback = parse_job_error(self.scratch_prefix, redun_job)
            logs = [f"*** Logs for GCP Batch job {job.name}:\n"]
            if container_reason:
                logs.append(f"container.reason: {container_reason}\n")

            logs.extend(parse_job_logs(job.uid))
            error_traceback.logs = logs
            self._scheduler.reject_job(
                redun_job, error, error_traceback=error_traceback, job_tags=job_tags
            )

    def _get_job_options(self, job: Job) -> dict:
        """
        Determine the task options for a job.

        Task options can be specified at the job-level have precedence over
        the executor-level (within `redun.ini`):
        """
        assert job.task

        job_options = job.get_options()

        task_options = {
            **self.default_task_options,
            **job_options,
        }

        # Add default batch tags to the job.
        # if self.use_default_batch_tags:
        #     execution = job.execution
        #     project = (
        #         execution.job.task.namespace
        #         if execution and execution.job and execution.job.task
        #         else ""
        #     )
        #     default_tags = {
        #         "redun_job_id": job.id,
        #         "redun_task_name": job.task.fullname,
        #         "redun_execution_id": execution.id if execution else "",
        #         "redun_project": project,
        #         "redun_aws_user": self._aws_user or "",
        #     }
        # else:
        #     default_tags = {}

        # # Merge batch_tags if needed.
        # batch_tags: dict = {
        #     **self.default_task_options.get("batch_tags", {}),
        #     **default_tags,
        #     **job_options.get("batch_tags", {}),
        # }
        # if batch_tags:
        #     task_options["batch_tags"] = batch_tags

        return task_options

    def _submit(self, job: Job, args: Tuple, kwargs: dict) -> None:
        """
        Submit Job to executor.
        """
        assert self._scheduler
        assert job.task
        assert not self.debug

        # If we are not in debug mode and this is the first submission gather inflight jobs. In
        # debug mode, we are running on docker locally so there is no need to hit the Batch API to
        # gather jobs as we are not going to run on batch. We also check is_running here as a way
        # of determining whether this is the first submission or not. If we are already running,
        # then we know we have already had jobs submitted and done the inflight check so no
        # reason to do that again here.
        # if not self.is_running:
        #     # Precompute existing inflight jobs for job reuniting.
        #     self.gather_inflight_jobs()

        # Package code if necessary and we have not already done so. If code_package is False,
        # then we can skip this step. Additionally, if we have already packaged and set code_file,
        # then we do not need to repackage.
        if self.code_package is not False and self.code_file is None:
            code_package = self.code_package or {}
            assert isinstance(code_package, dict)
            self.code_file = package_code(self.scratch_prefix, code_package)

        # job_dir = get_job_scratch_dir(self.scratch_prefix, job)
        # job_type = "GCP Batch job"

        # Determine job options.
        # task_options = self._get_job_options(job)
        # use_cache = task_options.get("cache", True)

        # Determine if we can reunite with a previous Batch output or job.
        batch_job_id: Optional[str] = None
        # if use_cache and job.eval_hash in self.preexisting_batch_jobs:
        #     batch_job_id = self.preexisting_batch_jobs.pop(job.eval_hash)

        #     # Make sure Batch API still has a status on this job.
        #     existing_job = next(
        #         aws_describe_jobs([batch_job_id], aws_region=self.aws_region), None
        #     )

        #     # Reunite with inflight batch job, if present.
        #     if existing_job:
        #         batch_job_id = existing_job["jobId"]
        #         self.log(
        #             "reunite redun job {redun_job} with {job_type} {batch_job}:\n"
        #             "  s3_scratch_path = {job_dir}".format(
        #                 redun_job=job.id,
        #                 job_type=job_type,
        #                 batch_job=batch_job_id,
        #                 job_dir=job_dir,
        #             )
        #         )
        #         assert batch_job_id
        #         self.pending_batch_jobs[batch_job_id] = job
        #     else:
        #         batch_job_id = None

        # Job arrayer will handle actual submission after bunching to an array
        # job, if necessary.
        if batch_job_id is None:
            self.arrayer.add_job(job, args, kwargs)

        self._start()

    def _submit_single_job(self, job: Job, args: Tuple, kwargs: dict) -> None:
        """
        Actually submits a job. Caching detects if it should be part
        of an array job
        """
        assert job.task
        task_options = self._get_job_options(job)
        image = task_options.pop("image", self.image)

        job_dir = get_job_scratch_dir(self.scratch_prefix, job)
        job_type = "GCP Batch job"

        # Submit a new Batch job.
        if not job.task.script:
            batch_resp = submit_task(
                image,
                self.scratch_prefix,
                job,
                job.task,
                args=args,
                kwargs=kwargs,
                job_options=task_options,
                code_file=self.code_file,
                gcp_region=self.gcp_region,
            )
        else:
            command = get_task_command(job.task, args, kwargs)
            batch_resp = submit_command(
                image,
                self.scratch_prefix,
                job,
                command,
                job_options=task_options,
            )

        self.log(
            # TODO: add
            # "  retry_attempts  = {retries}\n"
            "submit redun job {redun_job} as {job_type} {batch_job}:\n"
            "  job_id          = {batch_job}\n"
            "  job_name        = {job_name}\n"
            "  scratch_path = {job_dir}\n".format(
                redun_job=job.id,
                job_type=job_type,
                batch_job=batch_resp.name,
                job_dir=job_dir,
                job_name=batch_resp.name,
                # retries=batch_resp.get("ResponseMetadata", {}).get("RetryAttempts"),
            )
        )
        batch_job_id = batch_resp.name
        self.pending_batch_jobs[batch_job_id] = job

    def _is_debug_job(self, job: Job) -> bool:
        """
        Returns True if job should be sent to debugging DockerExecutor.
        """
        return self.debug or job.get_option("debug", False)

    def submit(self, job: Job, args: Tuple, kwargs: dict) -> None:
        """
        Submit Job to executor.
        """
        if self._is_debug_job(job):
            return self._docker_executor.submit(job, args, kwargs)
        else:
            return self._submit(job, args, kwargs)

    def submit_script(self, job: Job, args: Tuple, kwargs: dict) -> None:
        """
        Submit Job for script task to executor.
        """
        if self._is_debug_job(job):
            return self._docker_executor.submit_script(job, args, kwargs)
        else:
            return self._submit(job, args, kwargs)
