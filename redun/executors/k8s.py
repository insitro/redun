import datetime
import logging
import os
import json
import pickle
import re
import threading
import typing
import time
import uuid

from collections import OrderedDict, defaultdict
from itertools import islice
from shlex import quote
from typing import Any, Dict, Iterator, List, Optional, Tuple, cast

import kubernetes

from redun.executors import k8s_utils, s3_utils, aws_utils
from redun.executors.base import Executor, register_executor
from redun.file import File
from redun.job_array import JobArrayer
from redun.scheduler import Job, Scheduler, Traceback
from redun.scripting import ScriptError, get_task_command
from redun.task import Task
from redun.utils import get_import_paths, pickle_dump

SUCCEEDED = "SUCCEEDED"
FAILED = "FAILED"
ARRAY_JOB_SUFFIX = "array"


def is_array_job_name(job_name: str) -> bool:
    "Returns true if the job name looks like an array job"
    return job_name.endswith(f"-{ARRAY_JOB_SUFFIX}")


def k8s_submit(
    command: List[str],
    image: str,
    namespace: str,
    job_name: str = "k8s-job",
    array_size: int = 0,
    memory: int = 4,
    vcpus: int = 1,
    timeout: Optional[int] = None,
    k8s_labels: Optional[Dict[str, str]] = None,
    retries: int = 1,
) -> Dict[str, Any]:
    """Prepares and submits a k8s job to the API server"""
    requests = {
        "memory": f"{memory}G",
        "cpu": vcpus,
    }
    limits = requests
    resources = k8s_utils.create_resources(requests, limits)

    k8s_job = k8s_utils.create_job_object(
        job_name,
        image,
        command,
        resources,
        timeout,
        k8s_labels,
    )

    if array_size > 1:
        k8s_job.spec.completions = array_size
        k8s_job.spec.parallelism = array_size
        k8s_job.spec.completion_mode = "Indexed"
        k8s_job.spec.backoff_limit = retries * array_size
    else:
        k8s_job.spec.backoff_limit = retries
    k8s_job.spec.restart_policy = "OnFailure"
    # if batch_tags:
    #    batch_job_args["tags"] = batch_tags
    api_instance = k8s_utils.get_k8s_batch_client()
    api_response = k8s_utils.create_job(
        api_instance, k8s_job, namespace=namespace
    )
    return api_response


def get_k8s_job_name(prefix: str, job_hash: str, array: bool = False) -> str:
    """
    Return a K8S Job name by either job or job hash.
    """
    return "{}-{}{}".format(
        prefix, job_hash, f"-{ARRAY_JOB_SUFFIX}" if array else ""
    )


def get_hash_from_job_name(job_name: str) -> Optional[str]:
    """
    Returns the job/task eval_hash that corresponds with a particular job name
    on K8S.
    """
    # Remove array job suffix, if present.
    array_suffix = "-" + ARRAY_JOB_SUFFIX
    if job_name.endswith(array_suffix):
        job_name = job_name[: -len(array_suffix)]

    # It's possible we found jobs that are unrelated to the this work based off
    # the job_name_prefix matching when fetching in get_jobs. These jobs will
    # not have hashes so we can ignore them. For a concrete example of this,
    # see:
    #
    #   https://insitro.atlassian.net/browse/DE-2632
    #
    # where a headnode job is running but has no hash so we don't want to
    # interact with that job here. If we don't find a match, consider this a
    # case of the above where we matched unrelated jobs and return None to let
    # callers know this is the case.
    match = re.match(".*-(?P<hash>[^-]+)", job_name)
    if match:
        return match["hash"]

    return None


def get_k8s_job_options(job_options: dict) -> dict:
    """
    Returns K8S-specific job options from general job options.
    """
    keys = ["memory", "vcpus", "k8s_labels", "retries", "timeout"]
    return {key: job_options[key] for key in keys if key in job_options}


def submit_task(
    image: str,
    namespace: str,
    s3_scratch_prefix: str,
    job: Job,
    a_task: Task,
    args: Tuple = (),
    kwargs: Optional[Dict[str, Any]] = None,
    job_options: Optional[dict] = None,
    array_uuid: Optional[str] = None,
    array_size: int = 0,
    code_file: Optional[File] = None,
) -> kubernetes.client.V1Job:
    """
    Submit a redun Task to K8S
    """
    # To avoid dangerous default {}
    if kwargs is None:
        kwargs = {}
    if job_options is None:
        job_options = {}
    input_path = None
    output_path = None
    error_path = None

    if array_size:
        # Output_path will contain a pickled list of actual output paths, etc.
        # Want files that won't get clobbered when jobs actually run
        assert array_uuid
        input_path = aws_utils.get_array_scratch_file(
            s3_scratch_prefix, array_uuid, s3_utils.S3_SCRATCH_INPUT
        )
        output_path = aws_utils.get_array_scratch_file(
            s3_scratch_prefix, array_uuid, s3_utils.S3_SCRATCH_OUTPUT
        )
        error_path = aws_utils.get_array_scratch_file(
            s3_scratch_prefix, array_uuid, s3_utils.S3_SCRATCH_ERROR
        )
    else:
        input_path = aws_utils.get_job_scratch_file(
            s3_scratch_prefix, job, s3_utils.S3_SCRATCH_INPUT
        )
        output_path = aws_utils.get_job_scratch_file(
            s3_scratch_prefix, job, s3_utils.S3_SCRATCH_OUTPUT
        )
        error_path = aws_utils.get_job_scratch_file(
            s3_scratch_prefix, job, s3_utils.S3_SCRATCH_ERROR
        )

        # Serialize arguments to input file. Array jobs set this up earlier, in
        # `_submit_array_job`
        input_file = File(input_path)
        with input_file.open("wb") as out:
            pickle_dump([args, kwargs], out)

    # Determine additional python import paths.
    import_args = []
    base_path = os.getcwd()
    for abs_path in get_import_paths():
        # Use relative paths so that they work inside the docker container.
        rel_path = os.path.relpath(abs_path, base_path)
        import_args.append("--import-path")
        import_args.append(rel_path)

    # Build job command.
    code_arg = ["--code", code_file.path] if code_file else []
    array_arg = ["--array-job"] if array_size else []
    cache_arg = [] if job_options.get("cache", True) else ["--no-cache"]
    command = (
        [
            aws_utils.REDUN_PROG,
            "--check-version",
            aws_utils.REDUN_REQUIRED_VERSION,
            "oneshot",
            a_task.load_module,
        ]
        + import_args
        + code_arg
        + array_arg
        + cache_arg
        + [
            "--input",
            input_path,
            "--output",
            output_path,
            "--error",
            error_path,
            a_task.fullname,
        ]
    )
    if array_uuid:
        job_hash = array_uuid
    else:
        assert job.eval_hash
        job_hash = job.eval_hash

    # Submit to K8S
    job_name = get_k8s_job_name(
        job_options.get("job_name_prefix", "k8s-job"),
        job_hash,
        array=bool(array_size),
    )
    result = k8s_submit(
        command,
        image=image,
        namespace=namespace,
        job_name=job_name,
        array_size=array_size,
        **get_k8s_job_options(job_options),
    )

    return result


def submit_command(
    image: str,
    namespace: str,
    s3_scratch_prefix: str,
    job: Job,
    command: str,
    job_options: Optional[dict] = None,
) -> kubernetes.client.V1Job:
    """
    Submit a shell command to K8S
    """
    # to avoid dangerous default dict
    if job_options is None:
        job_options = {}
    input_path = aws_utils.get_job_scratch_file(
        s3_scratch_prefix, job, s3_utils.S3_SCRATCH_INPUT
    )
    output_path = aws_utils.get_job_scratch_file(
        s3_scratch_prefix, job, s3_utils.S3_SCRATCH_OUTPUT
    )
    error_path = aws_utils.get_job_scratch_file(
        s3_scratch_prefix, job, s3_utils.S3_SCRATCH_ERROR
    )
    status_path = aws_utils.get_job_scratch_file(
        s3_scratch_prefix, job, s3_utils.S3_SCRATCH_STATUS
    )

    # Serialize arguments to input file.
    input_file = File(input_path)
    input_file.write(command)
    assert input_file.exists()

    # Build job command.
    shell_command = [
        "bash",
        "-c",
        "-o",
        "pipefail",
        """
aws s3 cp {input_path} .task_command
chmod +x .task_command
(
  ./.task_command \
  2> >(tee .task_error >&2) | tee .task_output
) && (
    aws s3 cp .task_output {output_path}
    aws s3 cp .task_error {error_path}
    echo ok | aws s3 cp - {status_path}
) || (
    [ -f .task_output ] && aws s3 cp .task_output {output_path}
    [ -f .task_error ] && aws s3 cp .task_error {error_path}
    echo fail | aws s3 cp - {status_path}
    {exit_command}
)
""".format(
            input_path=quote(input_path),
            output_path=quote(output_path),
            error_path=quote(error_path),
            status_path=quote(status_path),
            exit_command="exit 1",
        ),
    ]

    # Submit to K8S.
    assert job.eval_hash
    job_name = get_k8s_job_name(
        job_options.get("job_name_prefix", "k8s-job"), job.eval_hash
    )

    # Submit to K8S.
    return k8s_submit(
        shell_command,
        image=image,
        namespace=namespace,
        job_name=job_name,
        **get_k8s_job_options(job_options),
    )


def parse_task_error(
    s3_scratch_prefix: str, job: Job
) -> Tuple[Exception, "Traceback"]:
    """
    Parse task error from s3 scratch path.
    """
    assert job.task

    error_path = aws_utils.get_job_scratch_file(
        s3_scratch_prefix, job, s3_utils.S3_SCRATCH_ERROR
    )
    error_file = File(error_path)

    if not job.task.script:
        # Normal Tasks (non-script) store errors as Pickled exception, traceback
        # tuples.
        if error_file.exists():
            error, error_traceback = pickle.loads(
                cast(bytes, error_file.read("rb"))
            )
        else:
            error = K8SError(
                "Exception and traceback could not be found for K8S Job."
            )
            error_traceback = Traceback.from_error(error)
    else:
        # Script task.
        if error_file.exists():
            error = ScriptError(cast(bytes, error_file.read("rb")))
        else:
            error = K8SError("stderr could not be found for K8S Job.")
        error_traceback = Traceback.from_error(error)

    return error, error_traceback


def parse_task_logs(
    k8s_job_id: str, namespace, max_lines: int = 1000
) -> Iterator[str]:
    """
    Iterates through most recent logs of an K8S Job.
    """

    lines_iter = iter_k8s_job_log_lines(
        k8s_job_id,
        namespace,
        reverse=True,
    )

    i = islice(lines_iter, 0, max_lines)
    lines = reversed(list(i))

    if next(lines_iter, None) is not None:
        yield "\n*** Earlier logs are truncated ***\n"
    yield from lines


def k8s_describe_jobs(
    job_names: List[str], namespace: str
) -> typing.List[kubernetes.client.V1Job]:
    """
    Returns K8S Job descriptions from the AWS API.
    """
    api_instance = k8s_utils.get_k8s_batch_client()
    responses = []
    for job_name in job_names:
        api_response = api_instance.read_namespaced_job(
            job_name, namespace=namespace
        )
        responses.append(api_response)
    return responses


def iter_k8s_job_status(job_ids, namespace):
    """Wraps k8s_describe_jobs in a generator"""
    for job in k8s_describe_jobs(job_ids, namespace):
        yield job


def iter_log_stream(
    job_id: str,
    namespace: str,
    reverse: bool = False,
) -> Iterator[str]:
    """
    Iterate through the events of a K8S log.
    """

    job = k8s_describe_jobs([job_id], namespace)[0]
    api_instance = k8s_utils.get_k8s_core_client()
    label_selector = f"job-name={job.metadata.name}"
    api_response = api_instance.list_pod_for_all_namespaces(
        label_selector=label_selector
    )
    if len(api_response.items) == 0:
        lines = [f"Cannot find pod for job {job.metadata.name}"]
    else:
        name = api_response.items[0].metadata.name
        namespace = api_response.items[0].metadata.namespace
        log_response = api_instance.read_namespaced_pod_log(
            name, namespace=namespace
        )
        lines = log_response.split("\n")
        state = api_response.items[0].status.container_statuses[0].state
        exit_code = state.terminated.exit_code
        if exit_code != 0:
            lines.append(f"Pod exited with {exit_code}")
            reason = state.terminated.reason
            lines.append(f"Exit reason: {reason}")
    if reverse:
        lines = reversed(lines)
        yield from lines


# Currently unused TODO(davidek): figure out if we need to format the logs
# correct (withi timestamps?)
def format_log_stream_event(event: dict) -> str:
    """
    Format a logStream event as a line.
    """
    timestamp = str(datetime.datetime.fromtimestamp(event["timestamp"] / 1000))
    return f"{timestamp}  {event['message']}"


# TODO(dek): DO NOT SUBMIT: can we get rid of this function?
def iter_k8s_job_logs(
    job_id: str,
    namespace: str,
    reverse: bool = False,
) -> Iterator[str]:
    """
    Iterate through the log events of an K8S job.
    """
    # another pass-through implementation due to copying the aws_batch
    # implementation.
    yield from iter_log_stream(
        job_id=job_id,
        namespace=namespace,
        reverse=reverse,
    )


# TODO(dek): DO NOT SUBMIT: can we get rid of this whole function?
def iter_k8s_job_log_lines(
    job_id: str,
    namespace,
    reverse: bool = False,
) -> Iterator[str]:
    """
    Iterate through the log lines of an K8S job.
    """
    log_lines = iter_k8s_job_logs(
        job_id,
        namespace,
        reverse=reverse,
    )
    return log_lines


class K8SError(Exception):
    """K8S-specific exception raised when a k8s job fails."""

    pass


class K8STimeoutExceeded(Exception):
    """
    Custom exception to raise when K8S jobs are killed due to timeout.
    """

    pass


@register_executor("k8s")
class K8SExecutor(Executor):
    """Implementation of K8SExecutor.
    This class adapts redun jobs to the k8s API.  It uses k8s jobs
    https://kubernetes.io/docs/concepts/workloads/controllers/job/ to
    encapsulate the submissions. For workflows with many tasks, it groups
    multiple tasks into a single k8s job
    (https://kubernetes.io/docs/tasks/job/indexed-parallel-processing-static/).
    to avoid overloading the k8s scheduler.
    and it lacks a debug mode (use minikube or another k8s tool for debug).

    Here are the details of the implementation:
    - The implementation is very similar to AWSBatchExecutor, with some minor
    differences to adapt to the k8s API.  The primary difference is that array
    jobs are handled differently
    - There is no debug mode; if you want to debug K8SExecutor-based workflows,
    use minikube https://minikube.sigs.k8s.io/ to test your workflows locally.
    Minikube is powerful enough to run many workflows on a single local machine.
    - K8SExecutor submits redun tasks (shell scripts and code) as K8S Jobs which
    execute the tasks in K8S Pods (containers).  K8S Jobs wrap pods with
    additional functionality, such as retry and parallel processing.
    - Like the AWSBatchExecutor, this implementation can batch up multiple tasks
    into a single Job.  This is more efficient for the k8s scheduler, if you are
    running more than about 100 tasks.
    - There are a few situations where this implementation can fail to return
    useful log messages from k8s jobs and pods.
    """

    def __init__(
        self, name: str, scheduler: Optional[Scheduler] = None, config=None
    ):
        super().__init__(name, scheduler=scheduler)
        if config is None:
            raise ValueError("K8SExecutor requires config.")

        # Required config.
        self.image = config["image"]
        self.namespace = config.get("namespace", "default")
        self.s3_scratch_prefix = config["s3_scratch"]

        # Optional config.
        self.role = config.get("role")
        self.code_package = aws_utils.parse_code_package_config(config)

        self.code_file: Optional[File] = None

        # Default task options.
        self.default_task_options = {
            "vcpus": config.getint("vcpus", 1),
            "memory": config.getint("memory", 4),
            "retries": config.getint("retries", 1),
            "job_name_prefix": config.get(
                "job_name_prefix", k8s_utils.DEFAULT_JOB_PREFIX
            ),
        }
        if config.get("k8s_labels"):
            self.default_task_options["k8s_labels"] = json.loads(
                config.get("k8s_labels")
            )
        self.use_default_k8s_labels = config.getboolean(
            "default_k8s_labels", True
        )

        self.is_running = False
        # We use an OrderedDict in order to retain submission order.
        self.pending_k8s_jobs: Dict[str, "Job"] = OrderedDict()
        self.preexisting_k8s_jobs: Dict[str, str] = {}  # Job hash -> Job ID

        self.interval = config.getfloat("job_monitor_interval", 5.0)

        self.arrayer = JobArrayer(
            executor=self,
            submit_interval=self.interval,
            stale_time=config.getfloat("job_stale_time", 3.0),
            min_array_size=config.getint("min_array_size", 5),
            max_array_size=config.getint("max_array_size", 1000),
        )

    def gather_inflight_jobs(self) -> None:
        """Collect existing k8s jobs and match them up to redun jobs"""
        running_arrays: Dict[str, List[Tuple[str, int]]] = defaultdict(list)
        # Get all running jobs by name.
        inflight_jobs = self.get_jobs()
        for job in inflight_jobs.items:
            job_name = job.metadata.name
            if not is_array_job_name(job_name):
                job_hash = get_hash_from_job_name(job_name)
                if job_hash:
                    self.preexisting_k8s_jobs[job_hash] = job.metadata.uid
                continue
            # Get all child pods of running array jobs for reuniting.
            for child_pod in self.get_array_child_pods(job.metadata.name).items:
                running_arrays[job_name].append(
                    (
                        child_pod.metadata.name,
                        child_pod.metadata.uid,
                        int(
                            child_pod.metadata.annotations[
                                "batch.kubernetes.io/job-completion-index"
                            ]
                        ),
                    )
                )
        # Match up running array jobs with consistent redun job naming scheme.
        for array_name, child_pod_indices in running_arrays.items():
            # Get path to array file directory on S3 from array job name.
            parent_hash = get_hash_from_job_name(array_name)
            if not parent_hash:
                continue
            eval_file = File(
                aws_utils.get_array_scratch_file(
                    self.s3_scratch_prefix,
                    parent_hash,
                    s3_utils.S3_SCRATCH_HASHES,
                )
            )
            if not eval_file.exists():
                # Eval file does not exist, so we cannot reunite with this array
                # job.
                continue

            # Get eval_hash for all jobs that were part of the array
            eval_hashes = cast(str, eval_file.read("r")).splitlines()

            # Now match up indices to eval hashes to populate pending jobs by
            # name.
            for (
                child_job_name,
                child_job_id,
                child_job_index,
            ) in child_pod_indices:
                job_hash = eval_hashes[child_job_index]
                self.preexisting_k8s_jobs[job_hash] = (
                    child_job_id,
                    child_job_name,
                    child_job_index,
                    parent_hash,
                )

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

        We use the following process for monitoring K8S jobs in order to achieve
        timely updates and avoid excessive API calls which can cause API
        throttling and slow downs.

        - We use the `describe_jobs()` API call on specific Batch job ids in
          order to avoid processing status of unrelated jobs on the same K8S
          queue.
        - We call `describe_jobs()` with 100 job ids at a time to reduce the
          number of API calls. 100 job ids is the maximum supported amount by
          `describe_jobs()`.
        - We do only one describe_jobs() API call per monitor loop, and then
          sleep `self.interval` seconds.
        - K8S runs jobs in approximately the order submitted. So if we monitor
          job statuses in submission order, a run of PENDING statuses
          (`pending_truncate`) suggests the rest of the jobs will be PENDING.
          Therefore, we can truncate our polling and restart at the beginning of
          list of job ids.

        By combining these techniques, we spend most of our time monitoring only
        running jobs (there could be a very large number of pending jobs), we
        stay under API rate limits, and we keep the compute in this thread low
        so as to not interfere with new submissions.
        """
        assert self.scheduler

        try:
            while self.is_running and (
                self.pending_k8s_jobs or self.arrayer.num_pending
            ):
                if self.scheduler.logger.level >= logging.DEBUG:
                    self.log(
                        f"Preparing {self.arrayer.num_pending} job(s) for Job Arrays.",
                        level=logging.DEBUG,
                    )
                    self.log(
                        f"Waiting on {len(self.pending_k8s_jobs)} K8S job(s): "
                        + " ".join(sorted(self.pending_k8s_jobs.keys())),
                        level=logging.DEBUG,
                    )

                # Copy pending_k8s_jobs.keys() since it can change due to new
                # submissions.
                pending_jobs = list(self.pending_k8s_jobs.keys())
                jobs = iter_k8s_job_status(pending_jobs, self.namespace)
                # changing this (IE, removing iter_k8s_job_status) breaks
                # inflight test jobs =
                # k8s_describe_jobs(list(self.pending_k8s_jobs.keys()))
                for job in jobs:
                    self._process_job_status(job)
                time.sleep(self.interval)

        except Exception as error:
            # Since we run this is method at the top-level of a thread, we need
            # to catch all exceptions so we can properly report them to the
            # scheduler.
            self.log("_monitor got exception", level=logging.INFO)
            self.scheduler.reject_job(None, error)

        self.log("Shutting down executor...", level=logging.DEBUG)
        self.stop()

    def _process_job_status(self, job: kubernetes.client.V1Job) -> None:
        """
        Process K8S job statuses.
        """
        assert self.scheduler
        job_status: Optional[str] = None

        if job.spec.parallelism is None or job.spec.parallelism == 1:
            # Should check status/reason to determine status.

            if job.status.succeeded is not None and job.status.succeeded > 0:
                job_status = SUCCEEDED
            elif job.status.failed is not None and job.status.failed > 0:
                job_status = FAILED
            elif job.status.conditions is not None:
                # Handle a special case where a job times out, but isn't counted
                # as a 'failure' in job.status.failed.  In this case we want to
                # reject the job early and skip reading logs from the pod
                # because the pod was already cleaned up
                conditions = job.status.conditions
                if conditions[0].type == "Failed":
                    job_status = FAILED
                    redun_job = self.pending_k8s_jobs.pop(job.metadata.name)
                    self.scheduler.reject_job(
                        redun_job,
                        K8STimeoutExceeded("Timeout exceeded"),
                        job_tags=[],
                    )
                    return
            else:
                return

            # Determine redun Job and job_labels.
            redun_job = self.pending_k8s_jobs.pop(job.metadata.name)
            k8s_labels = []
            k8s_labels.append(("k8s_job", job.metadata.uid))
            if job_status == SUCCEEDED:
                # Assume a recently completed job has valid results.
                result, exists = self._get_job_output(
                    redun_job, check_valid=False
                )
                if exists:
                    self.scheduler.done_job(
                        redun_job, result, job_tags=k8s_labels
                    )
                else:
                    # This can happen if job ended in an inconsistent state.
                    self.scheduler.reject_job(
                        redun_job,
                        FileNotFoundError(
                            aws_utils.get_job_scratch_file(
                                self.s3_scratch_prefix,
                                redun_job,
                                s3_utils.S3_SCRATCH_OUTPUT,
                            )
                        ),
                        job_tags=k8s_labels,
                    )
            elif job_status == FAILED:
                error, error_traceback = parse_task_error(
                    self.s3_scratch_prefix, redun_job
                )
                logs = [f"*** Logs for K8S job {job.metadata.uid}:\n"]

                try:
                    status_reason = job.status.conditions[0].message
                except (KeyError, IndexError, TypeError):
                    status_reason = ""

                # Job failed here, but without an exit code
                # Detect deadline exceeded here and raise exception.
                if status_reason:
                    logs.append(f"statusReason: {status_reason}\n")
                try:
                    logs.extend(
                        parse_task_logs(job.metadata.name, self.namespace)
                    )
                except Exception as e:
                    logs.append(
                        "Failed to parse task logs for: "
                        + job.metadata.name
                        + " "
                        + str(e)
                    )
                error_traceback.logs = logs
                self.scheduler.reject_job(
                    redun_job,
                    error,
                    error_traceback=error_traceback,
                    job_tags=k8s_labels,
                )
            api_instance = k8s_utils.get_k8s_batch_client()
            try:
                _ = k8s_utils.delete_job(
                    api_instance, job.metadata.name, self.namespace
                )
            except kubernetes.client.exceptions.ApiException as e:
                self.log(
                    "Failed to delete k8s job {job.metadata.name}: {e}",
                    level=logging.WARN,
                )
        else:
            label_selector = f"job-name={job.metadata.name}"
            k8s_pods = self.get_array_child_pods(job.metadata.name)

            # TODO(dek): check for job.status.conditions[*].ype == Failed and reason
            # for deadline exceeded, etc.
            redun_job = self.pending_k8s_jobs[job.metadata.name]
            for item in k8s_pods.items:
                pod_name = item.metadata.name
                pod_namespace = item.metadata.namespace
                index = int(
                    item.metadata.annotations[
                        "batch.kubernetes.io/job-completion-index"
                    ]
                )
                if index in redun_job:
                    container_statuses = item.status.container_statuses
                    if container_statuses is None:
                        continue
                    for container_status in container_statuses:
                        terminated = container_status.state.terminated
                        if terminated is None:
                            continue
                        if terminated.exit_code != 0:
                            api_instance = k8s_utils.get_k8s_core_client()
                            _ = api_instance.read_namespaced_pod_log(
                                pod_name, namespace=pod_namespace
                            )
                            # Detect deadline exceeded here and raise exception.
                            self.scheduler.reject_job(
                                redun_job[index],
                                K8SError(
                                    "K8S pod exited with error code {terminated.exit_code} because {terminated.reason}"
                                ),
                            )
                        else:
                            result, exists = self._get_job_output(
                                redun_job[index], check_valid=False
                            )
                            if exists:
                                self.scheduler.done_job(
                                    redun_job[index], result
                                )
                            else:
                                # This can happen if job ended in an
                                # inconsistent state.
                                self.scheduler.reject_job(
                                    redun_job[index],
                                    FileNotFoundError(
                                        aws_utils.get_job_scratch_file(
                                            self.s3_scratch_prefix,
                                            redun_job[index],
                                            s3_utils.S3_SCRATCH_OUTPUT,
                                        )
                                    ),
                                )
                        del self.pending_k8s_jobs[job.metadata.name][index]
                        if len(redun_job) == 0:
                            api_instance = k8s_utils.get_k8s_batch_client()
                            try:
                                _ = k8s_utils.delete_job(
                                    api_instance,
                                    job.metadata.name,
                                    self.namespace,
                                )
                            except kubernetes.client.exceptions.ApiException as e:
                                self.log(
                                    "Failed to delete k8s job {job.metadata.name}: {e}",
                                    level=logging.WARN,
                                )
                            del self.pending_k8s_jobs[job.metadata.name]

    def _get_job_options(self, job: Job) -> dict:
        """
        Determine the task options for a job.

        Task options can be specified at the job-level have precedence over the
        executor-level (within `redun.ini`):
        """
        assert job.task

        job_options = job.get_options()

        task_options = {
            **self.default_task_options,
            **job_options,
        }

        # Add default k8s labels to the job.
        if self.use_default_k8s_labels:
            execution = job.execution
            project = (
                execution.job.task.namespace
                if execution and execution.job and execution.job.task
                else ""
            )
            default_labels = {
                "redun_job_id": job.id,
                "redun_task_name": job.task.fullname,
                "redun_execution_id": execution.id if execution else "",
                "redun_project": project,
            }
        else:
            default_labels = {}

        # Merge k8s_labels if needed.
        k8s_labels = {
            **self.default_task_options.get("k8s_labels", {}),
            **default_labels,
            **job_options.get("k8s_labels", {}),
        }
        if k8s_labels:
            task_options["k8s_labels"] = k8s_labels

        return task_options

    def _get_job_output(
        self, job: Job, check_valid: bool = True
    ) -> Tuple[Any, bool]:
        """
        Return job output if it exists.

        Returns a tuple of (result, exists).
        """
        assert self.scheduler

        output_file = File(
            aws_utils.get_job_scratch_file(
                self.s3_scratch_prefix, job, s3_utils.S3_SCRATCH_OUTPUT
            )
        )
        if output_file.exists():
            result = aws_utils.parse_task_result(self.s3_scratch_prefix, job)
            if not check_valid or self.scheduler.is_valid_value(result):
                return result, True
        return None, False

    def _submit(self, job: Job, args: Tuple, kwargs: dict) -> None:
        """
        Submit Job to executor.
        """
        assert self.scheduler
        assert job.task

        # If this is the first submission gather inflight jobs. We also check
        # is_running here as a way of determining whether this is the first
        # submission or not. If we are already running, then we know we have
        # already had jobs submitted and done the inflight check so no reason to
        # do that again here.
        if not self.is_running:
            # Precompute existing inflight jobs for job reuniting.
            self.gather_inflight_jobs()
        # Package code if necessary and we have not already done so. If
        # code_package is False, then we can skip this step. Additionally, if we
        # have already packaged and set code_file, then we do not need to
        # repackage.
        if self.code_package is not False and self.code_file is None:
            code_package = self.code_package or {}
            assert isinstance(code_package, dict)
            self.code_file = aws_utils.package_code(
                self.s3_scratch_prefix, code_package
            )

        job_dir = aws_utils.get_job_scratch_dir(self.s3_scratch_prefix, job)
        job_type = "K8S job"

        # Determine job options.
        task_options = self._get_job_options(job)
        use_cache = task_options.get("cache", True)

        # # Determine if we can reunite with a previous K8S output or job.
        k8s_job_id: Optional[str] = None
        if use_cache and job.eval_hash in self.preexisting_k8s_jobs:
            k8s_job_id = self.preexisting_k8s_jobs.pop(job.eval_hash)
            # Handle both single and array jobs.
            if type(k8s_job_id) != tuple:
                # Single job case Make sure k8s API still has a status on this
                # job.
                job_name = task_options["job_name_prefix"] + "-" + job.eval_hash
                existing_jobs = k8s_describe_jobs(
                    [job_name], namespace=self.namespace
                )
                existing_job = existing_jobs[0]  # should be index
                # Reunite with inflight k8s job, if present.
                if existing_job:
                    k8s_job_id = existing_job.metadata.uid
                    self.log(
                        "reunite redun job {redun_job} with {job_type} {k8s_job_id}:\n"
                        "  s3_scratch_path = {job_dir}".format(
                            redun_job=job.id,
                            job_type=job_type,
                            k8s_job_id=k8s_job_id,
                            job_dir=job_dir,
                        )
                    )
                    assert k8s_job_id
                    self.pending_k8s_jobs[job_name] = job
                else:
                    k8s_job_id = None
            else:
                # Array job case
                (
                    child_job_id,
                    child_job_name,
                    child_job_index,
                    parent_hash,
                ) = k8s_job_id
                job_name = (
                    f"{task_options['job_name_prefix']}-{parent_hash}-array"
                )
                existing_jobs = k8s_describe_jobs(
                    [job_name], namespace=self.namespace
                )
                existing_job = existing_jobs[0]
                # Reunite with inflight k8s job, if present.
                if existing_job:
                    k8s_job_id = existing_job.metadata.uid
                    self.log(
                        "reunite redun job {redun_job} with {job_type} {k8s_job_id}:\n"
                        "  s3_scratch_path = {job_dir}".format(
                            redun_job=job.id,
                            job_type=job_type,
                            k8s_job_id=k8s_job_id,
                            job_dir=job_dir,
                        )
                    )
                    assert k8s_job_id
                    if job_name not in self.pending_k8s_jobs:
                        self.pending_k8s_jobs[job_name] = {}
                    self.pending_k8s_jobs[job_name][child_job_index] = job
                else:
                    k8s_job_id = None

        # Job arrayer will handle actual submission after bunching to an array
        # job, if necessary.
        if k8s_job_id is None:
            self.arrayer.add_job(job, args, kwargs)

        self._start()

    def _submit_array_job(
        self, jobs: List[Job], all_args: List[Tuple], all_kwargs: List[Dict]
    ) -> str:
        """Submits an array job, returning job name uuid"""
        array_size = len(jobs)
        assert array_size == len(all_args) == len(all_kwargs)

        # All jobs identical so just grab the first one
        job = jobs[0]
        assert job.task
        if job.task.script:
            raise NotImplementedError("Array jobs not supported for scripts")

        task_options = self._get_job_options(job)
        image = task_options.pop("image", self.image)
        namespace = task_options.pop("namespace", self.namespace)
        # Generate a unique name for job with no '-' to simplify job name
        # parsing.
        array_uuid = str(uuid.uuid4()).replace("-", "")

        job_type = "K8S job"

        # Setup input, output and error path files. Input file is a pickled list
        # of args, and kwargs, for each child job.
        input_file = aws_utils.get_array_scratch_file(
            self.s3_scratch_prefix, array_uuid, s3_utils.S3_SCRATCH_INPUT
        )
        with File(input_file).open("wb") as out:
            pickle_dump([all_args, all_kwargs], out)

        # Output file is a plaintext list of output paths, for each child job.
        output_file = aws_utils.get_array_scratch_file(
            self.s3_scratch_prefix, array_uuid, s3_utils.S3_SCRATCH_OUTPUT
        )
        output_paths = [
            aws_utils.get_job_scratch_file(
                self.s3_scratch_prefix, job, s3_utils.S3_SCRATCH_OUTPUT
            )
            for job in jobs
        ]
        with File(output_file).open("w") as ofile:
            json.dump(output_paths, ofile)

        # Error file is a plaintext list of error paths, one for each child job.
        error_file = aws_utils.get_array_scratch_file(
            self.s3_scratch_prefix, array_uuid, s3_utils.S3_SCRATCH_ERROR
        )
        error_paths = [
            aws_utils.get_job_scratch_file(
                self.s3_scratch_prefix, job, s3_utils.S3_SCRATCH_ERROR
            )
            for job in jobs
        ]
        with File(error_file).open("w") as efile:
            json.dump(error_paths, efile)

        # Eval hash file is plaintext hashes of child jobs for matching for job
        # reuniting.
        eval_file = aws_utils.get_array_scratch_file(
            self.s3_scratch_prefix, array_uuid, s3_utils.S3_SCRATCH_HASHES
        )
        with File(eval_file).open("w") as eval_f:
            eval_f.write("\n".join([job.eval_hash for job in jobs]))  # type: ignore

        k8s_resp = submit_task(
            image,
            namespace,
            self.s3_scratch_prefix,
            job,
            job.task,
            job_options=task_options,
            code_file=self.code_file,
            array_uuid=array_uuid,
            array_size=array_size,
        )

        # Add entire array to array jobs, and all jobs in array to pending jobs.
        array_job_name = k8s_resp.metadata.name
        self.pending_k8s_jobs[array_job_name] = {}
        for i in range(array_size):
            self.pending_k8s_jobs[array_job_name][i] = jobs[i]
        array_job_id = k8s_resp.metadata.uid

        self.log(
            "submit {array_size} redun job(s) as {job_type} {k8s_job_id}:\n"
            "  array_job_id    = {array_job_id}\n"
            "  array_job_name  = {job_name}\n"
            "  array_size      = {array_size}\n"
            "  s3_scratch_path = {job_dir}\n"
            "  retry_attempts  = {retries}".format(
                array_job_id=array_job_id,
                job_type=job_type,
                array_size=array_size,
                k8s_job_id=array_job_id,
                job_dir=aws_utils.get_array_scratch_file(
                    self.s3_scratch_prefix, array_uuid, ""
                ),
                job_name=array_job_name,
                retries=task_options["retries"],
            )
        )
        return array_uuid

    def _submit_single_job(self, job: Job, args: Tuple, kwargs: dict) -> None:
        """
                Actually submits a job.  Caching detects if it should be part
        +        of an array job
        """
        assert job.task
        task_options = self._get_job_options(job)
        image = task_options.pop("image", self.image)
        namespace = task_options.pop("namespace", self.namespace)

        job_dir = aws_utils.get_job_scratch_dir(self.s3_scratch_prefix, job)
        job_type = "K8S job"

        # Submit a new Batch job.
        if not job.task.script:
            k8s_resp = submit_task(
                image,
                namespace,
                self.s3_scratch_prefix,
                job,
                job.task,
                args=args,
                kwargs=kwargs,
                job_options=task_options,
                code_file=self.code_file,
            )
        else:
            command = get_task_command(job.task, args, kwargs)
            k8s_resp = submit_command(
                image,
                namespace,
                self.s3_scratch_prefix,
                job,
                command,
                job_options=task_options,
            )

        k8s_job_id = k8s_resp.metadata.uid
        job_name = k8s_resp.metadata.name
        self.log(
            "submit redun job {redun_job} as {job_type} {k8s_job_id}:\n"
            "  job_id          = {k8s_job_id}\n"
            "  job_name        = {job_name}\n"
            "  s3_scratch_path = {job_dir}\n"
            "  retry_attempts  = {retries}".format(
                redun_job=job.id,
                job_type=job_type,
                k8s_job_id=k8s_job_id,
                job_dir=job_dir,
                job_name=job_name,
                retries=task_options["retries"],
            )
        )
        self.pending_k8s_jobs[job_name] = job

    def submit(self, job: Job, args: Tuple, kwargs: dict) -> None:
        """
        Submit Job to executor.
        """
        return self._submit(job, args, kwargs)

    def submit_script(self, job: Job, args: Tuple, kwargs: dict) -> None:
        """
        Submit Job for script task to executor.
        """
        return self._submit(job, args, kwargs)

    def get_jobs(self) -> Iterator[dict]:
        """
        Returns K8S Job statuses from the AWS API.
        """
        api_instance = k8s_utils.get_k8s_batch_client()
        # We don't currently filter on "inflight" jobs, but that's what the
        # aws_batch implementation does.
        api_response = api_instance.list_job_for_all_namespaces(watch=False)
        return api_response

    def get_array_child_pods(self, job_name: str) -> List[Dict[str, Any]]:
        """Get all the pods for a job array"""
        api_instance = k8s_utils.get_k8s_core_client()
        label_selector = f"job-name={job_name}"
        api_response = api_instance.list_pod_for_all_namespaces(
            watch=False, label_selector=label_selector
        )
        return api_response
