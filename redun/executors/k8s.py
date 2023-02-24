import json
import logging
import os
import re
import threading
import time
import uuid
from collections import OrderedDict, defaultdict
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple, TypeVar, Union, cast

import kubernetes
from kubernetes.client import CoreV1Api, V1Job, V1Pod
from kubernetes.client.exceptions import ApiException

from redun.executors import aws_utils, k8s_utils
from redun.executors.base import Executor, register_executor
from redun.executors.code_packaging import package_code, parse_code_package_config
from redun.executors.command import get_oneshot_command, get_script_task_command
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
from redun.logging import logger
from redun.scheduler import Job, Scheduler
from redun.scripting import get_task_command
from redun.task import CacheScope, Task
from redun.utils import pickle_dump

T = TypeVar("T")
SUCCEEDED = "SUCCEEDED"
FAILED = "FAILED"
ARRAY_JOB_SUFFIX = "array"


def k8s_submit(
    k8s_client: k8s_utils.K8SClient,
    command: List[str],
    image: str,
    namespace: str,
    job_name: str = "k8s-job",
    array_size: int = 0,
    memory: int = 4,
    vcpus: int = 1,
    gpus: int = 0,
    timeout: Optional[int] = None,
    k8s_labels: Optional[Dict[str, str]] = None,
    retries: int = 1,
    service_account_name: str = "default",
    annotations: Optional[Dict[str, str]] = None,
    secret_name: Optional[str] = None,
) -> kubernetes.client.V1Job:
    """Prepares and submits a k8s job to the API server"""
    requests = {
        "memory": f"{memory}G",
        "cpu": vcpus,
        "nvidia.com/gpu": gpus,
    }
    limits = requests
    resources = k8s_utils.create_resources(requests, limits)

    k8s_job = k8s_utils.create_job_object(
        name=job_name,
        image=image,
        command=command,
        resources=resources,
        timeout=timeout,
        labels=k8s_labels,
        service_account_name=service_account_name,
        annotations=annotations,
        secret_name=secret_name,
    )

    if array_size > 1:
        k8s_job.spec.completions = array_size
        k8s_job.spec.parallelism = array_size
        k8s_job.spec.completion_mode = "Indexed"
        k8s_job.spec.backoff_limit = retries * array_size
    else:
        k8s_job.spec.backoff_limit = retries
    k8s_job.spec.restart_policy = "OnFailure"

    return k8s_utils.create_job(k8s_client, k8s_job, namespace=namespace)


def get_k8s_job_name(prefix: str, job_hash: str, array: bool = False) -> str:
    """
    Return a K8S Job name by either job or job hash.
    """
    return "{}-{}{}".format(prefix, job_hash, f"-{ARRAY_JOB_SUFFIX}" if array else "")


def is_array_job_name(job_name: str) -> bool:
    "Returns true if the job name looks like an array job"
    return job_name.endswith(f"-{ARRAY_JOB_SUFFIX}")


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
    keys = [
        "memory",
        "vcpus",
        "gpus",
        "k8s_labels",
        "annotations",
        "service_account_name",
        "retries",
        "timeout",
    ]
    return {key: job_options[key] for key in keys if key in job_options}


def submit_task(
    k8s_client: k8s_utils.K8SClient,
    image: str,
    namespace: str,
    scratch_prefix: str,
    job: Job,
    a_task: Task,
    args: Tuple = (),
    kwargs: Dict[str, Any] = {},
    job_options: dict = {},
    array_uuid: Optional[str] = None,
    array_size: int = 0,
    code_file: Optional[File] = None,
    secret_name: Optional[str] = None,
) -> kubernetes.client.V1Job:
    """
    Submit a redun Task to K8S.
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
        k8s_client,
        command,
        image=image,
        namespace=namespace,
        job_name=job_name,
        array_size=array_size,
        secret_name=secret_name,
        **get_k8s_job_options(job_options),
    )

    return result


def submit_command(
    k8s_client: k8s_utils.K8SClient,
    image: str,
    namespace: str,
    scratch_prefix: str,
    job: Job,
    command: str,
    job_options: dict = {},
    secret_name: Optional[str] = None,
) -> kubernetes.client.V1Job:
    """
    Submit a shell command to K8S
    """
    shell_command = get_script_task_command(
        scratch_prefix,
        job,
        command,
        exit_command="exit 1",
    )

    # Submit to K8S.
    assert job.eval_hash
    job_name = get_k8s_job_name(job_options.get("job_name_prefix", "k8s-job"), job.eval_hash)
    return k8s_submit(
        k8s_client,
        shell_command,
        image=image,
        namespace=namespace,
        job_name=job_name,
        secret_name=secret_name,
        **get_k8s_job_options(job_options),
    )


def k8s_paginate(request_func: Callable) -> Iterator:
    """
    Yields items from a paginated k8s API endpoint.
    """
    token: Optional[str] = None
    while True:
        response = request_func(token)
        if response.items:
            yield from response.items
        if response.metadata._continue:
            token = response.metadata._continue
        else:
            break


def k8s_list_jobs(k8s_client: k8s_utils.K8SClient) -> Iterator[V1Job]:
    """
    Returns active K8S Jobs.
    """
    yield from k8s_paginate(
        lambda _continue: k8s_client.batch.list_job_for_all_namespaces(
            watch=False, _continue=_continue
        )
    )


def k8s_describe_jobs(
    k8s_client: k8s_utils.K8SClient, job_names: List[str], namespace: str
) -> List[kubernetes.client.V1Job]:
    """
    Returns K8S Job descriptions.
    """
    batch_api = k8s_client.batch
    jobs = []
    for job_name in job_names:
        try:
            jobs.append(batch_api.read_namespaced_job(job_name, namespace=namespace))
        except ApiException:
            # TODO: Decide what to do.
            raise
    return jobs


def get_pod_logs(
    k8s_client: k8s_utils.K8SClient, pod: kubernetes.client.V1Pod, max_lines: Optional[int] = None
) -> List[str]:
    """
    Returns the logs of a K8S pod.
    """
    kwargs = {}
    if max_lines is not None:
        kwargs["tail_lines"] = max_lines

    log_response = k8s_client.core.read_namespaced_pod_log(
        pod.metadata.name, namespace=pod.metadata.namespace, timestamps=True, **kwargs
    )
    lines = log_response.split("\n")

    # Use latest container status.
    state = pod.status.container_statuses[-1].state.terminated
    if state and state.exit_code != 0:
        lines.append(f"Exit {state.exit_code} ({state.reason}): {state.message}")

    return lines


def parse_pod_logs(
    k8s_client: k8s_utils.K8SClient, pod: kubernetes.client.V1Pod, max_lines: int = 1000
) -> Iterator[str]:
    """
    Iterates through most recent logs of an K8S Job.
    """
    lines = get_pod_logs(k8s_client, pod, max_lines=max_lines)
    if len(lines) < max_lines:
        yield "\n*** Earlier logs are truncated ***\n"
    yield from lines


def get_k8s_job_pods(core_api: CoreV1Api, job_name: str) -> Iterator[V1Pod]:
    """
    Iterates the pods for a k8s job.
    """
    yield from k8s_paginate(
        lambda _continue: core_api.list_pod_for_all_namespaces(
            watch=False,
            label_selector=f"job-name={job_name}",
            _continue=_continue,
        )
    )


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

    def __init__(self, name: str, scheduler: Optional[Scheduler] = None, config=None):
        super().__init__(name, scheduler=scheduler)
        if config is None:
            raise ValueError("K8SExecutor requires config.")

        # Required config.
        self.image = config["image"]
        self.namespace = config.get("namespace", "default")
        self.scratch_prefix = config["scratch"]

        # Optional config.
        self.code_package = parse_code_package_config(config)
        self.secret_name: Optional[str] = config.get("secret_name")
        self.use_import_aws_secrets: bool = config.getboolean("import_aws_secrets", fallback=True)
        self.secret_env_vars = config.get("secret_env_vars", fallback="").strip().split()

        self.code_file: Optional[File] = None

        # Default task options.
        self.default_task_options = {
            "vcpus": config.getint("vcpus", 1),
            "memory": config.getfloat("memory", 4),
            "retries": config.getint("retries", 1),
            "service_account_name": config.get("service_account_name", "default"),
            "job_name_prefix": config.get("job_name_prefix", k8s_utils.DEFAULT_JOB_PREFIX),
        }
        if config.get("annotations"):
            self.default_task_options["annotations"] = json.loads(config.get("annotations"))
        if config.get("k8s_labels"):
            self.default_task_options["k8s_labels"] = json.loads(config.get("k8s_labels"))
        self.use_default_k8s_labels = config.getboolean("default_k8s_labels", True)

        self.is_running = False
        self._k8s_client = k8s_utils.K8SClient()
        # We use an OrderedDict in order to retain submission order.
        self.pending_k8s_jobs: Dict[str, Union[Job, Dict[int, Job]]] = OrderedDict()
        self.preexisting_k8s_jobs: Dict[
            str, Union[str, Tuple[str, str, int, str]]
        ] = {}  # Job hash -> Job ID

        self.interval = config.getfloat("job_monitor_interval", 5.0)

        min_array_size = config.getint("min_array_size", 5)
        max_array_size = config.getint("max_array_size", 1000)

        major, minor = self._k8s_client.version()
        if major == 1 and minor < 21:
            # Versions prior to 1.21 didn't support indexed jobs
            # (https://kubernetes.io/docs/tasks/job/indexed-parallel-processing-static/)
            logger.warn(
                "kubernetes server version is too old for indexed k8s jobs, "
                "defaulting to individual redun jobs"
            )
            min_array_size = 0
            max_array_size = 0

        self.arrayer = JobArrayer(
            self._submit_jobs,
            self._on_error,
            submit_interval=self.interval,
            stale_time=config.getfloat("job_stale_time", 3.0),
            min_array_size=min_array_size,
            max_array_size=max_array_size,
        )

    def _submit_jobs(self, jobs: List[Job]) -> None:
        """
        Callback for JobArrayer to return arrays of Jobs.
        """
        if len(jobs) == 1:
            # Singleton jobs are given as single element lists.
            self._submit_single_job(jobs[0])
        else:
            self._submit_array_job(jobs)

    def _on_error(self, error: Exception) -> None:
        """
        Callback for JobArrayer to report scheduler-level errors.
        """
        assert self._scheduler
        self._scheduler.reject_job(None, error)

    def gather_inflight_jobs(self) -> None:
        """Collect existing k8s jobs and match them up to redun jobs"""
        running_arrays: Dict[str, List[Tuple[str, str, int]]] = defaultdict(list)

        # We don't currently filter on "inflight" jobs, but that's what the
        # aws_batch implementation does.
        jobs = self.get_jobs()

        # Get all running jobs by name.
        for job in jobs:
            job_name = job.metadata.name
            if not is_array_job_name(job_name):
                job_hash = get_hash_from_job_name(job_name)
                if job_hash:
                    self.preexisting_k8s_jobs[job_hash] = job.metadata.uid
                continue

            # Get all child pods of running array jobs for reuniting.
            child_pods = get_k8s_job_pods(self._k8s_client.core, job.metadata.name)
            running_arrays[job_name].extend(
                (
                    child_pod.metadata.name,
                    child_pod.metadata.uid,
                    int(
                        child_pod.metadata.annotations["batch.kubernetes.io/job-completion-index"]
                    ),
                )
                for child_pod in child_pods
            )

        # Match up running array jobs with consistent redun job naming scheme.
        for array_name, child_pod_indices in running_arrays.items():
            # Get path to array file directory on S3 from array job name.
            parent_hash = get_hash_from_job_name(array_name)
            if not parent_hash:
                continue
            eval_file = File(
                get_array_scratch_file(
                    self.scratch_prefix,
                    parent_hash,
                    SCRATCH_HASHES,
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

    def _setup_secrets(self) -> None:
        """
        Setup secrets for k8s jobs.
        """
        if not self.secret_name:
            # If secret name is not given, do not setup secrets.
            return

        # Determine env vars to import through a k8s secret.
        env_vars = {}

        # Import AWS secrets.
        if self.use_import_aws_secrets:
            env_vars.update(aws_utils.get_aws_env_vars())

        # Import local env vars into secret.
        for env_var in self.secret_env_vars:
            value = os.environ.get(env_var)
            if value:
                env_vars[env_var] = value

        k8s_utils.create_k8s_secret(self._k8s_client, self.secret_name, self.namespace, env_vars)

    def _start(self) -> None:
        """
        Start monitoring thread.
        """
        if self.is_running:
            return

        self.is_running = True

        # Ensure k8s namespace exists.
        k8s_utils.create_namespace(self._k8s_client, self.namespace)

        self._setup_secrets()

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
            while self.is_running and (self.pending_k8s_jobs or self.arrayer.num_pending):
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
                jobs = k8s_describe_jobs(self._k8s_client, pending_jobs, self.namespace)
                # changing this (IE, removing iter_k8s_job_status) breaks
                # inflight test jobs =
                for job in jobs:
                    self._process_k8s_job_status(job)
                time.sleep(self.interval)

        except Exception as error:
            # Since we run this is method at the top-level of a thread, we need
            # to catch all exceptions so we can properly report them to the
            # scheduler.
            self.log("_monitor got exception", level=logging.INFO)
            self._scheduler.reject_job(None, error)

        self.log("Shutting down executor...", level=logging.DEBUG)
        self.stop()

    def _process_pod_status(self, pod: V1Pod) -> Tuple[Optional[str], Optional[str]]:
        """
        Determine if there was a failure at the pod-level.

        Returns (job_status, status_reason)
        """

        # Get container states for most recent pod.
        # TODO: check sorting.
        if not pod.status.container_statuses:
            # No container yet, so do nothing.
            return None, None
        state = pod.status.container_statuses[0].state

        # Detect-specific pod-level errors that aren't caught at the job-level.
        ERROR_STATES = {"ErrImagePull", "FailedScheduling"}
        if state.waiting and state.waiting.reason in ERROR_STATES:
            return FAILED, f"{state.waiting.reason}: {state.waiting.message}"

        elif state.terminated:
            if state.terminated.exit_code == 0:
                return SUCCEEDED, None
            else:
                return FAILED, f"{state.terminated.reason}: {state.terminated.message}"

        return None, None

    def _process_single_k8s_job_status(self, job: V1Job) -> Tuple[Optional[str], Optional[str]]:
        """
        Determine status of a non-array k8s job.
        """
        if job.status.succeeded is not None and job.status.succeeded > 0:
            return SUCCEEDED, None

        elif job.status.failed is not None and job.status.failed > 0:
            try:
                status_reason = job.status.conditions[0].message
            except (KeyError, IndexError, TypeError):
                status_reason = "Job failed."

            return FAILED, f"Error: {status_reason}"

        elif job.status.conditions is not None:
            # Handle a special case where a job times out, but isn't counted
            # as a 'failure' in job.status.failed.  In this case we want to
            # reject the job early and skip reading logs from the pod
            # because the pod was already cleaned up.
            if job.status.conditions[0].type == "Failed":
                return FAILED, "Failed: Timeout exceeded."
            else:
                return None, None
        else:
            return None, None

    def _process_redun_job(
        self,
        job: Job,
        pod: V1Pod,
        job_status: str,
        status_reason: Optional[str],
        k8s_labels: List[Tuple[str, str]] = [],
    ) -> None:
        """
        Complete a redun job (done or reject).
        """
        assert self._scheduler

        if job_status == SUCCEEDED:
            # Assume a recently completed job has valid results.
            result, exists = parse_job_result(self.scratch_prefix, job)
            if exists:
                self._scheduler.done_job(job, result, job_tags=k8s_labels)
            else:
                # This can happen if job ended in an inconsistent state.
                self._scheduler.reject_job(
                    job,
                    FileNotFoundError(
                        get_job_scratch_file(
                            self.scratch_prefix,
                            job,
                            SCRATCH_OUTPUT,
                        )
                    ),
                    job_tags=k8s_labels,
                )

        elif job_status == FAILED:
            error, error_traceback = parse_job_error(self.scratch_prefix, job)
            logs = [f"*** Logs for K8S pod {pod.metadata.name}:\n"]

            # TODO: Consider displaying events in the logs since this can have
            # helpful info as well.
            # core_api = k8s_utils.get_k8s_core_client()
            # events = core_api.list_namespaced_event(
            #     pod.metadata.namespace, field_selector=f"involvedObject.name={pod.metadata.name}"
            # )

            # Job failed here, but without an exit code
            # Detect deadline exceeded here and raise exception.
            if status_reason:
                logs.append(f"statusReason: {status_reason}\n")
            try:
                logs.extend(parse_pod_logs(self._k8s_client, pod))
            except Exception as e:
                logs.append(f"Failed to parse task logs for redun job {job.id}: {e}")

            error_traceback.logs = logs
            self._scheduler.reject_job(
                job,
                error,
                error_traceback=error_traceback,
                job_tags=k8s_labels,
            )

        else:
            raise AssertionError(f"Unexpected job_status: {job_status}")

    def _process_k8s_job_status(self, job: kubernetes.client.V1Job) -> None:
        """
        Process K8S job statuses.
        """
        if job.spec.parallelism is None or job.spec.parallelism == 1:
            # Check status of single k8s job.
            pods = list(get_k8s_job_pods(self._k8s_client.core, job.metadata.name))

            if not pods:
                # No pods yet, do nothing.
                return

            # Use most recent pod. TODO: double check sorting.
            pod = pods[0]

            job_status, status_reason = self._process_single_k8s_job_status(job)
            if not job_status:
                # If job does not have status, fallback to pod.
                job_status, status_reason = self._process_pod_status(pod)
                if not job_status:
                    # Job is still running.
                    return

            # Determine redun Job and job_labels.
            redun_job: Job = cast(Job, self.pending_k8s_jobs.pop(job.metadata.name))

            k8s_labels = [("k8s_job", job.metadata.uid)]
            self._process_redun_job(redun_job, pod, job_status, status_reason, k8s_labels)

            # Clean up k8s job immediately.
            try:
                k8s_utils.delete_job(self._k8s_client, job.metadata.name, self.namespace)
            except kubernetes.client.exceptions.ApiException as e:
                self.log(
                    f"Failed to delete k8s job {job.metadata.name}: {e}",
                    level=logging.WARN,
                )
        else:
            # Check status of array k8s job.
            k8s_pods = get_k8s_job_pods(self._k8s_client.core, job.metadata.name)
            redun_jobs = cast(Dict[int, Job], self.pending_k8s_jobs[job.metadata.name])

            for pod in k8s_pods:
                job_status, status_reason = self._process_pod_status(pod)
                if not job_status:
                    # Job is still running.
                    return

                if "batch.kubernetes.io/job-completion-index" not in pod.metadata.annotations:
                    self.log(
                        f"Pod {pod.metadata.name} is missing job-completion-index:",
                        pod.metadata.annotations,
                    )
                    continue

                index = int(pod.metadata.annotations["batch.kubernetes.io/job-completion-index"])
                redun_job2: Optional[Job] = redun_jobs.pop(index, None)
                if redun_job2:
                    k8s_labels = [("k8s_job", job.metadata.uid)]
                    self._process_redun_job(redun_job2, pod, job_status, status_reason, k8s_labels)

                # When the last pod finishes, clean up the k8s job.
                if len(redun_jobs) == 0:
                    try:
                        k8s_utils.delete_job(
                            self._k8s_client, job.metadata.name, job.metadata.namespace
                        )
                    except kubernetes.client.exceptions.ApiException as e:
                        self.log(
                            f"Failed to delete k8s job {job.metadata.name}: {e}",
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

    def _submit(self, job: Job) -> None:
        """
        Submit Job to executor.
        """
        assert self._scheduler
        assert job.args
        args, kargs = job.args

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
            self.code_file = package_code(self.scratch_prefix, code_package)

        job_dir = get_job_scratch_dir(self.scratch_prefix, job)
        job_type = "K8S job"

        # Determine job options.
        task_options = self._get_job_options(job)
        cache_scope = CacheScope(task_options.get("cache_scope", CacheScope.BACKEND))

        # Determine if we can reunite with a previous K8S output or job.
        k8s_job_id: Optional[Union[str, Tuple[str, str, int, str]]] = None
        if cache_scope == CacheScope.BACKEND and job.eval_hash in self.preexisting_k8s_jobs:
            k8s_job_id = self.preexisting_k8s_jobs.pop(job.eval_hash)

            # Handle both single and array jobs.
            if isinstance(k8s_job_id, str):
                # Single job case Make sure k8s API still has a status on this
                # job.
                job_name = task_options["job_name_prefix"] + "-" + job.eval_hash
                existing_jobs = k8s_describe_jobs(
                    self._k8s_client, [job_name], namespace=self.namespace
                )
                existing_job = existing_jobs[0]  # should be index
                # Reunite with inflight k8s job, if present.
                if existing_job:
                    k8s_job_id = existing_job.metadata.uid
                    self.log(
                        "reunite redun job {redun_job} with {job_type} {k8s_job_id}:\n"
                        "  scratch_path = {job_dir}".format(
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

            elif isinstance(k8s_job_id, tuple):
                # Array job case
                _, _, child_job_index, parent_hash = cast(Tuple[str, str, int, str], k8s_job_id)
                job_name = f"{task_options['job_name_prefix']}-{parent_hash}-array"
                existing_jobs = k8s_describe_jobs(
                    self._k8s_client, [job_name], namespace=self.namespace
                )
                # Reunite with inflight k8s job, if present.
                if existing_jobs:
                    existing_job = existing_jobs[0]
                    k8s_job_id = existing_job.metadata.uid
                    self.log(
                        "reunite redun job {redun_job} with {job_type} {k8s_job_id}:\n"
                        "  scratch_path = {job_dir}".format(
                            redun_job=job.id,
                            job_type=job_type,
                            k8s_job_id=k8s_job_id,
                            job_dir=job_dir,
                        )
                    )
                    assert k8s_job_id
                    if job_name not in self.pending_k8s_jobs:
                        self.pending_k8s_jobs[job_name] = cast(Dict[int, Job], {})
                    cast(Dict[int, Job], self.pending_k8s_jobs[job_name])[child_job_index] = job
                else:
                    k8s_job_id = None
            else:
                raise AssertionError(f"Unknown k8s_job_id type: {type(k8s_job_id)}")

        # Job arrayer will handle actual submission after bunching to an array
        # job, if necessary.
        if k8s_job_id is None:
            self.arrayer.add_job(job)

        self._start()

    def _submit_array_job(self, jobs: List[Job]) -> str:
        """Submits an array job, returning job name uuid"""
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

        task_options = self._get_job_options(job)
        image = task_options.pop("image", self.image)
        namespace = task_options.pop("namespace", self.namespace)
        # Generate a unique name for job with no '-' to simplify job name
        # parsing.
        array_uuid = str(uuid.uuid4()).replace("-", "")

        job_type = "K8S job"

        # Setup input, output and error path files. Input file is a pickled list
        # of args, and kwargs, for each child job.
        input_file = get_array_scratch_file(self.scratch_prefix, array_uuid, SCRATCH_INPUT)
        with File(input_file).open("wb") as out:
            pickle_dump([all_args, all_kwargs], out)

        # Output file is a plaintext list of output paths, for each child job.
        output_file = get_array_scratch_file(self.scratch_prefix, array_uuid, SCRATCH_OUTPUT)
        output_paths = [
            get_job_scratch_file(self.scratch_prefix, job, SCRATCH_OUTPUT) for job in jobs
        ]
        with File(output_file).open("w") as ofile:
            json.dump(output_paths, ofile)

        # Error file is a plaintext list of error paths, one for each child job.
        error_file = get_array_scratch_file(self.scratch_prefix, array_uuid, SCRATCH_ERROR)
        error_paths = [
            get_job_scratch_file(self.scratch_prefix, job, SCRATCH_ERROR) for job in jobs
        ]
        with File(error_file).open("w") as efile:
            json.dump(error_paths, efile)

        # Eval hash file is plaintext hashes of child jobs for matching for job
        # reuniting.
        eval_file = get_array_scratch_file(self.scratch_prefix, array_uuid, SCRATCH_HASHES)
        with File(eval_file).open("w") as eval_f:
            eval_f.write("\n".join([job.eval_hash for job in jobs]))  # type: ignore

        k8s_resp = submit_task(
            self._k8s_client,
            image,
            namespace,
            self.scratch_prefix,
            job,
            job.task,
            job_options=task_options,
            code_file=self.code_file,
            array_uuid=array_uuid,
            array_size=array_size,
            secret_name=self.secret_name,
        )

        # Add entire array to array jobs, and all jobs in array to pending jobs.
        array_job_name = k8s_resp.metadata.name
        self.pending_k8s_jobs[array_job_name] = {}
        for i in range(array_size):
            cast(Dict[int, Job], self.pending_k8s_jobs[array_job_name])[i] = jobs[i]
        array_job_id = k8s_resp.metadata.uid

        self.log(
            "submit {array_size} redun job(s) as {job_type} {k8s_job_id}:\n"
            "  array_job_id   = {array_job_id}\n"
            "  array_job_name = {job_name}\n"
            "  array_size     = {array_size}\n"
            "  scratch_path   = {job_dir}".format(
                array_job_id=array_job_id,
                job_type=job_type,
                array_size=array_size,
                k8s_job_id=array_job_id,
                job_dir=get_array_scratch_file(self.scratch_prefix, array_uuid, ""),
                job_name=array_job_name,
            )
        )
        return array_uuid

    def _submit_single_job(self, job: Job) -> None:
        """
        Actually submits a job.  Caching detects if it should be part of an array job.
        """
        assert job.args
        args, kwargs = job.args
        task_options = self._get_job_options(job)
        image = task_options.pop("image", self.image)
        namespace = task_options.pop("namespace", self.namespace)

        job_dir = get_job_scratch_dir(self.scratch_prefix, job)
        job_type = "K8S job"

        # Submit a new Batch job.
        if not job.task.script:
            k8s_resp = submit_task(
                self._k8s_client,
                image,
                namespace,
                self.scratch_prefix,
                job,
                job.task,
                args=args,
                kwargs=kwargs,
                job_options=task_options,
                code_file=self.code_file,
                secret_name=self.secret_name,
            )
        else:
            command = get_task_command(job.task, args, kwargs)
            k8s_resp = submit_command(
                self._k8s_client,
                image,
                namespace,
                self.scratch_prefix,
                job,
                command,
                job_options=task_options,
                secret_name=self.secret_name,
            )

        k8s_job_id = k8s_resp.metadata.uid
        job_name = k8s_resp.metadata.name
        self.log(
            "submit redun job {redun_job} as {job_type} {k8s_job_id}:\n"
            "  job_id       = {k8s_job_id}\n"
            "  job_name     = {job_name}\n"
            "  scratch_path = {job_dir}".format(
                redun_job=job.id,
                job_type=job_type,
                k8s_job_id=k8s_job_id,
                job_dir=job_dir,
                job_name=job_name,
            )
        )
        self.pending_k8s_jobs[job_name] = job

    def submit(self, job: Job) -> None:
        """
        Submit Job to executor.
        """
        return self._submit(job)

    def submit_script(self, job: Job) -> None:
        """
        Submit Job for script task to executor.
        """
        return self._submit(job)

    def get_jobs(self) -> Iterator[V1Job]:
        """
        Iterates active k8s jobs.
        """
        return k8s_list_jobs(self._k8s_client)
