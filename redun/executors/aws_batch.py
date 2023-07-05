import copy
import datetime
import json
import logging
import re
import subprocess
import threading
import time
import uuid
from collections import OrderedDict, defaultdict
from configparser import SectionProxy
from functools import lru_cache
from itertools import islice
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple, cast

import boto3

from redun.executors import aws_utils
from redun.executors.base import Executor, register_executor
from redun.executors.code_packaging import package_code, parse_code_package_config
from redun.executors.command import get_oneshot_command, get_script_task_command
from redun.executors.docker import DockerExecutor, get_docker_executor_config
from redun.executors.scratch import (
    SCRATCH_ERROR,
    SCRATCH_HASHES,
    SCRATCH_INPUT,
    SCRATCH_OUTPUT,
    SCRATCH_STATUS,
    ExceptionNotFoundError,
    get_array_scratch_file,
    get_job_scratch_dir,
    get_job_scratch_file,
)
from redun.executors.scratch import parse_job_error as _parse_job_error
from redun.executors.scratch import parse_job_result
from redun.file import File
from redun.job_array import JobArrayer
from redun.scheduler import Job, Scheduler, Traceback
from redun.scripting import get_task_command
from redun.task import CacheScope, Task
from redun.utils import json_cache_key, lru_cache_custom, merge_dicts, pickle_dump

SUBMITTED = "SUBMITTED"
PENDING = "PENDING"
RUNNABLE = "RUNNABLE"
STARTING = "STARTING"
RUNNING = "RUNNING"
SUCCEEDED = "SUCCEEDED"
FAILED = "FAILED"

# AWS Batch job statuses.
BATCH_JOB_STATUSES = aws_utils.JobStatus(
    all=[SUBMITTED, PENDING, RUNNABLE, STARTING, RUNNING, SUCCEEDED, FAILED],
    inflight=[SUBMITTED, PENDING, RUNNABLE, STARTING, RUNNING],
    pending=[SUBMITTED, PENDING, RUNNABLE],
    success=[SUCCEEDED],
    failure=[FAILED],
    stopped=[],
    timeout=[],
)


BATCH_LOG_GROUP = "/aws/batch/job"
ARRAY_JOB_SUFFIX = "array"
DOCKER_INSPECT_ERROR = "CannotInspectContainerError: Could not transition to inspecting"
BATCH_JOB_TIMEOUT_ERROR = "Job attempt duration exceeded timeout"
DEBUG_SCRATCH = "scratch"
JOB_ROLE_NONE = "none"


def get_default_registry() -> str:
    """
    Returns the default ECR registry.
    """
    client = boto3.client("ecr")
    resp = client.get_authorization_token()
    registry = resp["authorizationData"][0]["proxyEndpoint"].strip("https://")
    return registry


def get_or_create_repo(repo_name: str) -> dict:
    """
    Get or create an ECR repository.
    """
    client = boto3.client("ecr")

    resp = client.describe_repositories(repositoryNames=[repo_name])
    if resp["repositories"]:
        repo = resp["repositories"][0]
    else:
        repo = client.create_repository(repositoryName=repo_name)["repository"]
    return repo


def ecr_push(repo_name: str, registry: Optional[str] = None) -> None:

    # Get default registry based on login.
    if registry is None:
        registry = get_default_registry()

    get_or_create_repo(repo_name)

    # docker uri
    ecr_uri = "/".join([registry, repo_name])

    subprocess.check_call(["docker", "build", "-t", ecr_uri, "."])

    # push
    subprocess.check_call(["docker", "push", ecr_uri])


def is_array_job_name(job_name: str) -> bool:
    return job_name.endswith(f"-{ARRAY_JOB_SUFFIX}")


@lru_cache(maxsize=200)
def get_job_definition(
    job_def_name: str,
    aws_region: str = aws_utils.DEFAULT_AWS_REGION,
    batch_client: boto3.Session = None,
) -> dict:
    """
    Returns a job definition with the supplied name or empty dict if no matches are found..

    The returned job definition will be the most recent, active revision if there are more than one
    returned from the API.
    """
    if not batch_client:
        batch_client = aws_utils.get_aws_client("batch", aws_region=aws_region)

    # Check if job definition exists.
    resp = batch_client.describe_job_definitions(jobDefinitionName=job_def_name, status="ACTIVE")
    if resp["jobDefinitions"]:
        return sorted(resp["jobDefinitions"], key=lambda jd: jd["revision"])[-1]

    return {}


def get_job_details(
    image: str,
    command: Optional[List[str]] = None,
    memory: int = 4,
    vcpus: int = 1,
    num_nodes: Optional[int] = None,
    shared_memory: Optional[int] = None,
    role: Optional[str] = None,
    aws_region: str = aws_utils.DEFAULT_AWS_REGION,
    privileged: bool = False,
) -> dict:
    """
    Returns a JSON that can be used for creating a job definition.
    """
    # Get default IAM role.
    if role is None:
        caller_id = aws_utils.get_aws_client("sts", aws_region=aws_region).get_caller_identity()
        account_num = caller_id["Account"]
        role = "arn:aws:iam::%d:role/ecsTaskExecutionRole" % int(account_num)

    # If command is not provided, use "ls" as default
    if not command:
        command = ["ls"]

    container_props = {
        "command": command,
        "image": image,
        "vcpus": vcpus,
        "memory": memory,
        "jobRoleArn": role,
        "environment": [],
        "mountPoints": [],
        "volumes": [],
        "resourceRequirements": [],
        "ulimits": [],
        "privileged": privileged,
    }
    if shared_memory is not None:
        container_props["linuxParameters"] = {"sharedMemorySize": int(shared_memory * 1024)}

    # Don't specify a role if it's None.
    if role.lower() == JOB_ROLE_NONE:
        container_props.pop("jobRoleArn")

    if num_nodes is None:
        # Single-node job type
        return {"type": "container", "containerProperties": container_props}

    else:
        # Multi-node job type

        # for multi-node jobs, increase ulimit for number of open file descriptors to
        # allow for large number of open socket connections
        container_props["ulimits"] = [
            {
                "name": "nofile",
                "softLimit": 65535,
                "hardLimit": 65535,
            }
        ]
        return {
            "type": "multinode",
            "nodeProperties": {
                "mainNode": 0,
                "numNodes": num_nodes,
                "nodeRangeProperties": [
                    # Create two identical node groups, so we can tell only the main node to
                    # provide outputs.
                    {
                        "container": container_props,
                        "targetNodes": "0",
                    },
                    {
                        "container": container_props,
                        "targetNodes": "1:",
                    },
                ],
            },
        }


def equiv_job_def(job_def1: dict, job_def2: dict) -> bool:
    """
    Returns True if two job definition are equivalent.

    Limit equality to the keys of job_def1.
    """
    # At submission-time we override resource properties, so ignore them when
    # looking for an equivalent job def.
    no_resource_container_properties = {
        "vcpus": "any_vcpus",
        "memory": "any_memory",
        "resourceRequirements": ["any_requirements"],
    }

    def sanitize_job_def(job_def: Dict) -> Dict:
        """Overwrite the resource properties with redactions."""
        result = copy.deepcopy(job_def)

        # Ignore some fields.
        result.pop("jobDefinitionName", None)

        result.setdefault("containerProperties", {}).update(no_resource_container_properties)
        node_range = result.setdefault("nodeProperties", {}).setdefault(
            "nodeRangeProperties", [{}, {}]
        )
        for node in node_range:
            node.setdefault("container", {}).update(no_resource_container_properties)
        return result

    # Limit equality to the keys of job_def1.
    job_def1 = sanitize_job_def(job_def1)
    job_def2 = sanitize_job_def(job_def2)

    def dict_eq_lhs_keys(lhs: Dict, rhs: Dict) -> bool:
        """Recursively check that the lhs is a subset of the rhs - Batch may return more keys
        than we set, usually to empty values, but we don't care."""

        return all(
            lhs[key] == rhs[key]
            or (isinstance(lhs[key], dict) and dict_eq_lhs_keys(lhs[key], rhs[key]))
            for key in lhs.keys()
        )

    return dict_eq_lhs_keys(job_def1, job_def2)


def get_job_def_revision(job_def_name: str) -> int:
    """
    Returns the job definition revision from a job definition name.
    """
    return int(job_def_name.rsplit(":", 1)[1]) if ":" in job_def_name else 0


@lru_cache_custom(maxsize=200, cache_key=json_cache_key)
def get_or_create_job_definition(
    job_def_name: str,
    image: str,
    command: Optional[List[str]] = None,
    memory: int = 4,
    vcpus: int = 1,
    num_nodes: Optional[int] = None,
    shared_memory: Optional[int] = None,
    role: Optional[str] = None,
    aws_region: str = aws_utils.DEFAULT_AWS_REGION,
    privileged: bool = False,
    job_def_extra: Optional[dict] = None,
) -> dict:
    """
    Returns a job definition with the specified requirements. Although the resource requirements
    provided are used when creating a job, they are specifically excluded from creating new
    job definitions.

    Either an existing active job definition is used or a new one is created.

    num_nodes - if present, create a multi-node batch job.
    """
    batch_client = aws_utils.get_aws_client("batch", aws_region=aws_region)

    # Propose a new job definition.
    job_details = get_job_details(
        image,
        command=command,
        memory=memory,
        vcpus=vcpus,
        num_nodes=num_nodes,
        shared_memory=shared_memory,
        role=role,
        aws_region=aws_region,
        privileged=privileged,
    )
    if job_def_extra:
        job_details = merge_dicts([job_details, job_def_extra])

    # Look for an equivalent existing job defintiion.
    # Give preference for newer job defs.
    existing_job_defs = sorted(
        batch_client.describe_job_definitions(jobDefinitionName=job_def_name, status="ACTIVE").get(
            "jobDefinitions", []
        ),
        key=lambda job_def: get_job_def_revision(job_def["jobDefinitionName"]),
        reverse=True,
    )
    for existing_job_def in existing_job_defs:
        if equiv_job_def(job_details, existing_job_def):
            return existing_job_def

    # No equivalent job defs exist, so create a new one.
    return batch_client.register_job_definition(jobDefinitionName=job_def_name, **job_details)


def make_job_def_name(image_name: str, job_def_suffix: str = "-jd") -> str:
    """
    Autogenerate a job definition name from an image name.
    """
    # Trim registry and tag from image_name.
    if "amazonaws.com" in image_name:
        image_name = image_name.split("/", 1)[1].split(":")[0].replace("/", "-")

    # https://docs.aws.amazon.com/batch/latest/userguide/create-job-definition.html
    # For Job definition name, enter a unique name for your job definition. Up to 128 letters
    # (uppercase and lowercase), numbers, hyphens, and underscores are allowed.
    job_def_prefix = re.sub("[^A-Za-z_0-9-]", "", image_name)[: 128 - len(job_def_suffix)]
    job_def_name = job_def_prefix + job_def_suffix
    return job_def_name


def create_job_override_command(
    command: List[str],
    command_worker: Optional[List[str]] = None,
    num_nodes: Optional[int] = None,
) -> Dict[str, Any]:
    """Format the command into the form needed for the AWS Batch `submit_job` API.

    command: A list of tokens comprising the command
    command_worker: List of command tokens for worker nodes, if present.
    num_nodes: If `None`, this is a single-node job. If not `None`, a multi-node job.

    Returns a dictionary to be passed to `submit_job` as kwargs."""
    batch_job_args: Dict[str, Any] = {}

    if num_nodes is None:
        # Single-node jobs override the container.
        batch_job_args["containerOverrides"] = {"command": command}
    else:

        # Make a shallow copy so we can suppress output on these nodes
        node_overrides = [
            {
                "targetNodes": "0",
                "containerOverrides": {"command": command},
            },
            {"targetNodes": "1:", "containerOverrides": {"command": command_worker}},
        ]

        batch_job_args["nodeOverrides"] = {"nodePropertyOverrides": node_overrides}

    return batch_job_args


def batch_submit(
    batch_job_args: dict,
    queue: str,
    image: str,
    job_def_name: Optional[str] = None,
    job_def_suffix: str = "-jd",
    job_name: str = "batch-job",
    array_size: int = 0,
    memory: int = 4,
    vcpus: int = 1,
    gpus: int = 0,
    num_nodes: Optional[int] = None,
    shared_memory: Optional[int] = None,
    retries: int = 1,
    role: Optional[str] = None,
    job_def_extra: Optional[dict] = None,
    aws_region: str = aws_utils.DEFAULT_AWS_REGION,
    privileged: bool = False,
    autocreate_job_def: bool = True,
    timeout: Optional[int] = None,
    batch_tags: Optional[Dict[str, str]] = None,
    propagate_tags: bool = True,
    share_id: Optional[str] = None,
    scheduling_priority_override: Optional[str] = None,
) -> Dict[str, Any]:
    """Actually perform job submission to AWS batch. Create or retrieve the job definition, then
    use it to submit the job.

    batch_job_args : dict
        These are passed as kwargs to the `submit_job` API. Generally, it must configure the
        commands to be run by setting node or container overrides. This function will modify
        the provided dictionary, at a minimum applying overrides for the resource requirements
        stated by other arguments to this function.
    """
    batch_client = aws_utils.get_aws_client("batch", aws_region=aws_region)

    # Get or create job definition. If autocreate_job is disabled, then we require a job_def_name
    # to be present and lookup the job by name. If autocreate_job is enabled, then we will create
    # a job if an existing one matching job def name and required properties cannot be found.
    if not autocreate_job_def:
        assert job_def_name
        job_def = get_job_definition(job_def_name, aws_region=aws_region)
        if not job_def:
            raise ValueError(f"No job with name {job_def_name} was found.")
    else:
        job_def_name = job_def_name or make_job_def_name(image, job_def_suffix)
        job_def = get_or_create_job_definition(
            job_def_name,
            image,
            role=role,
            aws_region=aws_region,
            privileged=privileged,
            num_nodes=num_nodes,
            shared_memory=shared_memory,
            job_def_extra=job_def_extra,
        )

    def apply_resources(container_properties: Dict) -> None:
        """Modify the provided dictionary of container properties to apply requested resources"""

        # Switch units, redun configs are in GB but AWS uses MB.
        memory_mb = int(memory * 1024)

        container_properties.update({"vcpus": vcpus, "memory": memory_mb})
        if gpus > 0:
            container_properties["resourceRequirements"] = [{"type": "GPU", "value": str(gpus)}]

    if "containerOverrides" in batch_job_args:
        apply_resources(batch_job_args["containerOverrides"])

    if "nodeOverrides" in batch_job_args:
        for node in batch_job_args["nodeOverrides"]["nodePropertyOverrides"]:
            apply_resources(node["containerOverrides"])

    batch_job_args["propagateTags"] = propagate_tags
    if array_size > 0:
        batch_job_args["arrayProperties"] = {"size": array_size}
        if num_nodes is not None:
            raise ValueError("Cannot combine array jobs and multi-node jobs.")
    if timeout:
        batch_job_args["timeout"] = {"attemptDurationSeconds": timeout}
    if batch_tags:
        batch_job_args["tags"] = batch_tags

    # If provided, set batch args required by queues with Scheduling Policies
    if share_id is not None:
        batch_job_args["shareIdentifier"] = share_id
    if scheduling_priority_override is not None:
        batch_job_args["schedulingPriorityOverride"] = scheduling_priority_override

    # Submit to batch.
    batch_run = batch_client.submit_job(
        jobName=job_name,
        jobQueue=queue,
        jobDefinition=job_def["jobDefinitionArn"],
        retryStrategy={"attempts": retries},
        **batch_job_args,
    )

    return batch_run


def get_batch_job_name(prefix: str, job_hash: str, array: bool = False) -> str:
    """
    Return a AWS Batch Job name by either job or job hash.
    """
    return "{}-{}{}".format(prefix, job_hash, f"-{ARRAY_JOB_SUFFIX}" if array else "")


def get_hash_from_job_name(job_name: str) -> Optional[str]:
    """
    Returns the job/task eval_hash that corresponds with a particular job name
    on Batch.
    """
    # Remove array job suffix, if present.
    array_suffix = "-" + ARRAY_JOB_SUFFIX
    if job_name.endswith(array_suffix):
        job_name = job_name[: -len(array_suffix)]

    # It's possible we found jobs that are unrelated to the this work based off the job_name_prefix
    # matching when fetching in get_jobs. These jobs will not have hashes so we can ignore them.
    # For a concrete example of this, see:
    #
    #   https://insitro.atlassian.net/browse/DE-2632
    #
    # where a headnode job is running but has no hash so we don't want to interact with that job
    # here. If we don't find a match, consider this a case of the above where we matched unrelated
    # jobs and return None to let callers know this is the case.
    match = re.match(".*-(?P<hash>[^-]+)", job_name)
    if match:
        return match["hash"]

    return None


def get_batch_job_options(job_options: dict) -> dict:
    """
    Returns AWS Batch-specific job options from general job options.
    """
    # These options are usually mirrored in default_task_options for use with the redun.ini.
    keys = [
        "vcpus",
        "gpus",
        "memory",
        "shared_memory",
        "role",
        "retries",
        "privileged",
        "job_def_name",
        "autocreate_job_def",
        "timeout",
        "batch_tags",
        "num_nodes",
        "job_def_extra",
        "share_id",
        "scheduling_priority_override",
    ]
    return {key: job_options[key] for key in keys if key in job_options}


def submit_task(
    image: str,
    queue: str,
    s3_scratch_prefix: str,
    job: Job,
    a_task: Task,
    args: Tuple = (),
    kwargs: Dict[str, Any] = {},
    job_options: dict = {},
    array_uuid: Optional[str] = None,
    array_size: int = 0,
    code_file: Optional[File] = None,
    aws_region: str = aws_utils.DEFAULT_AWS_REGION,
) -> Dict[str, Any]:
    """
    Submit a redun Task to AWS Batch.
    """
    command = get_oneshot_command(
        s3_scratch_prefix,
        job,
        a_task,
        args,
        kwargs,
        job_options=job_options,
        code_file=code_file,
        array_uuid=array_uuid,
    )
    command_worker = get_oneshot_command(
        s3_scratch_prefix,
        job,
        a_task,
        args,
        kwargs,
        # Suppress cache checking since output is discarded.
        job_options={**job_options, "cache_scope": CacheScope.NONE},
        code_file=code_file,
        array_uuid=array_uuid,
        output_path="/dev/null",  # Let main command write to scratch file.
    )

    if array_uuid:
        job_hash = array_uuid
    else:
        assert job.eval_hash
        job_hash = job.eval_hash

    # Submit to AWS Batch.
    job_name = get_batch_job_name(
        job_options.get("job_name_prefix", "batch-job"), job_hash, array=bool(array_size)
    )

    job_batch_options = get_batch_job_options(job_options)
    job_batch_args = create_job_override_command(
        command=command,
        command_worker=command_worker,
        num_nodes=job_batch_options.get("num_nodes", None),
    )

    result = batch_submit(
        job_batch_args,
        queue,
        image=image,
        job_name=job_name,
        job_def_suffix="-redun-jd",
        aws_region=aws_region,
        array_size=array_size,
        **job_batch_options,
    )
    return result


def submit_command(
    image: str,
    queue: str,
    s3_scratch_prefix: str,
    job: Job,
    command: str,
    job_options: dict = {},
    aws_region: str = aws_utils.DEFAULT_AWS_REGION,
) -> dict:
    """
    Submit a shell command to AWS Batch.
    """
    shell_command = get_script_task_command(
        s3_scratch_prefix,
        job,
        command,
        exit_command="exit 1",
    )

    # Submit to AWS Batch.
    assert job.eval_hash
    job_name = get_batch_job_name(job_options.get("job_name_prefix", "batch-job"), job.eval_hash)

    job_batch_options = get_batch_job_options(job_options)
    job_batch_args = create_job_override_command(
        command=shell_command,
        num_nodes=job_batch_options.get("num_nodes", None),
    )

    # Submit to AWS Batch.
    return batch_submit(
        job_batch_args,
        queue,
        image=image,
        job_name=job_name,
        job_def_suffix="-redun-jd",
        aws_region=aws_region,
        **job_batch_options,
    )


def parse_job_error(
    s3_scratch_prefix: str, job: Job, batch_job_metadata: Optional[dict] = None
) -> Tuple[Exception, "Traceback"]:
    """
    Parse task error from s3 scratch path.
    """
    error, error_traceback = _parse_job_error(s3_scratch_prefix, job)

    # Handle AWS Batch-specific errors.
    if isinstance(error, ExceptionNotFoundError) and batch_job_metadata:
        try:
            status_reason = batch_job_metadata["attempts"][-1]["statusReason"]
            if status_reason == BATCH_JOB_TIMEOUT_ERROR:
                error = AWSBatchJobTimeoutError(BATCH_JOB_TIMEOUT_ERROR)
                error_traceback = Traceback.from_error(error)

        except (KeyError, IndexError):
            pass

    return error, error_traceback


def parse_job_logs(
    batch_job_id: str,
    max_lines: int = 1000,
    required: bool = True,
    aws_region: str = aws_utils.DEFAULT_AWS_REGION,
) -> Iterator[str]:
    """
    Iterates through most recent CloudWatch logs of an AWS Batch Job.
    """
    lines_iter = iter_batch_job_log_lines(
        batch_job_id, reverse=True, required=required, aws_region=aws_region
    )
    lines = reversed(list(islice(lines_iter, 0, max_lines)))

    if next(lines_iter, None) is not None:
        yield "\n*** Earlier logs are truncated ***\n"
    yield from lines


def aws_describe_jobs(
    job_ids: List[str], chunk_size: int = 100, aws_region: str = aws_utils.DEFAULT_AWS_REGION
) -> Iterator[dict]:
    """
    Returns AWS Batch Job descriptions from the AWS API.
    """
    # The AWS Batch API can only query up to 100 jobs at a time.
    batch_client = aws_utils.get_aws_client("batch", aws_region=aws_region)
    for i in range(0, len(job_ids), chunk_size):
        chunk_job_ids = job_ids[i : i + chunk_size]
        response = batch_client.describe_jobs(jobs=chunk_job_ids)

        for job in response["jobs"]:
            yield job


def iter_batch_job_status(
    job_ids: List[str], pending_truncate: int = 10, aws_region: str = aws_utils.DEFAULT_AWS_REGION
) -> Iterator[dict]:
    """
    Yields AWS Batch jobs statuses.

    If pending_truncate is used (> 0) then rely on AWS Batch's behavior of running
    jobs approximately in order. This allows us to truncate the polling of jobs
    once we see a sufficient number of pending jobs.

    Parameters
    ----------
    job_ids : List[str]
      Batch job ids that should be in order of submission.
    pending_truncate : int
      After seeing `pending_truncate` number of pending jobs, assume the rest are pending.
      Use a negative int to disable this optimization.
    aws_region : str
       AWS region that jobs are running in.
    """
    pending_run = 0

    for job in aws_describe_jobs(job_ids, aws_region=aws_region):
        yield job

        if job["status"] in BATCH_JOB_STATUSES.pending:
            pending_run += 1
        else:
            pending_run = 0

        if pending_truncate > 0 and pending_run > pending_truncate:
            break


def get_job_log_stream(job: Optional[dict], aws_region: str) -> Optional[str]:
    """Extract the log stream from a `JobDetail` status dictionary. For non-multi-node jobs,
    (i.e., single node and array jobs), this is simply a field in the detail dictionary. But for
    multi-node jobs, this requires another query to get the log stream for the main node."""
    if job and "nodeProperties" in job:
        # There isn't a log stream on the main job detail object. However, we can find the right
        # node and query it:
        main_node = job["nodeProperties"]["mainNode"]
        job_id = job["jobId"]
        # The docs indicate we can rely on this format for getting the per-worker jobs ids:
        # `jobId#worker_id`.
        jobs: Iterator[Optional[Dict[Any, Any]]] = aws_describe_jobs(
            [f"{job_id}#{main_node}"], aws_region=aws_region
        )
        job = next(jobs, None)
    if not job:
        # Job is no longer present in AWS API. Return no logs.
        return None

    return job.get("container", {}).get("logStreamName")


def format_log_stream_event(event: dict) -> str:
    """
    Format a logStream event as a line.
    """
    timestamp = str(datetime.datetime.fromtimestamp(event["timestamp"] / 1000))
    return "{timestamp}  {message}".format(timestamp=timestamp, message=event["message"])


def iter_batch_job_logs(
    job_id: str,
    aws_region: str = aws_utils.DEFAULT_AWS_REGION,
    log_group_name: str = BATCH_LOG_GROUP,
    limit: Optional[int] = None,
    reverse: bool = False,
    required: bool = True,
) -> Iterator[dict]:
    """
    Iterate through the log events of an AWS Batch job.
    """
    # Get job's log stream.
    job = next(aws_describe_jobs([job_id], aws_region=aws_region), None)

    log_stream = get_job_log_stream(job, aws_region)
    if not log_stream:
        # No log stream is present. Return no logs.
        return

    yield from aws_utils.iter_log_stream(
        log_group_name=log_group_name,
        log_stream=log_stream,
        limit=limit,
        reverse=reverse,
        required=required,
        aws_region=aws_region,
    )


def iter_batch_job_log_lines(
    job_id: str,
    aws_region: str = aws_utils.DEFAULT_AWS_REGION,
    log_group_name: str = BATCH_LOG_GROUP,
    reverse: bool = False,
    required: bool = True,
) -> Iterator[str]:
    """
    Iterate through the log lines of an AWS Batch job.
    """
    events = iter_batch_job_logs(
        job_id,
        reverse=reverse,
        log_group_name=log_group_name,
        required=required,
        aws_region=aws_region,
    )
    return map(format_log_stream_event, events)


class AWSBatchJobTimeoutError(Exception):
    """
    Custom exception to raise when AWS Batch Jobs are killed due to timeout.
    """

    pass


def parse_nullable_json(text: Optional[str]) -> Any:
    if text is None:
        return None
    else:
        return json.loads(text)


@register_executor("aws_batch")
class AWSBatchExecutor(Executor):
    """
    A redun Executor for running jobs on AWS Batch.
    """

    def __init__(
        self,
        name: str,
        scheduler: Optional["Scheduler"] = None,
        config: Optional[SectionProxy] = None,
    ):
        super().__init__(name, scheduler=scheduler)
        if config is None:
            raise ValueError("AWSBatchExecutor requires config.")

        # Use DockerExecutor for local debug mode.
        docker_config = get_docker_executor_config(config)
        docker_config["scratch"] = config.get("debug_scratch", fallback=DEBUG_SCRATCH)
        self._docker_executor = DockerExecutor(name + "_debug", scheduler, config=docker_config)

        # Required config.
        self.image = config["image"]
        self.queue = config["queue"]
        self.s3_scratch_prefix = config["s3_scratch"]

        # Optional config.
        self.aws_region = config.get("aws_region", aws_utils.get_default_region())
        self.role = config.get("role")
        self.code_package = parse_code_package_config(config)
        self.code_file: Optional[File] = None
        self.debug = config.getboolean("debug", fallback=False)

        # Default task options.
        # Defaults should be set for all values mirrored in get_batch_job_options.
        self.default_task_options: Dict[str, Any] = {
            "vcpus": config.getint("vcpus", fallback=1),
            "gpus": config.getint("gpus", fallback=0),
            "memory": config.getfloat("memory", fallback=4),
            "shared_memory": config.getint("shared_memory", fallback=None),
            "timeout": config.getint("timeout", fallback=None),
            "privileged": config.getboolean("privileged", fallback=False),
            "autocreate_job_def": config.getboolean(
                "autocreate_job_def",
                # autocreate_job is a deprecated but backwards compatible alternative.
                fallback=config.getboolean("autocreate_job", fallback=True),
            ),
            "job_def_name": config.get("job_def_name", fallback=None),
            "retries": config.getint("retries", fallback=1),
            "role": config.get("role"),
            "job_name_prefix": config.get("job_name_prefix", fallback="redun-job"),
            "num_nodes": config.getint("num_nodes", fallback=None),
            "job_def_extra": parse_nullable_json(config.get("job_def_extra", fallback=None)),
            "share_id": config.get("share_id"),
            "scheduling_priority_override": config.getint("scheduling_priority_override"),
        }
        if config.get("batch_tags"):
            self.default_task_options["batch_tags"] = json.loads(config.get("batch_tags"))
        self.use_default_batch_tags = config.getboolean("default_batch_tags", fallback=True)

        self.is_running = False
        # We use an OrderedDict in order to retain submission order.
        self.pending_batch_jobs: Dict[str, "Job"] = OrderedDict()
        self.preexisting_batch_jobs: Dict[str, str] = {}  # Job hash -> Job ID
        self.interval = config.getfloat("job_monitor_interval", fallback=5.0)

        self._thread: Optional[threading.Thread] = None
        self.arrayer = JobArrayer(
            self._submit_jobs,
            self._on_error,
            submit_interval=self.interval,
            stale_time=config.getfloat("job_stale_time", fallback=3.0),
            min_array_size=config.getint("min_array_size", fallback=5),
            max_array_size=config.getint("max_array_size", fallback=1000),
        )
        self._aws_user: Optional[str] = None

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

    def set_scheduler(self, scheduler: Scheduler) -> None:
        super().set_scheduler(scheduler)
        self._docker_executor.set_scheduler(scheduler)

    def gather_inflight_jobs(self) -> None:

        running_arrays: Dict[str, List[Tuple[str, int]]] = defaultdict(list)

        # Get all running jobs by name
        inflight_jobs = self.get_jobs(BATCH_JOB_STATUSES.inflight)
        for job in inflight_jobs:
            name = job["jobName"]

            # Single jobs can be simply added to dict of pre-existing jobs.
            if not is_array_job_name(name):
                job_hash = get_hash_from_job_name(name)
                if job_hash:
                    self.preexisting_batch_jobs[job_hash] = job["jobId"]
                continue

            # Get all child jobs of running array jobs for reuniting.
            running_arrays[name] = [
                (child_job["jobId"], child_job["arrayProperties"]["index"])
                for child_job in self.get_array_child_jobs(
                    job["jobId"], BATCH_JOB_STATUSES.inflight
                )
            ]

        # Match up running array jobs with consistent redun job naming scheme.
        for array_name, child_job_indices in running_arrays.items():

            # Get path to array file directory on S3 from array job name.
            parent_hash = get_hash_from_job_name(array_name)
            if not parent_hash:
                continue
            eval_file = File(
                get_array_scratch_file(self.s3_scratch_prefix, parent_hash, SCRATCH_HASHES)
            )
            if not eval_file.exists():
                # Eval file does not exist, so we cannot reunite with this array job.
                continue

            # Get eval_hash for all jobs that were part of the array
            eval_hashes = cast(str, eval_file.read("r")).splitlines()

            # Now match up indices to eval hashes to populate pending jobs by name.
            for job_id, job_index in child_job_indices:
                job_hash = eval_hashes[job_index]
                self.preexisting_batch_jobs[job_hash] = job_id

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
        self._docker_executor.stop()
        self.arrayer.stop()
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
        Thread for monitoring running AWS Batch jobs.

        We use the following process for monitoring AWS Batch jobs in order to
        achieve timely updates and avoid excessive API calls which can cause
        API throttling and slow downs.

        - We use the `describe_jobs()` API call on specific Batch job ids in order
          to avoid processing status of unrelated jobs on the same Batch queue.
        - We call `describe_jobs()` with 100 job ids at a time to reduce the number
          of API calls. 100 job ids is the maximum supported amount by
          `describe_jobs()`.
        - We do only one describe_jobs() API call per monitor loop, and then
          sleep `self.interval` seconds.
        - AWS Batch runs jobs in approximately the order submitted. So if we
          monitor job statuses in submission order, a run of PENDING statuses
          (`pending_truncate`) suggests the rest of the jobs will be PENDING.
          Therefore, we can truncate our polling and restart at the beginning
          of list of job ids.

        By combining these techniques, we spend most of our time monitoring
        only running jobs (there could be a very large number of pending jobs),
        we stay under API rate limits, and we keep the compute in this
        thread low so as to not interfere with new submissions.
        """
        assert self._scheduler
        chunk_size = 100
        pending_truncate = 10

        try:
            while self.is_running and (self.pending_batch_jobs or self.arrayer.num_pending):
                if self._scheduler.logger.level >= logging.DEBUG:
                    self.log(
                        f"Preparing {self.arrayer.num_pending} job(s) for Job Arrays.",
                        level=logging.DEBUG,
                    )
                    self.log(
                        f"Waiting on {len(self.pending_batch_jobs)} Batch job(s): "
                        + " ".join(sorted(self.pending_batch_jobs.keys())),
                        level=logging.DEBUG,
                    )
                # Copy pending_batch_jobs.keys() since it can change due to new submissions.
                jobs = iter_batch_job_status(
                    list(self.pending_batch_jobs.keys()),
                    pending_truncate=pending_truncate,
                    aws_region=self.aws_region,
                )
                for i, job in enumerate(jobs):
                    self._process_job_status(job)
                    if i % chunk_size == 0:
                        # Sleep after every chunk to avoid excessive API calls.
                        time.sleep(self.interval)
                time.sleep(self.interval)

        except Exception as error:
            # Since we run this is method at the top-level of a thread, we
            # need to catch all exceptions so we can properly report them to
            # the scheduler.
            self._scheduler.reject_job(None, error)

        self.log("Shutting down executor...", level=logging.DEBUG)
        self.stop()

    def _can_override_failed(self, job: dict) -> Tuple[bool, str]:
        """
        Certain AWS errors can be ignored that do not effect the result.

        https://github.com/aws/amazon-ecs-agent/issues/2312
        """
        try:
            container_reason = job["attempts"][-1]["container"]["reason"]
        except (KeyError, IndexError):
            container_reason = ""

        if DOCKER_INSPECT_ERROR in container_reason:
            redun_job = self.pending_batch_jobs[job["jobId"]]
            if redun_job.task.script:
                # Script tasks will report their status in a status file.
                status_file = File(
                    get_job_scratch_file(self.s3_scratch_prefix, redun_job, SCRATCH_STATUS)
                )
                if status_file.exists():
                    return status_file.read().strip() == "ok", container_reason
            else:
                # Non-script tasks only create an output file if it is successful.
                output_file = File(
                    get_job_scratch_file(self.s3_scratch_prefix, redun_job, SCRATCH_OUTPUT)
                )
                return output_file.exists(), container_reason

        return False, container_reason

    def _process_job_status(self, job: dict) -> None:
        """
        Process AWS Batch job statuses.
        """
        assert self._scheduler
        job_status: Optional[str] = None

        # Determine job status.
        if job["status"] == SUCCEEDED:
            job_status = SUCCEEDED

        elif job["status"] == FAILED:
            can_override, container_reason = self._can_override_failed(job)
            if can_override:
                job_status = SUCCEEDED
                self._scheduler.log(
                    "NOTE: Overriding AWS Batch error: {}".format(container_reason)
                )
            else:
                job_status = FAILED
        else:
            return

        # Determine redun Job and job_tags.
        redun_job = self.pending_batch_jobs.pop(job["jobId"])
        job_tags = []
        job_tags.append(("aws_batch_job", job["jobId"]))
        log_stream = get_job_log_stream(job, aws_region=self.aws_region)
        if log_stream:
            job_tags.append(("aws_log_stream", log_stream))

        if job_status == SUCCEEDED:
            # Assume a recently completed job has valid results.
            result, exists = parse_job_result(self.s3_scratch_prefix, redun_job)
            if exists:
                self._scheduler.done_job(redun_job, result, job_tags=job_tags)
            else:
                # This can happen if job ended in an inconsistent state.
                self._scheduler.reject_job(
                    redun_job,
                    FileNotFoundError(
                        get_job_scratch_file(self.s3_scratch_prefix, redun_job, SCRATCH_OUTPUT)
                    ),
                    job_tags=job_tags,
                )
        elif job_status == FAILED:
            error, error_traceback = parse_job_error(
                self.s3_scratch_prefix, redun_job, batch_job_metadata=job
            )
            logs = [f"*** CloudWatch logs for AWS Batch job {job['jobId']}:\n"]
            if container_reason:
                logs.append(f"container.reason: {container_reason}\n")

            try:
                status_reason = job["attempts"][-1]["statusReason"]
            except (KeyError, IndexError):
                status_reason = ""
            if status_reason:
                logs.append(f"statusReason: {status_reason}\n")

            logs.extend(parse_job_logs(job["jobId"], required=False, aws_region=self.aws_region))
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
        job_options = job.get_options()

        # Normalize deprecated options.
        if "autocreate_job" in job_options and "autocreate_job_def" not in job_options:
            job_options["autocreate_job_def"] = job_options["autocreate_job"]

        task_options = {
            **self.default_task_options,
            **job_options,
        }

        # Add default batch tags to the job.
        if self.use_default_batch_tags:
            execution = job.execution
            project = (
                execution.job.task.namespace
                if execution and execution.job and execution.job.task
                else ""
            )
            default_tags = {
                "redun_job_id": job.id,
                "redun_task_name": job.task.fullname,
                "redun_execution_id": execution.id if execution else "",
                "redun_project": project,
                "redun_aws_user": self._aws_user or "",
            }
        else:
            default_tags = {}

        # Merge batch_tags if needed.
        batch_tags: dict = {
            **self.default_task_options.get("batch_tags", {}),
            **default_tags,
            **job_options.get("batch_tags", {}),
        }
        if batch_tags:
            task_options["batch_tags"] = batch_tags

        return task_options

    def _submit(self, job: Job) -> None:
        """
        Submit Job to executor.
        """
        assert self._scheduler
        assert not self.debug

        # If we are not in debug mode and this is the first submission gather inflight jobs. In
        # debug mode, we are running on docker locally so there is no need to hit the Batch API to
        # gather jobs as we are not going to run on batch. We also check is_running here as a way
        # of determining whether this is the first submission or not. If we are already running,
        # then we know we have already had jobs submitted and done the inflight check so no
        # reason to do that again here.
        if not self.is_running:
            # Precompute existing inflight jobs for job reuniting.
            self.gather_inflight_jobs()

        # Package code if necessary and we have not already done so. If code_package is False,
        # then we can skip this step. Additionally, if we have already packaged and set code_file,
        # then we do not need to repackage.
        if self.code_package is not False and self.code_file is None:
            code_package = self.code_package or {}
            assert isinstance(code_package, dict)
            self.code_file = package_code(self.s3_scratch_prefix, code_package)

        job_dir = get_job_scratch_dir(self.s3_scratch_prefix, job)
        job_type = "AWS Batch job"

        # Determine job options.
        task_options = self._get_job_options(job)
        cache_scope = CacheScope(task_options.get("cache_scope", CacheScope.BACKEND))

        # Determine if we can reunite with a previous Batch output or job.
        batch_job_id: Optional[str] = None
        if cache_scope == CacheScope.BACKEND and job.eval_hash in self.preexisting_batch_jobs:
            batch_job_id = self.preexisting_batch_jobs.pop(job.eval_hash)

            # Make sure Batch API still has a status on this job.
            existing_job = next(
                aws_describe_jobs([batch_job_id], aws_region=self.aws_region), None
            )

            # Reunite with inflight batch job, if present.
            if existing_job:
                batch_job_id = existing_job["jobId"]
                self.log(
                    "reunite redun job {redun_job} with {job_type} {batch_job}:\n"
                    "  s3_scratch_path = {job_dir}".format(
                        redun_job=job.id,
                        job_type=job_type,
                        batch_job=batch_job_id,
                        job_dir=job_dir,
                    )
                )
                assert batch_job_id
                self.pending_batch_jobs[batch_job_id] = job
            else:
                batch_job_id = None

        # Job arrayer will handle actual submission after bunching to an array
        # job, if necessary.
        if batch_job_id is None:
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
        if job.task.script:
            raise NotImplementedError("Array jobs not supported for scripts")

        task_options = self._get_job_options(job)
        image = task_options.pop("image", self.image)
        queue = task_options.pop("queue", self.queue)
        # Generate a unique name for job with no '-' to simplify job name parsing.
        array_uuid = str(uuid.uuid4()).replace("-", "")

        job_type = "AWS Batch job"

        # Setup input, output and error path files.
        # Input file is a pickled list of args, and kwargs, for each child job.
        input_file = get_array_scratch_file(self.s3_scratch_prefix, array_uuid, SCRATCH_INPUT)
        with File(input_file).open("wb") as out:
            pickle_dump([all_args, all_kwargs], out)

        # Output file is a plaintext list of output paths, for each child job.
        output_file = get_array_scratch_file(self.s3_scratch_prefix, array_uuid, SCRATCH_OUTPUT)
        output_paths = [
            get_job_scratch_file(self.s3_scratch_prefix, job, SCRATCH_OUTPUT) for job in jobs
        ]
        with File(output_file).open("w") as ofile:
            json.dump(output_paths, ofile)

        # Error file is a plaintext list of error paths, one for each child job.
        error_file = get_array_scratch_file(self.s3_scratch_prefix, array_uuid, SCRATCH_ERROR)
        error_paths = [
            get_job_scratch_file(self.s3_scratch_prefix, job, SCRATCH_ERROR) for job in jobs
        ]
        with File(error_file).open("w") as efile:
            json.dump(error_paths, efile)

        # Eval hash file is plaintext hashes of child jobs for matching for job reuniting.
        eval_file = get_array_scratch_file(self.s3_scratch_prefix, array_uuid, SCRATCH_HASHES)
        with File(eval_file).open("w") as eval_f:
            eval_f.write("\n".join([job.eval_hash for job in jobs]))  # type: ignore

        batch_resp = submit_task(
            image,
            queue,
            self.s3_scratch_prefix,
            job,
            job.task,
            job_options=task_options,
            code_file=self.code_file,
            aws_region=self.aws_region,
            array_uuid=array_uuid,
            array_size=array_size,
        )

        # Add entire array to array jobs, and all jobs in array to pending jobs.
        array_job_id = batch_resp["jobId"]
        for i in range(array_size):
            self.pending_batch_jobs[f"{array_job_id}:{i}"] = jobs[i]

        self.log(
            "submit {array_size} redun job(s) as {job_type} {batch_job}:\n"
            "  array_job_id    = {array_job_id}\n"
            "  array_job_name  = {job_name}\n"
            "  array_size      = {array_size}\n"
            "  s3_scratch_path = {job_dir}\n"
            "  submit_retry_attempts  = {retries}\n".format(
                array_job_id=array_job_id,
                array_size=array_size,
                job_type=job_type,
                batch_job=array_job_id,
                job_dir=get_array_scratch_file(self.s3_scratch_prefix, array_uuid, ""),
                job_name=batch_resp.get("jobName"),
                retries=batch_resp.get("ResponseMetadata", {}).get("RetryAttempts"),
            )
        )

        # AWS Batch job tags at submission time.
        assert self._scheduler
        for i, job in enumerate(jobs):
            self._scheduler.add_job_tags(job, [("aws_batch_job", f"{array_job_id}:{i}")])

        return array_uuid

    def _submit_single_job(self, job) -> None:
        """
        Actually submits a job. Caching detects if it should be part
        of an array job
        """
        assert job.args
        args, kwargs = job.args

        task_options = self._get_job_options(job)
        image = task_options.pop("image", self.image)
        queue = task_options.pop("queue", self.queue)

        job_dir = get_job_scratch_dir(self.s3_scratch_prefix, job)
        job_type = "AWS Batch job"

        # Submit a new Batch job.
        if not job.task.script:
            batch_resp = submit_task(
                image,
                queue,
                self.s3_scratch_prefix,
                job,
                job.task,
                args=args,
                kwargs=kwargs,
                job_options=task_options,
                code_file=self.code_file,
                aws_region=self.aws_region,
            )
        else:
            command = get_task_command(job.task, args, kwargs)
            batch_resp = submit_command(
                image,
                queue,
                self.s3_scratch_prefix,
                job,
                command,
                job_options=task_options,
                aws_region=self.aws_region,
            )

        self.log(
            "submit redun job {redun_job} as {job_type} {batch_job}:\n"
            "  job_id          = {batch_job}\n"
            "  job_name        = {job_name}\n"
            "  s3_scratch_path = {job_dir}\n"
            "  submit_retry_attempts  = {retries}\n".format(
                redun_job=job.id,
                job_type=job_type,
                batch_job=batch_resp["jobId"],
                job_dir=job_dir,
                job_name=batch_resp.get("jobName"),
                retries=batch_resp.get("ResponseMetadata", {}).get("RetryAttempts"),
            )
        )
        batch_job_id = batch_resp["jobId"]
        self.pending_batch_jobs[batch_job_id] = job

        # Add batch job tags at submission time.
        assert self._scheduler
        self._scheduler.add_job_tags(job, [("aws_batch_job", batch_job_id)])

    def _is_debug_job(self, job: Job) -> bool:
        """
        Returns True if job should be sent to debugging DockerExecutor.
        """
        return self.debug or job.get_option("debug", False)

    def submit(self, job: Job) -> None:
        """
        Submit Job to executor.
        """
        if self._is_debug_job(job):
            return self._docker_executor.submit(job)
        else:
            return self._submit(job)

    def submit_script(self, job: Job) -> None:
        """
        Submit Job for script task to executor.
        """
        if self._is_debug_job(job):
            return self._docker_executor.submit_script(job)
        else:
            return self._submit(job)

    def get_jobs(self, statuses: Optional[List[str]] = None) -> Iterator[dict]:
        """
        Returns AWS Batch Job statuses from the AWS API.
        """
        batch_client = aws_utils.get_aws_client("batch", aws_region=self.aws_region)
        paginator = batch_client.get_paginator("list_jobs")

        if not statuses:
            statuses = BATCH_JOB_STATUSES.all
        job_name_prefix = self.default_task_options["job_name_prefix"]

        for status in statuses:
            pages = paginator.paginate(jobQueue=self.queue, jobStatus=status)
            for response in pages:
                for job in response["jobSummaryList"]:
                    if job["jobName"].startswith(job_name_prefix):
                        yield job

    def get_array_child_jobs(
        self, job_id: str, statuses: List[str] = BATCH_JOB_STATUSES.inflight
    ) -> List[Dict[str, Any]]:
        batch_client = aws_utils.get_aws_client("batch", aws_region=self.aws_region)
        paginator = batch_client.get_paginator("list_jobs")

        found_jobs = []
        for status in statuses:
            pages = paginator.paginate(arrayJobId=job_id, jobStatus=status)
            found_jobs.extend([job for response in pages for job in response["jobSummaryList"]])

        return found_jobs

    def kill_jobs(
        self, job_ids: Iterable[str], reason: str = "Terminated by user"
    ) -> Iterator[dict]:
        """
        Kill AWS Batch Jobs.
        """
        batch_client = aws_utils.get_aws_client("batch", aws_region=self.aws_region)

        for job_id in job_ids:
            yield batch_client.terminate_job(jobId=job_id, reason=reason)

    def scratch_root(self) -> str:
        return self.s3_scratch_prefix
