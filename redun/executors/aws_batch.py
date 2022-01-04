import datetime
import json
import logging
import os
import pickle
import re
import subprocess
import threading
import time
import uuid
from collections import OrderedDict, defaultdict
from functools import lru_cache
from itertools import islice
from shlex import quote
from tempfile import mkstemp
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple, cast
from urllib.error import URLError
from urllib.request import urlopen

import boto3

from redun.executors import aws_utils
from redun.executors.base import Executor, register_executor
from redun.file import File
from redun.job_array import AWS_ARRAY_VAR, JobArrayer
from redun.scheduler import Job, Scheduler, Traceback
from redun.scripting import ScriptError, get_task_command
from redun.task import Task
from redun.utils import get_import_paths, pickle_dump

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


@lru_cache(maxsize=200)
def get_or_create_job_definition(
    job_def_name: str,
    image: str,
    command: List[str] = ["ls"],
    memory: int = 4,
    vcpus: int = 1,
    role: Optional[str] = None,
    aws_region: str = aws_utils.DEFAULT_AWS_REGION,
    privileged: bool = False,
) -> dict:
    """
    Returns a job definition with the specified requirements.

    Either an existing active job definition is used or a new one is created.
    """
    batch_client = aws_utils.get_aws_client("batch", aws_region=aws_region)

    # Get default IAM role.
    if role is None:
        caller_id = aws_utils.get_aws_client("sts", aws_region=aws_region).get_caller_identity()
        account_num = caller_id["Account"]
        role = "arn:aws:iam::%d:role/ecsTaskExecutionRole" % int(account_num)

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
    existing_job_def = get_job_definition(job_def_name, batch_client=batch_client)
    if existing_job_def:
        existing_container_props = existing_job_def["containerProperties"]
        if existing_container_props == container_props:
            return existing_job_def

    # register job definition
    job_def = batch_client.register_job_definition(
        jobDefinitionName=job_def_name, type="container", containerProperties=container_props
    )
    return job_def


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


def batch_submit(
    command: List[str],
    queue: str,
    image: str,
    job_def_name: Optional[str] = None,
    job_def_suffix: str = "-jd",
    job_name: str = "batch-job",
    array_size: int = 0,
    memory: int = 4,
    vcpus: int = 1,
    gpus: int = 0,
    retries: int = 1,
    role: Optional[str] = None,
    aws_region: str = aws_utils.DEFAULT_AWS_REGION,
    privileged: bool = False,
    autocreate_job: bool = True,
    timeout: Optional[int] = None,
    batch_tags: Optional[Dict[str, str]] = None,
    propagate_tags: bool = True,
) -> Dict[str, Any]:

    batch_client = aws_utils.get_aws_client("batch", aws_region=aws_region)

    # Get or create job definition. If autocreate_job is disabled, then we require a job_def_name
    # to be present and lookup the job by name. If autocreate_job is enabled, then we will create
    # a job if an existing one matching job def name and required properties cannot be found.
    if not autocreate_job:
        assert job_def_name
        job_def = get_job_definition(job_def_name, aws_region=aws_region)
        if not job_def:
            raise ValueError(f"No job with name {job_def_name} was found.")
    else:
        job_def_name = job_def_name or make_job_def_name(image, job_def_suffix)
        job_def = get_or_create_job_definition(
            job_def_name, image, role=role, aws_region=aws_region, privileged=privileged
        )

    # Container overrides for this job.
    container_overrides = {"vcpus": vcpus, "memory": int(memory * 1024), "command": command}
    if gpus > 0:
        container_overrides["resourceRequirements"] = [{"type": "GPU", "value": str(gpus)}]

    batch_job_args: dict = {
        "propagateTags": propagate_tags,
    }
    if array_size > 0:
        batch_job_args["arrayProperties"] = {"size": array_size}
    if timeout:
        batch_job_args["timeout"] = {"attemptDurationSeconds": timeout}
    if batch_tags:
        batch_job_args["tags"] = batch_tags

    # Submit to batch.
    batch_run = batch_client.submit_job(
        jobName=job_name,
        jobQueue=queue,
        jobDefinition=job_def["jobDefinitionArn"],
        containerOverrides=container_overrides,
        retryStrategy={"attempts": retries},
        **batch_job_args,
    )
    return batch_run


def is_ec2_instance() -> bool:
    """
    Returns True if this process is running on an EC2 instance.

    We use the presence of a link-local address as a sign we are on an EC2 instance.
    """
    try:
        resp = urlopen("http://169.254.169.254/latest/meta-data/", timeout=1)
        return resp.status == 200
    except URLError:
        return False


def run_docker(
    command: List[str],
    image: str,
    array_index: int = -1,
    volumes: Optional[Iterable[Tuple[str, str]]] = None,
    interactive: bool = True,
    cleanup: bool = False,
) -> str:
    """
    volumes: a list of ('host', 'container') path pairs for volume mouting.
    """
    # Add AWS credentials to environment for docker command.
    env = dict(os.environ)
    if not is_ec2_instance():
        session = boto3.Session()
        creds = session.get_credentials().get_frozen_credentials()
        cred_map = {
            "AWS_ACCESS_KEY_ID": creds.access_key,
            "AWS_SECRET_ACCESS_KEY": creds.secret_key,
            "AWS_SESSION_TOKEN": creds.token,
        }
        defined = {k: v for k, v in cred_map.items() if v}
        env.update(defined)

    common_args = []
    if cleanup:
        common_args.append("--rm")

    # Environment args.
    common_args.extend(
        ["-e", "AWS_ACCESS_KEY_ID", "-e", "AWS_SECRET_ACCESS_KEY", "-e", "AWS_SESSION_TOKEN"]
    )

    # Add array index environment variable if running an array job
    if array_index >= 0:
        env.update({AWS_ARRAY_VAR: str(array_index)})
        common_args.extend(["-e", AWS_ARRAY_VAR])

    # Volume mounting args.
    if not volumes:
        volumes = []
    for host, container in volumes:
        common_args.extend(["-v", f"{host}:{container}"])

    common_args.append(image)
    common_args.extend(command)

    if interactive:
        # Adding this flag is necessary to prevent docker from hijacking the terminal and modifying
        # the tty settings. One of the modifications it makes when it hijacks the terminal is to
        # change the behavior of line endings which means output(like from logging) will be
        # malformed until the docker container exits and the hijacked connection is closed which
        # resets the tty settings.
        env["NORAW"] = "true"

        # Run Docker interactively.
        fd, cidfile = mkstemp()
        os.close(fd)
        os.remove(cidfile)

        docker_command = ["docker", "run", "-it", "--cidfile", cidfile] + common_args
        subprocess.check_call(docker_command, env=env)
        with open(cidfile) as infile:
            container_id = infile.read().strip()
        os.remove(cidfile)
    else:
        # Run Docker in the background.
        docker_command = ["docker", "run", "-d"] + common_args
        container_id = subprocess.check_output(docker_command, env=env).strip().decode("utf8")

    return container_id


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
    keys = [
        "vcpus",
        "gpus",
        "memory",
        "role",
        "retries",
        "privileged",
        "job_def_name",
        "autocreate_job",
        "timeout",
        "batch_tags",
    ]
    return {key: job_options[key] for key in keys if key in job_options}


def get_docker_job_options(job_options: dict) -> dict:
    """
    Returns Docker-specific job options from general job options.
    """
    keys = ["volumes", "interactive"]
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
    debug: bool = False,
    code_file: Optional[File] = None,
    aws_region: str = aws_utils.DEFAULT_AWS_REGION,
) -> Dict[str, Any]:
    """
    Submit a redun Task to AWS Batch or Docker (debug=True).
    """
    if array_size:
        # Output_path will contain a pickled list of actual output paths, etc.
        # Want files that won't get clobbered when jobs actually run
        assert array_uuid
        input_path = aws_utils.get_array_scratch_file(
            s3_scratch_prefix, array_uuid, aws_utils.S3_SCRATCH_INPUT
        )
        output_path = aws_utils.get_array_scratch_file(
            s3_scratch_prefix, array_uuid, aws_utils.S3_SCRATCH_OUTPUT
        )
        error_path = aws_utils.get_array_scratch_file(
            s3_scratch_prefix, array_uuid, aws_utils.S3_SCRATCH_ERROR
        )
    else:
        input_path = aws_utils.get_job_scratch_file(
            s3_scratch_prefix, job, aws_utils.S3_SCRATCH_INPUT
        )
        output_path = aws_utils.get_job_scratch_file(
            s3_scratch_prefix, job, aws_utils.S3_SCRATCH_OUTPUT
        )
        error_path = aws_utils.get_job_scratch_file(
            s3_scratch_prefix, job, aws_utils.S3_SCRATCH_ERROR
        )

        # Serialize arguments to input file.
        # Array jobs set this up earlier, in `_submit_array_job`
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
        + ["--input", input_path, "--output", output_path, "--error", error_path, a_task.fullname]
    )

    if not debug:
        if array_uuid:
            job_hash = array_uuid
        else:
            assert job.eval_hash
            job_hash = job.eval_hash

        # Submit to AWS Batch.
        job_name = get_batch_job_name(
            job_options.get("job_name_prefix", "batch-job"), job_hash, array=bool(array_size)
        )

        result = batch_submit(
            command,
            queue,
            image=image,
            job_name=job_name,
            job_def_suffix="-redun-jd",
            aws_region=aws_region,
            array_size=array_size,
            **get_batch_job_options(job_options),
        )
    else:
        # Submit to local Docker.
        # This loop only runs if array_size > 0
        result = {"jobId": [], "redun_job_id": []}
        for i in range(array_size):
            container_id = run_docker(
                command, image=image, array_index=i, **get_docker_job_options(job_options)
            )
            result["jobId"].append(container_id)
            result["redun_job_id"].append(job.id)

        # Otherwise, submit one non-array job
        if not array_size:
            container_id = run_docker(command, image=image, **get_docker_job_options(job_options))
            result = {"jobId": container_id, "redun_job_id": job.id}
    return result


def submit_command(
    image: str,
    queue: str,
    s3_scratch_prefix: str,
    job: Job,
    command: str,
    job_options: dict = {},
    debug: bool = False,
    aws_region: str = aws_utils.DEFAULT_AWS_REGION,
) -> dict:
    """
    Submit a shell command to AWS Batch or Docker (debug=True).
    """
    input_path = aws_utils.get_job_scratch_file(s3_scratch_prefix, job, aws_utils.S3_SCRATCH_INPUT)
    output_path = aws_utils.get_job_scratch_file(
        s3_scratch_prefix, job, aws_utils.S3_SCRATCH_OUTPUT
    )
    error_path = aws_utils.get_job_scratch_file(s3_scratch_prefix, job, aws_utils.S3_SCRATCH_ERROR)
    status_path = aws_utils.get_job_scratch_file(
        s3_scratch_prefix, job, aws_utils.S3_SCRATCH_STATUS
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
            exit_command="exit 1" if not debug else "",
        ),
    ]

    if not debug:
        # Submit to AWS Batch.
        assert job.eval_hash
        job_name = get_batch_job_name(
            job_options.get("job_name_prefix", "batch-job"), job.eval_hash
        )

        # Submit to AWS Batch.
        return batch_submit(
            shell_command,
            queue,
            image=image,
            job_name=job_name,
            job_def_suffix="-redun-jd",
            aws_region=aws_region,
            **get_batch_job_options(job_options),
        )
    else:
        # Submit to local Docker.
        container_id = run_docker(
            shell_command, image=image, **get_docker_job_options(job_options)
        )
        return {"jobId": container_id, "redun_job_id": job.id}


def parse_task_error(
    s3_scratch_prefix: str, job: Job, batch_job_metadata: Optional[dict] = None
) -> Tuple[Exception, "Traceback"]:
    """
    Parse task error from s3 scratch path.
    """
    assert job.task

    error_path = aws_utils.get_job_scratch_file(s3_scratch_prefix, job, aws_utils.S3_SCRATCH_ERROR)
    error_file = File(error_path)

    if not job.task.script:
        # Normal Tasks (non-script) store errors as Pickled exception, traceback tuples.
        if error_file.exists():
            error, error_traceback = pickle.loads(cast(bytes, error_file.read("rb")))
        else:
            if batch_job_metadata:
                try:
                    status_reason = batch_job_metadata["attempts"][-1]["statusReason"]
                except (KeyError, IndexError):
                    status_reason = ""
            else:
                status_reason = ""

            if status_reason == BATCH_JOB_TIMEOUT_ERROR:
                error = AWSBatchJobTimeoutError(BATCH_JOB_TIMEOUT_ERROR)
            else:
                error = AWSBatchError(
                    "Exception and traceback could not be found for AWS Batch Job."
                )
            error_traceback = Traceback.from_error(error)
    else:
        # Script task.
        if error_file.exists():
            error = ScriptError(cast(bytes, error_file.read("rb")))
        else:
            error = AWSBatchError("stderr could not be found for AWS Batch Job.")
        error_traceback = Traceback.from_error(error)

    return error, error_traceback


def parse_task_logs(
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


def iter_log_stream(
    log_group_name: str,
    log_stream: str,
    limit: Optional[int] = None,
    reverse: bool = False,
    required: bool = True,
    aws_region: str = aws_utils.DEFAULT_AWS_REGION,
) -> Iterator[dict]:
    """
    Iterate through the events of logStream.
    """
    logs_client = aws_utils.get_aws_client("logs", aws_region=aws_region)
    try:
        response = logs_client.get_log_events(
            logGroupName=log_group_name,
            logStreamName=log_stream,
            startFromHead=not reverse,
            # boto API does not allow passing None, so we must fully exclude the parameter.
            **{"limit": limit} if limit else {},
        )
    except logs_client.exceptions.ResourceNotFoundException as error:
        if required:
            # If logs are required, raise an error.
            raise error
        else:
            return

    while True:
        events = response["events"]

        # If no events, we are at the end of the stream.
        if not events:
            break

        if reverse:
            events = reversed(events)
        yield from events

        if not reverse:
            next_token = response["nextForwardToken"]
        else:
            next_token = response["nextBackwardToken"]
        response = logs_client.get_log_events(
            logGroupName=log_group_name,
            logStreamName=log_stream,
            nextToken=next_token,
            # boto API does not allow passing None, so we must fully exclude the parameter.
            **{"limit": limit} if limit else {},
        )


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
    if not job:
        # Job is no longer present in AWS API. Return no logs.
        return
    log_stream = job["container"].get("logStreamName")
    if not log_stream:
        # No log stream is present. Return no logs.
        return

    yield from iter_log_stream(
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


def iter_local_job_status(s3_scratch_prefix: str, job_id2job: Dict[str, "Job"]) -> Iterator[dict]:
    """
    Returns local Docker jobs grouped by their status.
    """
    running_containers = subprocess.check_output(["docker", "ps", "--no-trunc"]).decode("utf8")

    for job_id, redun_job in job_id2job.items():
        if job_id not in running_containers:
            # Job is done running.
            status_file = File(
                aws_utils.get_job_scratch_file(
                    s3_scratch_prefix, redun_job, aws_utils.S3_SCRATCH_STATUS
                )
            )
            output_file = File(
                aws_utils.get_job_scratch_file(
                    s3_scratch_prefix, redun_job, aws_utils.S3_SCRATCH_OUTPUT
                )
            )

            # Get docker logs and remove container.
            logs = subprocess.check_output(["docker", "logs", job_id]).decode("utf8")
            logs += "Removing container...\n"
            logs += subprocess.check_output(["docker", "rm", job_id]).decode("utf8")

            # TODO: Simplify whether status file is always used or not.
            if status_file.exists():
                succeeded = status_file.read().strip() == "ok"
            else:
                succeeded = output_file.exists()

            status = SUCCEEDED if succeeded else FAILED
            yield {"jobId": job_id, "status": status, "logs": logs}


class AWSBatchError(Exception):
    pass


class AWSBatchJobTimeoutError(Exception):
    """
    Custom exception to raise when AWS Batch Jobs are killed due to timeout.
    """

    pass


@register_executor("aws_batch")
class AWSBatchExecutor(Executor):
    def __init__(self, name: str, scheduler: Optional["Scheduler"] = None, config=None):
        super().__init__(name, scheduler=scheduler)
        if config is None:
            raise ValueError("AWSBatchExecutor requires config.")

        # Required config.
        self.image = config["image"]
        self.queue = config["queue"]
        self.s3_scratch_prefix = config["s3_scratch"]

        # Optional config.
        self.aws_region = config.get("aws_region", aws_utils.get_default_region())
        self.role = config.get("role")
        self.code_package = aws_utils.parse_code_package_config(config)
        self.code_file: Optional[File] = None
        self.debug = config.getboolean("debug", fallback=False)

        # Default task options.
        self.default_task_options = {
            "vcpus": config.getint("vcpus", 1),
            "gpus": config.getint("gpus", 0),
            "memory": config.getint("memory", 4),
            "retries": config.getint("retries", 1),
            "role": config.get("role"),
            "job_name_prefix": config.get("job_name_prefix", "redun-job"),
        }
        if config.get("batch_tags"):
            self.default_task_options["batch_tags"] = json.loads(config.get("batch_tags"))
        self.use_default_batch_tags = config.getboolean("default_batch_tags", True)

        self.is_running = False
        # We use an OrderedDict in order to retain submission order.
        self.pending_batch_jobs: Dict[str, "Job"] = OrderedDict()
        self.preexisting_batch_jobs: Dict[str, str] = {}  # Job hash -> Job ID

        if not self.debug:
            self.interval = config.getfloat("job_monitor_interval", 5.0)
        else:
            self.interval = config.getfloat("job_monitor_interval", 0.2)

        self._thread: Optional[threading.Thread] = None
        self.arrayer = JobArrayer(
            executor=self,
            submit_interval=self.interval,
            stale_time=config.getfloat("job_stale_time", 3.0),
            min_array_size=config.getint("min_array_size", 5),
            max_array_size=config.getint("max_array_size", 1000),
        )
        self._aws_user: Optional[str] = None

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
                aws_utils.get_array_scratch_file(
                    self.s3_scratch_prefix, parent_hash, aws_utils.S3_SCRATCH_HASHES
                )
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
        assert self.scheduler
        chunk_size = 100
        pending_truncate = 10

        try:
            while self.is_running and (self.pending_batch_jobs or self.arrayer.num_pending):
                if self.scheduler.logger.level >= logging.DEBUG:
                    self.log(
                        f"Preparing {self.arrayer.num_pending} job(s) for Job Arrays.",
                        level=logging.DEBUG,
                    )
                    self.log(
                        f"Waiting on {len(self.pending_batch_jobs)} Batch job(s): "
                        + " ".join(sorted(self.pending_batch_jobs.keys())),
                        level=logging.DEBUG,
                    )
                if not self.debug:
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

                else:
                    # Copy pending_batch_jobs since it can change due to new submissions.
                    jobs = iter_local_job_status(
                        self.s3_scratch_prefix, dict(self.pending_batch_jobs)
                    )
                    for job in jobs:
                        self._process_job_status(job)
                time.sleep(self.interval)

        except Exception as error:
            # Since we run this is method at the top-level of a thread, we
            # need to catch all exceptions so we can properly report them to
            # the scheduler.
            self.scheduler.reject_job(None, error)

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
            assert redun_job.task
            if redun_job.task.script:
                # Script tasks will report their status in a status file.
                status_file = File(
                    aws_utils.get_job_scratch_file(
                        self.s3_scratch_prefix, redun_job, aws_utils.S3_SCRATCH_STATUS
                    )
                )
                if status_file.exists():
                    return status_file.read().strip() == "ok", container_reason
            else:
                # Non-script tasks only create an output file if it is successful.
                output_file = File(
                    aws_utils.get_job_scratch_file(
                        self.s3_scratch_prefix, redun_job, aws_utils.S3_SCRATCH_OUTPUT
                    )
                )
                return output_file.exists(), container_reason

        return False, container_reason

    def _process_job_status(self, job: dict) -> None:
        """
        Process AWS Batch job statuses.
        """
        assert self.scheduler
        job_status: Optional[str] = None

        # Determine job status.
        if job["status"] == SUCCEEDED:
            job_status = SUCCEEDED

        elif job["status"] == FAILED:
            can_override, container_reason = self._can_override_failed(job)
            if can_override:
                job_status = SUCCEEDED
                self.scheduler.log("NOTE: Overriding AWS Batch error: {}".format(container_reason))
            else:
                job_status = FAILED
        else:
            return

        # Determine redun Job and job_tags.
        redun_job = self.pending_batch_jobs.pop(job["jobId"])
        job_tags = []
        if not self.debug:
            job_tags.append(("aws_batch_job", job["jobId"]))
            log_stream = job.get("container", {}).get("logStreamName")
            if log_stream:
                job_tags.append(("aws_log_stream", log_stream))

        if job_status == SUCCEEDED:
            # Assume a recently completed job has valid results.
            result, exists = self._get_job_output(redun_job, check_valid=False)
            if exists:
                self.scheduler.done_job(redun_job, result, job_tags=job_tags)
            else:
                # This can happen if job ended in an inconsistent state.
                self.scheduler.reject_job(
                    redun_job,
                    FileNotFoundError(
                        aws_utils.get_job_scratch_file(
                            self.s3_scratch_prefix, redun_job, aws_utils.S3_SCRATCH_OUTPUT
                        )
                    ),
                    job_tags=job_tags,
                )
        elif job_status == FAILED:
            error, error_traceback = parse_task_error(
                self.s3_scratch_prefix, redun_job, batch_job_metadata=job
            )
            if not self.debug:
                logs = [f"*** CloudWatch logs for AWS Batch job {job['jobId']}:\n"]
                if container_reason:
                    logs.append(f"container.reason: {container_reason}\n")

                try:
                    status_reason = job["attempts"][-1]["statusReason"]
                except (KeyError, IndexError):
                    status_reason = ""
                if status_reason:
                    logs.append(f"statusReason: {status_reason}\n")

                logs.extend(
                    parse_task_logs(job["jobId"], required=False, aws_region=self.aws_region)
                )
                error_traceback.logs = logs
            else:
                error_traceback.logs = [line + "\n" for line in job["logs"].split("\n")]
            self.scheduler.reject_job(
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
        batch_tags = {
            **self.default_task_options.get("batch_tags", {}),
            **default_tags,
            **job_options.get("batch_tags", {}),
        }
        if batch_tags:
            task_options["batch_tags"] = batch_tags

        return task_options

    def _get_job_output(self, job: Job, check_valid: bool = True) -> Tuple[Any, bool]:
        """
        Return job output if it exists.

        Returns a tuple of (result, exists).
        """
        assert self.scheduler

        output_file = File(
            aws_utils.get_job_scratch_file(
                self.s3_scratch_prefix, job, aws_utils.S3_SCRATCH_OUTPUT
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

        # If we are not in debug mode and this is the first submission gather inflight jobs. In
        # debug mode, we are running on docker locally so there is no need to hit the Batch API to
        # gather jobs as we are not going to run on batch. We also check is_running here as a way
        # of determining whether this is the first submission or not. If we are already running,
        # then we know we have already had jobs submitted and done the inflight check so no
        # reason to do that again here.
        if not self.debug and not self.is_running:
            # Precompute existing inflight jobs for job reuniting.
            self.gather_inflight_jobs()

        # Package code if necessary and we have not already done so. If code_package is False,
        # then we can skip this step. Additionally, if we have already packaged and set code_file,
        # then we do not need to repackage.
        if self.code_package is not False and self.code_file is None:
            code_package = self.code_package or {}
            assert isinstance(code_package, dict)
            self.code_file = aws_utils.package_code(self.s3_scratch_prefix, code_package)

        job_dir = aws_utils.get_job_scratch_dir(self.s3_scratch_prefix, job)
        job_type = "AWS Batch job" if not self.debug else "Docker container"

        # Determine job options.
        task_options = self._get_job_options(job)
        use_cache = task_options.get("cache", True)

        # Determine if we can reunite with a previous Batch output or job.
        batch_job_id: Optional[str] = None
        if use_cache and job.eval_hash in self.preexisting_batch_jobs:
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
        queue = task_options.pop("queue", self.queue)
        # Generate a unique name for job with no '-' to simplify job name parsing.
        array_uuid = str(uuid.uuid4()).replace("-", "")

        job_type = "AWS Batch job" if not self.debug else "Docker container"

        # Setup input, output and error path files.
        # Input file is a pickled list of args, and kwargs, for each child job.
        input_file = aws_utils.get_array_scratch_file(
            self.s3_scratch_prefix, array_uuid, aws_utils.S3_SCRATCH_INPUT
        )
        with File(input_file).open("wb") as out:
            pickle_dump([all_args, all_kwargs], out)

        # Output file is a plaintext list of output paths, for each child job.
        output_file = aws_utils.get_array_scratch_file(
            self.s3_scratch_prefix, array_uuid, aws_utils.S3_SCRATCH_OUTPUT
        )
        output_paths = [
            aws_utils.get_job_scratch_file(
                self.s3_scratch_prefix, job, aws_utils.S3_SCRATCH_OUTPUT
            )
            for job in jobs
        ]
        with File(output_file).open("w") as ofile:
            json.dump(output_paths, ofile)

        # Error file is a plaintext list of error paths, one for each child job.
        error_file = aws_utils.get_array_scratch_file(
            self.s3_scratch_prefix, array_uuid, aws_utils.S3_SCRATCH_ERROR
        )
        error_paths = [
            aws_utils.get_job_scratch_file(self.s3_scratch_prefix, job, aws_utils.S3_SCRATCH_ERROR)
            for job in jobs
        ]
        with File(error_file).open("w") as efile:
            json.dump(error_paths, efile)

        # Eval hash file is plaintext hashes of child jobs for matching for job reuniting.
        eval_file = aws_utils.get_array_scratch_file(
            self.s3_scratch_prefix, array_uuid, aws_utils.S3_SCRATCH_HASHES
        )
        with File(eval_file).open("w") as eval_f:
            eval_f.write("\n".join([job.eval_hash for job in jobs]))  # type: ignore

        batch_resp = submit_task(
            image,
            queue,
            self.s3_scratch_prefix,
            job,
            job.task,
            job_options=task_options,
            debug=self.debug,
            code_file=self.code_file,
            aws_region=self.aws_region,
            array_uuid=array_uuid,
            array_size=array_size,
        )

        if self.debug:
            # Debug mode just starts N docker containers
            array_job_id = "None"
            for i in range(array_size):
                self.pending_batch_jobs[batch_resp["jobId"][i]] = jobs[i]
        else:
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
            "  retry_attempts  = {retries}\n"
            "  debug           = {debug}".format(
                array_job_id=array_job_id,
                array_size=array_size,
                job_type=job_type,
                batch_job=array_job_id,
                job_dir=aws_utils.get_array_scratch_file(self.s3_scratch_prefix, array_uuid, ""),
                job_name=batch_resp.get("jobName"),
                retries=batch_resp.get("ResponseMetadata", {}).get("RetryAttempts"),
                debug=self.debug,
            )
        )

        return array_uuid

    def _submit_single_job(self, job: Job, args: Tuple, kwargs: dict) -> None:
        """
        Actually submits a job. Caching detects if it should be part
        of an array job
        """
        assert job.task
        task_options = self._get_job_options(job)
        image = task_options.pop("image", self.image)
        queue = task_options.pop("queue", self.queue)

        job_dir = aws_utils.get_job_scratch_dir(self.s3_scratch_prefix, job)
        job_type = "AWS Batch job" if not self.debug else "Docker container"

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
                debug=self.debug,
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
                debug=self.debug,
            )

        self.log(
            "submit redun job {redun_job} as {job_type} {batch_job}:\n"
            "  job_id          = {batch_job}\n"
            "  job_name        = {job_name}\n"
            "  s3_scratch_path = {job_dir}\n"
            "  retry_attempts  = {retries}\n"
            "  debug           = {debug}".format(
                debug=self.debug,
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
