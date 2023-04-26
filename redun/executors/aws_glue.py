import json
import os
import tempfile
import threading
import time
from collections import OrderedDict, deque
from typing import Any, Deque, Dict, Iterator, List, Optional, Tuple, Union, cast

from redun.executors import aws_utils
from redun.executors.base import Executor, register_executor
from redun.executors.code_packaging import create_zip, package_code, parse_code_package_config
from redun.executors.command import REDUN_PROG, REDUN_REQUIRED_VERSION
from redun.executors.scratch import (
    SCRATCH_ERROR,
    SCRATCH_INPUT,
    SCRATCH_OUTPUT,
    ExceptionNotFoundError,
    get_job_scratch_dir,
    get_job_scratch_file,
)
from redun.executors.scratch import parse_job_error as _parse_job_error
from redun.executors.scratch import parse_job_result
from redun.file import File
from redun.hashing import hash_stream, hash_text
from redun.scheduler import Job, Scheduler, Traceback
from redun.task import CacheScope, Task
from redun.utils import pickle_dump

ARGS = ["code", "script", "task", "input", "output", "error"]
VALID_GLUE_WORKERS = ["Standard", "G.1X", "G.2X"]
ONESHOT_FILE = "glue_oneshot.py.txt"

# AWS Glue job statuses
GLUE_JOB_STATUSES = aws_utils.JobStatus(
    all=["STARTING", "RUNNING", "STOPPING", "SUCCEEDED", "FAILED", "ERROR", "STOPPED", "TIMEOUT"],
    inflight=["STARTING", "RUNNING", "STOPPING"],
    pending=["STARTING"],
    success=["SUCCEEDED"],
    failure=["FAILED", "ERROR"],
    stopped=["STOPPED"],
    timeout=["TIMEOUT"],
)

# These packages are needed for the redun lib to work on glue.
# Versions should remain up to date with setup.py
DEFAULT_ADDITIONAL_PYTHON_MODULES = [
    "alembic>=1.4",
    "mako",
    "promise",
    "s3fs>=2021.11.1",
    "sqlalchemy>=1.4.0or<2.1",  # Don't use commas as AWS won't parse them.
]


def get_spark_history_dir(s3_scratch_prefix: str) -> str:
    """
    Returns s3 scratch path for Spark UI monitoring files.
    """
    return os.path.join(s3_scratch_prefix, "glue", "spark_history")


def get_glue_oneshot_scratch_file(s3_scratch_prefix: str, code_hash: str) -> str:
    """
    Returns s3 scratch path for a code package tar file.
    """
    return os.path.join(s3_scratch_prefix, "glue", f"oneshot-{code_hash}.py")


def get_redun_lib_scratch_file(s3_scratch_prefix: str, lib_hash: str) -> str:
    """
    Returns s3 scratch path for a code package tar file.
    """
    return os.path.join(s3_scratch_prefix, "glue", f"redun-{lib_hash}.zip")


def get_redun_lib_files() -> Iterator[str]:
    """
    Iterates through the files of the redun library.
    """
    exclude_dirs = ["__pycache__", "/tests/"]

    redun_module = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    for root, dirs, files in os.walk(redun_module):
        for file in files:
            full_filepath = os.path.join(root, file)
            if all(pattern not in full_filepath for pattern in exclude_dirs):
                yield full_filepath


def package_redun_lib(s3_scratch_prefix: str) -> File:
    """
    Package redun lib to S3.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        base_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        temp_path = os.path.join(tmpdir, "redun.zip")
        lib_files = get_redun_lib_files()
        temp_file = create_zip(temp_path, base_path, lib_files)

        with temp_file.open("rb") as infile:
            lib_hash = hash_stream(infile)
        lib_file = File(get_redun_lib_scratch_file(s3_scratch_prefix, lib_hash))
        if not lib_file.exists():
            temp_file.copy_to(lib_file)
    return lib_file


def get_or_create_glue_job_definition(
    script_location: str,
    role: str,
    temp_dir: str,
    extra_py_files: str,
    spark_history_dir: str,
    additional_python_modules: List[str] = DEFAULT_ADDITIONAL_PYTHON_MODULES,
    aws_region: str = aws_utils.DEFAULT_AWS_REGION,
) -> str:
    """
    Gets or creates an AWS Glue Job.

    Parameters
    ----------
    script_location : str
        S3 path of script that runs Spark job.

    role : str
        ARN of IAM role to associate with Glue job.

    temp_dir : str
        S3 path of scratch directory associated with job data and code.

    extra_py_files : str
        Comma separated S3 file paths for additional Python code needed for job.

    spark_history_dir : str
        S3 path where Spark event logs are stored.

    additional_python_modules : List[str]
        Python modules to be installed with pip before job start.

    aws_region : str
        AWS region in which to run the Glue job.

    Returns
    -------
    str
        Unique Glue job definition name.
    """
    client = aws_utils.get_aws_client("glue", aws_region=aws_region)

    # Define job definition.
    glue_job_def = dict(
        Description=f"Redun oneshot glue job {REDUN_REQUIRED_VERSION}",
        Role=role,
        ExecutionProperty={
            "MaxConcurrentRuns": 1000,
        },  # account-max value.
        Command={
            "Name": "glueetl",
            "ScriptLocation": script_location,
            "PythonVersion": "3",
        },
        DefaultArguments={
            "--TempDir": temp_dir,
            "--enable-s3-parquet-optimized-committer": "true",
            "--extra-py-files": extra_py_files,
            "--additional-python-modules": ",".join(additional_python_modules),
            "--job-bookmark-option": "job-bookmark-disable",
            "--job-language": "python",
            "--enable-spark-ui": "true",
            "--enable-job-insights": "true",
            "--spark-event-logs-path": spark_history_dir,
        },
        MaxRetries=0,
        NumberOfWorkers=2,  # Jobs will override this, so set to minimum value.
        WorkerType="Standard",
        GlueVersion="3.0",
        Timeout=2880,
    )
    glue_job_def_hash = hash_text(json.dumps(glue_job_def, sort_keys=True))
    glue_job_name = f"redun-glue-{glue_job_def_hash}"

    try:
        # See if job definition already exists.
        client.get_job(JobName=glue_job_name)
    except client.exceptions.EntityNotFoundException:
        # Create job definition.
        resp = client.create_job(Name=glue_job_name, **glue_job_def)
        assert resp["Name"] == glue_job_name

    return glue_job_name


def get_default_glue_service_role(
    account_num: Optional[str] = None, aws_region: str = aws_utils.DEFAULT_AWS_REGION
) -> str:
    """
    Returns the default Glue service role for the current account.
    """
    if not account_num:
        caller_id = aws_utils.get_aws_client("sts", aws_region=aws_region).get_caller_identity()
        account_num = caller_id["Account"]
    return f"arn:aws:iam::{account_num}:role/service-role/AWSGlueServiceRole"


@register_executor("aws_glue")
class AWSGlueExecutor(Executor):
    """
    A redun Executor for running jobs on AWS Glue.
    """

    def __init__(self, name: str, scheduler: Optional[Scheduler] = None, config=None):
        super().__init__(name, scheduler=scheduler)
        if config is None:
            raise ValueError("AWSGlueExecutor requires config.")

        # Required config
        self.s3_scratch_prefix = config["s3_scratch"]

        # Optional config
        self.aws_region = config.get("aws_region", aws_utils.get_default_region())
        self.role = config.get("role") or get_default_glue_service_role(aws_region=self.aws_region)
        self.code_package = parse_code_package_config(config)
        self.code_file: Optional[File] = None
        self.debug = config.getboolean("debug", fallback=False)
        self.interval = config.getfloat("job_monitor_interval", 10.0)
        self.retry_interval = config.getfloat("job_retry_interval", 60.0)
        self.glue_job_prefix = config.get("glue_job_prefix", REDUN_PROG.upper())
        self.glue_job_name: Optional[str] = None
        self.spark_history_dir = config.get(
            "spark_history_dir", get_spark_history_dir(self.s3_scratch_prefix)
        )

        # Default task options
        self.default_task_options = {
            "workers": config.getint("workers", 10),
            "worker_type": config.get("worker_type", "G.1X"),
            "timeout": config.getint("timeout", 2880),
            "role": config.get("role"),
            "additional_libs": [],
            "extra_files": [],
        }

        # Execution state.
        self.is_running = False
        self._monitor_thread = threading.Thread(target=self._monitor, daemon=False)
        self._submit_thread = threading.Thread(target=self._submission_thread, daemon=False)
        self.pending_glue_jobs: Deque["Job"] = deque()
        self.running_glue_jobs: Dict[str, "Job"] = OrderedDict()
        self.preexisting_glue_jobs: Dict[str, str] = {}  # Job hash -> Job ID
        self._oneshot_path: Optional[str] = None
        self.redun_zip_location: Optional[str] = None

    def gather_inflight_jobs(self) -> None:
        for run in self.get_jobs(statuses=GLUE_JOB_STATUSES.inflight):
            hash = run["Arguments"].get("--job-hash")
            self.preexisting_glue_jobs[hash] = run["Id"]

    def get_jobs(self, statuses: Optional[List[str]] = None) -> Iterator[dict]:
        """
        Gets all job runs with given status.
        """
        client = aws_utils.get_aws_client("glue", aws_region=self.aws_region)
        paginator = client.get_paginator("get_job_runs")

        for page in paginator.paginate(JobName=self.glue_job_name):
            for run in page["JobRuns"]:
                if statuses:
                    if run["JobRunState"] in statuses:
                        yield run
                else:
                    yield run

    def get_or_create_job_definition(self) -> None:
        """
        Get or create the default Glue job.
        """
        if not self._oneshot_path:
            # Copy Glue oneshot file to S3 with unique hash.
            oneshot_file = File(
                os.path.join(os.path.dirname(os.path.abspath(__file__)), ONESHOT_FILE)
            )
            oneshot_hash = hash_text(cast(str, oneshot_file.read()))
            oneshot_s3_path = get_glue_oneshot_scratch_file(self.s3_scratch_prefix, oneshot_hash)
            oneshot_s3_file = File(oneshot_s3_path)
            if not oneshot_s3_file.exists():
                oneshot_file.copy_to(oneshot_s3_file)
            self._oneshot_path = oneshot_s3_path

        if not self.redun_zip_location:
            self.redun_zip_location = package_redun_lib(self.s3_scratch_prefix).path

        self.glue_job_name = get_or_create_glue_job_definition(
            script_location=self._oneshot_path,
            role=self.role,
            spark_history_dir=self.spark_history_dir,
            temp_dir=self.s3_scratch_prefix,
            extra_py_files=self.redun_zip_location,
            aws_region=self.aws_region,
        )

    def _start(self) -> None:
        """
        Starts monitoring thread
        """
        if not self.is_running:
            self.is_running = True

        if not self._monitor_thread.is_alive():
            self._monitor_thread = threading.Thread(target=self._monitor, daemon=False)
            self._monitor_thread.start()

        if not self._submit_thread.is_alive():
            self._submit_thread = threading.Thread(target=self._submission_thread, daemon=False)
            self._submit_thread.start()

    def _monitor(self) -> None:
        """Thread for monitoring running AWS Glue jobs."""
        assert self._scheduler
        assert self.glue_job_name

        try:
            while self.is_running and (self.running_glue_jobs or self.pending_glue_jobs):
                # Process running glue jobs
                jobs = glue_describe_jobs(
                    list(self.running_glue_jobs.keys()),
                    glue_job_name=self.glue_job_name,
                    aws_region=self.aws_region,
                )

                for job in jobs:
                    self._process_job_status(job)

                time.sleep(self.interval)

        except Exception as error:
            self._scheduler.reject_job(None, error)

        self.stop()

    def _submission_thread(self) -> None:
        """
        Thread for submitting AWS Glue jobs.

        Jobs are submitted in approximately the order in which they are started.
        Job submission may fail due to too many other running jobs or insufficient DPUs
        available. If submission fails, that job moves to the back of the pending jobs
        queue in case there are other jobs that need fewer resources that could be
        successfully submitted in the meantime. Once submission fails for 5 jobs in
        a row, we wait `self.retry_interval` seconds before submitting another job.
        """
        assert self._scheduler
        try:
            while self.is_running and self.pending_glue_jobs:
                fail_counter = 0
                while fail_counter < 5 and self.pending_glue_jobs:
                    job = self.pending_glue_jobs.popleft()
                    job_id = self.submit_pending_job(job)

                    if job_id is None:
                        fail_counter += 1
                        self.pending_glue_jobs.append(job)
                    else:
                        self.running_glue_jobs[job_id] = job
                        fail_counter = 0

                time.sleep(self.retry_interval)

        except Exception as error:
            self._scheduler.reject_job(None, error)

        # Thread can exit when there are no more pending jobs. That's okay,
        # as new job submissions will restart it.

    def stop(self) -> None:
        self.is_running = False

    def _process_job_status(self, job: dict) -> None:
        assert self._scheduler

        error: Optional[Exception] = None
        error_traceback: Optional[Traceback] = None
        result: Optional[Any] = None
        redun_job = self.running_glue_jobs.get(job["Id"])
        assert redun_job

        if job["JobRunState"] in GLUE_JOB_STATUSES.success:
            result, exists = parse_job_result(self.s3_scratch_prefix, redun_job)
            if exists:
                error = None
            else:
                error = FileNotFoundError(
                    get_job_scratch_file(self.s3_scratch_prefix, redun_job, SCRATCH_OUTPUT)
                )
                error_traceback = None

        elif job["JobRunState"] in GLUE_JOB_STATUSES.stopped:
            error = AWSGlueJobStoppedError("Job stopped by user.")

        elif job["JobRunState"] in GLUE_JOB_STATUSES.failure:
            # Errors may not be unpicklable out of the Glue environment, so use CloudWatch
            # job insights to get a traceback.
            if self.debug:
                error, error_traceback = parse_job_error(self.s3_scratch_prefix, redun_job, job)
            else:
                error = AWSGlueError("Glue job failed: see log info.")
                error_traceback = Traceback.from_error(error)

                error_traceback.logs = get_job_insight_traceback(
                    job_id=job["Id"],
                    log_group_name=job["LogGroupName"],
                    aws_region=self.aws_region,
                    max_results=100,
                )

        elif job["JobRunState"] in GLUE_JOB_STATUSES.timeout:
            error = AWSGlueJobTimeoutError(job.get("ErrorMessage", ""))
            error_traceback = Traceback.from_error(error)

        # Job is still running
        else:
            return

        self.running_glue_jobs.pop(job["Id"])
        if error:
            self._scheduler.reject_job(redun_job, error, error_traceback=error_traceback)
        else:
            self._scheduler.done_job(redun_job, result)

    def _get_job_options(self, job: Job) -> dict:
        """
        Determines task options for a job.
        """
        task_options = dict(self.default_task_options)
        task_options.update(job.get_options())
        return task_options

    def submit(self, job: Job) -> None:
        """
        Submit job to executor.
        """
        assert self._scheduler
        assert job.args

        # Check glue job definition exists. Otherwise, create it.
        if not self.glue_job_name:
            self.get_or_create_job_definition()

        # Gather inflight jobs if this is the first submission, using `is_running` as a
        # way of determining if this is the first submission or not. If we are already running,
        # then we know we have already had jobs submitted and done the inflight check so no
        # reason to do that again here.
        if not self.debug and not self.is_running:
            # Precompute existing inflight jobs for job reuniting.
            self.gather_inflight_jobs()

        # Package code if not already done
        if self.code_package is not False and self.code_file is None:
            code_package = self.code_package or {}
            assert isinstance(code_package, dict)
            self.code_file = package_code(self.s3_scratch_prefix, code_package, use_zip=True)

        # Determine job options
        task_options = self._get_job_options(job)

        # Determine if we can reunite with a previous Glue output or job.
        glue_job_id: Optional[str] = None
        cache_scope = CacheScope(task_options.get("cache_scope", CacheScope.BACKEND))

        if cache_scope == CacheScope.BACKEND and job.eval_hash in self.preexisting_glue_jobs:
            assert self.glue_job_name
            glue_job_id = self.preexisting_glue_jobs.pop(job.eval_hash)

            # Make sure glue API still has a status on this job
            existing_job = next(
                glue_describe_jobs(
                    [glue_job_id], glue_job_name=self.glue_job_name, aws_region=self.aws_region
                )
            )

            if existing_job:
                glue_job_id = existing_job["Id"]
                assert glue_job_id  # for mypy
                self.running_glue_jobs[glue_job_id] = job
                self.log(
                    "reunite redun job {redun_job} with Glue job {glue_job}:\n".format(
                        redun_job=job.id, glue_job=glue_job_id
                    )
                )
            else:
                glue_job_id = None

        if glue_job_id is None:
            # Set up files and data for run.
            input_path = get_job_scratch_file(self.s3_scratch_prefix, job, SCRATCH_INPUT)
            input_file = File(input_path)
            with input_file.open("wb") as out:
                pickle_dump(job.args, out)

            self.pending_glue_jobs.append(job)

        self._start()

    def submit_pending_job(self, job: Job) -> Union[str, None]:
        """
        Returns true if job submission was successful
        """
        assert self.glue_job_name
        assert self.redun_zip_location
        assert self.code_file
        client = aws_utils.get_aws_client("glue", aws_region=self.aws_region)

        try:
            glue_resp = submit_glue_job(
                job,
                job.task,
                glue_job_name=self.glue_job_name,
                redun_zip_location=self.redun_zip_location,
                s3_scratch_prefix=self.s3_scratch_prefix,
                job_options=self._get_job_options(job),
                code_file=self.code_file,
                aws_region=self.aws_region,
            )
        except client.exceptions.ConcurrentRunsExceededException:
            self.log("Too many concurrent runs of the glue job. Waiting for some to complete...")
            return None
        except client.exceptions.ResourceNumberLimitExceededException:
            self.log("No AWS DPUs available. Waiting for some to free up...")
            return None

        self.log(
            "submit redun job {redun_job} as {job_type} {glue_job}:\n"
            "  job_id          = {glue_job}\n"
            "  job_name        = {job_name}\n"
            "  s3_scratch_path = {job_dir}\n"
            "  retries         = {retries}\n".format(
                redun_job=job.id,
                glue_job=glue_resp["JobRunId"],
                job_name=self.glue_job_name,
                job_type="AWS Glue job",
                job_dir=get_job_scratch_dir(self.s3_scratch_prefix, job),
                retries=glue_resp.get("ResponseMetadata", {}).get("RetryAttempts"),
            )
        )
        return glue_resp["JobRunId"]

    def scratch_root(self) -> str:
        return self.s3_scratch_prefix


def submit_glue_job(
    job: Job,
    a_task: Task,
    s3_scratch_prefix: str,
    glue_job_name: str,
    redun_zip_location: str,
    code_file: File,
    job_options: dict = {},
    aws_region: str = aws_utils.DEFAULT_AWS_REGION,
) -> Dict[str, Any]:
    """
    Submits a redun task to AWS glue.

    Parameters
    ----------
    job : Job
        The :class:`redun.scheduler.Job` that is in the process of resolving the Task `a_task`.

    a_task : Task
        The :class:`redun.task.Task` to be run by the `job`.

    s3_scratch_prefix : str
        Prefix for S3 url where job files, including additional Python and data files, will reside.

    glue_job_name : str
        Name of Glue job definition to run.

    redun_zip_location : str
        S3 path of zipped redun source code.

    code_file : File
        Code to be executed as a redun workflow. Path within `redun.file.File` should be an S3 url.

    job_options : Dict[str, Any]
        Option map to configure job initialization and execution.

        Required fields
            `worker_type`: str
            `timeout`: int
            `workers`: int

        Optional fields
            `additional_libs`: List[str]
            `extra_py_files`: List[str]
            `extra_files`: List[str]

    aws_region : str
        AWS region in which to run the Glue job.

    Returns
    -------
    Dict[str, Any]
        Response contains `JobRunId` field.

    Raises
    ------
    ValueError
        If `worker_type` is not a valid predefined Glue worker type string.
    """
    input_path = get_job_scratch_file(s3_scratch_prefix, job, SCRATCH_INPUT)
    output_path = get_job_scratch_file(s3_scratch_prefix, job, SCRATCH_OUTPUT)
    error_path = get_job_scratch_file(s3_scratch_prefix, job, SCRATCH_ERROR)

    # Assemble job arguments
    assert job.eval_hash
    glue_args = {
        "--check-version": REDUN_REQUIRED_VERSION,
        "--input": input_path,
        "--output": output_path,
        "--script": a_task.load_module,
        "--task": a_task.fullname,
        "--error": error_path,
        "--job-hash": job.eval_hash,
    }

    # Comma separated string of Python modules to be installed with pip before job start.
    if job_options.get("additional_libs"):
        glue_args["--additional-python-modules"] = ",".join(
            DEFAULT_ADDITIONAL_PYTHON_MODULES + job_options["additional_libs"]
        )

    # Extra python and data files are specified as comma separated strings.
    # Files are first copied to S3, as Glue requires them to be there.

    # Extra py files will be in an importable location at job start.
    # They can be either importable zip files, or .py source files.
    # Redun and the code to run are provided as importable zip files.
    scratch_dir = get_job_scratch_dir(s3_scratch_prefix, job)

    glue_args["--extra-py-files"] = ",".join([redun_zip_location, code_file.path]) + ","
    if job_options.get("extra_py_files"):
        addl_files = [aws_utils.copy_to_s3(f, scratch_dir) for f in job_options["extra_py_files"]]
        glue_args["--extra-py-files"] += ",".join(addl_files)

    # Extra files will be available in job's $PWD.
    if job_options.get("extra_files"):
        glue_args["--extra-files"] = ",".join(
            aws_utils.copy_to_s3(f, scratch_dir) for f in job_options["extra_files"]
        )

    # Validate job options
    if job_options["worker_type"] not in VALID_GLUE_WORKERS:
        raise ValueError(f"Invalid worker type {job_options['worker_type']}")

    # Submit glue job
    # Any submission exceptions need to be handled by calling function.
    glue_client = aws_utils.get_aws_client("glue", aws_region=aws_region)
    result = glue_client.start_job_run(
        JobName=glue_job_name,
        Arguments=glue_args,
        Timeout=job_options["timeout"],
        WorkerType=job_options["worker_type"],
        NumberOfWorkers=job_options["workers"],
    )

    return result


def glue_describe_jobs(
    job_ids: List[str], glue_job_name: str, aws_region: str = aws_utils.DEFAULT_AWS_REGION
) -> Iterator[Dict[str, Any]]:

    glue_client = aws_utils.get_aws_client("glue", aws_region=aws_region)

    for id in job_ids:
        response = glue_client.get_job_run(
            JobName=glue_job_name, RunId=id, PredecessorsIncluded=False
        )
        yield response.get("JobRun")


def parse_job_error(
    s3_scratch_prefix: str, job: Job, glue_job_metadata: Optional[dict] = None
) -> Tuple[Exception, "Traceback"]:
    """
    Parse job error from s3 scratch path.
    """
    error, error_traceback = _parse_job_error(s3_scratch_prefix, job)

    # Handle AWS Batch-specific errors.
    if isinstance(error, ExceptionNotFoundError) and glue_job_metadata:
        message = glue_job_metadata.get(
            "ErrorMessage", "Exception and traceback could not be found for AWS Glue Job"
        )
        error = AWSGlueError(message)
        error_traceback = Traceback.from_error(error)

    return error, error_traceback


def get_job_insight_traceback(
    job_id: str,
    log_group_name: str,
    aws_region: str = aws_utils.DEFAULT_AWS_REGION,
    max_results=200,
) -> List[str]:
    """
    Gets the traceback from AWS' Glue Job insights.
    """
    log_lines = aws_utils.iter_log_stream(
        log_group_name=f"{log_group_name}/logs-v2",
        log_stream=f"{job_id}-job-insights-rca-driver",
        limit=max_results,
        reverse=False,
        required=False,
        aws_region=aws_region,
    )

    if not log_lines:
        return ["No job insights found. See AWS Glue GUI online."]

    traceback = []
    for line in log_lines:
        message = line.get("message")
        if message and "Failure Reason" in message:
            # Message looks like "ERROR GlueException... [Glue Exception Analysis] {DICT we want}".
            try:
                traceback.extend(
                    json.loads(message[message.index("{") :])["Failure Reason"].split("\n")
                )
            except Exception:
                continue

    return traceback


class AWSGlueError(Exception):
    pass


class AWSGlueJobTimeoutError(Exception):
    """
    Custom exception to raise when AWS Glue jobs are killed due to timeout.
    """

    pass


class AWSGlueJobStoppedError(Exception):
    pass
