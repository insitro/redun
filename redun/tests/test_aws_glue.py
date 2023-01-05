import json
import os
import re
import zipfile
from typing import Any, Dict
from unittest.mock import Mock, patch

import boto3
import moto
from moto.core.models import base_decorator
from moto.glue.responses import GlueResponse

from redun import File, task
from redun.config import Config
from redun.executors.aws_glue import AWSGlueExecutor, get_redun_lib_files, package_redun_lib
from redun.executors.aws_utils import get_aws_client
from redun.executors.code_packaging import create_zip
from redun.scheduler import Job
from redun.tests.utils import mock_s3, mock_scheduler, use_tempdir, wait_until
from redun.utils import pickle_dumps


class RedunGlueBackend(moto.glue.models.GlueBackend):
    """
    Moto doesn't implement all the glue methods we need, so this class,
    combined with RedunGlueResponse, adds the necessary mock functionality.
    Submitted jobs automatically start in the RUNNING state and don't change
    state, but it's enough to test the state of the queue for our purposes.

    For strange moto internal reasons this is NOT thread safe and the internal
    glue state depends on which thread you're calling from :(
    """

    from moto.core import BaseBackend, BaseModel

    def __init__(self):
        self.job_id_num = 0
        self.job_defs: Dict[str, dict] = {}  # Job definitions.
        super(RedunGlueBackend, self).__init__()

    @property
    def client(self):
        return get_aws_client("glue")

    @property
    def urls(self):
        urls = {}
        for url_base in self.url_bases:
            for url_path, handler in self.url_paths.items():
                url = url_path.format(url_base)
                urls[url] = handler
        return urls

    @property
    def url_bases(self):
        return ["https?://glue.(.+).amazonaws.com"]

    @property
    def url_paths(self):
        return {"{0}/$": RedunGlueResponse.dispatch}

    def get_job(self, JobName: str) -> Dict[str, Any]:
        if JobName in self.job_defs:
            return {"Job": self.job_defs[JobName]}

        raise self.client.exceptions.EntityNotFoundException(
            error_response={"Error": {"Code": "blah", "Message": "oops", "client": self.client}},
            operation_name="get_job",
        )

    def get_job_run(self, **kwargs):
        return {
            "JobRun": {
                # "JobRunState": "SUCCEEDED",
                "JobRunState": "RUNNING",
                "Id": kwargs["RunId"],
            }
        }

    def create_job(self, **kwargs) -> Dict[str, str]:
        assert "Name" in kwargs
        jobname = kwargs["Name"]

        self.job_defs[jobname] = kwargs

        return {"Name": jobname}

    def start_job_run(
        self,
        JobName: str,
        Arguments: Dict,
        Timeout: int,
        WorkerType: str,
        NumberOfWorkers: int,
        **kwargs,
    ) -> Dict[str, str]:
        if JobName not in self.job_defs:
            raise self.client.exceptions.EntityNotFoundException(
                {
                    "Error": {
                        "Code": "ENOENT",
                        "Message": (
                            f"{JobName} not in definitions " f"{self.job_defs.keys()} for {self}"
                        ),
                    }
                },
                "start_job_run",
            )

        self.job_id_num += 1
        return {"JobRunId": str(self.job_id_num)}


redun_glue_backend = RedunGlueBackend()
mock_redun_glue = base_decorator({"global": redun_glue_backend})


class RedunGlueResponse(GlueResponse):
    @property
    def glue_backend(self):
        return redun_glue_backend

    def get_job(self):
        name = self.parameters.get("JobName")
        job = self.glue_backend.get_job(name)
        return json.dumps(job)

    def create_job(self):
        return json.dumps(self.glue_backend.create_job(**self.parameters))

    def start_job_run(self):
        return json.dumps(self.glue_backend.start_job_run(**self.parameters))

    def get_job_run(self):
        return json.dumps(self.glue_backend.get_job_run(**self.parameters))


# Creates a fresh redun glue backend for each test.
def setup_function():
    redun_glue_backend.jobs = {}


@moto.mock_s3
@mock_redun_glue
def mock_executor(scheduler, debug=False, code_package=False):
    """
    Returns an AWSGlueExecutor with AWS API mocks.
    """

    s3_scratch_prefix = "s3://example-bucket/redun/"
    config = Config(
        {
            "glue": {
                "s3_scratch": s3_scratch_prefix,
                "role": "arn:aws:iam::123:role/service-role/AWSGlueServiceRole",
                "job_monitor_interval": 1.0,
                "job_retry_interval": 0.5,
                "code_package": code_package,
                "debug": debug,
                "glue_job_name": "test_glue_job",
                "glue_code_location": "s3://example-bucket/",
                "redun_zip_path": "s3://example-bucket/redun.zip",
            }
        }
    )
    s3_client = boto3.client("s3", region_name="us-east-1")
    s3_client.create_bucket(Bucket="example-bucket")

    with File("s3://example-bucket/redun.zip").open("w") as fh:
        fh.write("I am definitely a real file")

    executor = AWSGlueExecutor("glue", scheduler, config["glue"])

    executor.get_jobs = Mock()
    executor.get_jobs.return_value = []

    return executor


@task()
def task1(x: int):
    return x + 10


@mock_s3
@mock_redun_glue
def test_executor_config(scheduler):
    config = Config(
        {
            "glue": {
                "s3_scratch": "s3_scratch_prefix",
                "aws_region": "pangea",
                "role": "overlord",
                "job_monitor_interval": 1.0,
                "code_includes": "*.txt",
                "glue_job_name": "test_glue_job",
                "glue_code_location": "s3://example-bucket/",
                "redun_zip_path": "s3://example-bucket/redun.zip",
            }
        }
    )
    s3_client = boto3.client("s3", region_name="us-east-1")
    s3_client.create_bucket(Bucket="example-bucket")

    with File("s3://example-bucket/redun.zip").open("w") as fh:
        fh.write("I am definitely a real file")

    executor = AWSGlueExecutor("glue", scheduler, config["glue"])

    assert executor.aws_region == "pangea"
    assert executor.role == "overlord"
    assert executor.interval == 1.0
    assert executor.code_package["includes"] == ["*.txt"]
    assert executor.debug is False


@use_tempdir
def test_redun_lib_files() -> None:
    """
    We should be able to create a zip archive of the redun library.
    """
    lib_files = list(get_redun_lib_files())
    assert len(lib_files) > 0

    base_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    zip_file = create_zip("redun.zip", base_path, lib_files)
    zip_obj = zipfile.ZipFile(zip_file.path)
    namelist = zip_obj.namelist()
    assert len(namelist) == len(lib_files)
    assert "redun/__init__.py" in namelist


@mock_s3
def test_package_redun_lib() -> None:
    """
    We should be able to package the redun lib to S3.
    """
    s3_client = boto3.client("s3", region_name="us-east-1")
    s3_client.create_bucket(Bucket="example-bucket")
    s3_scratch_prefix = "s3://example-bucket/redun/"

    redun_file = package_redun_lib(s3_scratch_prefix)
    assert re.match("s3://example-bucket/redun/glue/redun-[0-9a-f]{40}.zip", redun_file.path)


@mock_s3
@mock_redun_glue
@patch("redun.executors.aws_glue.glue_describe_jobs")
@patch("redun.executors.aws_glue.submit_glue_job")
def test_executor_inflight_glue_job(submit_job_mock, describe_jobs_mock) -> None:
    """
    Ensure we reunite with an inflight glue job
    """
    scheduler = mock_scheduler()
    executor = mock_executor(scheduler)
    assert executor.preexisting_glue_jobs == {}

    # Set up mocks to indicate a job is inflight.
    executor.get_jobs.return_value = [
        {"Id": "carrots", "Arguments": {"--job-hash": "hashbrowns"}, "JobRunState": "RUNNING"}
    ]
    describe_jobs_mock.return_value = iter(
        [{"Id": "carrots", "Arguments": {"--job-hash": "hashbrowns"}, "JobRunState": "RUNNING"}]
    )

    # Force scheduler to gather inflight jobs.
    executor.gather_inflight_jobs()

    assert executor.preexisting_glue_jobs == {"hashbrowns": "carrots"}

    # Submit a job with same hash and ensure it isn't actually submitted
    rjob = Job(task1, task1(10))
    rjob.id = "123"
    rjob.eval_hash = "hashbrowns"
    rjob.args = ((10,), {})
    executor.submit(rjob)
    executor.stop()
    executor._monitor_thread.join()
    executor._submit_thread.join()

    # Ensure no new job was submitted.
    assert submit_job_mock.call_count == 0
    assert executor.running_glue_jobs["carrots"] == rjob


@mock_s3
@mock_redun_glue
def test_job_def_creation() -> None:
    """
    Tests that the job definition and oneshot file are created
    on executor init.
    """

    # Code file should not exist.
    client = boto3.client("glue", region_name="us-east-1")
    s3client = boto3.client("s3", region_name="us-east-1")
    s3client.create_bucket(Bucket="example-bucket")

    scheduler = mock_scheduler()
    executor = mock_executor(scheduler)

    executor.get_or_create_job_definition()

    # Now job def should exist
    retval = client.get_job(JobName=executor.glue_job_name)
    assert retval["Job"]["Name"] == executor.glue_job_name
    assert retval["Job"]["Role"] == executor.role
    assert re.match(
        "s3://example-bucket/redun/glue/oneshot-.*.py", retval["Job"]["Command"]["ScriptLocation"]
    )

    # Glue oneshot should now exist on S3.
    codefile = File(retval["Job"]["Command"]["ScriptLocation"])
    assert codefile.exists()


@mock_redun_glue
@mock_s3
@patch("redun.executors.aws_glue.glue_describe_jobs")
def test_glue_submission_retry(describe_jobs_mock) -> None:
    scheduler = mock_scheduler()
    executor = mock_executor(scheduler)
    assert executor.preexisting_glue_jobs == {}

    # Make job submission return None and test resubmit works
    do_fail = True

    def get_retval(*args, **kwargs):
        if do_fail:
            return None
        return "potato"

    executor.submit_pending_job = Mock()
    executor.submit_pending_job.side_effect = get_retval

    # Submit a job and make sure it is in pending queue.
    rjob = Job(task1, task1(10))
    rjob.id = "123"
    rjob.eval_hash = "hashbrowns"
    rjob.args = ((10,), {})

    executor.submit(rjob)

    wait_until(lambda: executor.submit_pending_job.call_count > 0)
    assert list(executor.pending_glue_jobs) == [rjob]
    assert len(executor.running_glue_jobs) == 0

    # Now when job submission works, it should be removed from pending queue
    do_fail = False
    wait_until(
        lambda: (
            len(executor.pending_glue_jobs) == 0
            and list(executor.running_glue_jobs.keys()) == ["potato"]
        )
    )

    # Simulate AWS Glue completing with valid value.
    output_file = File("s3://example-bucket/redun/jobs/hashbrowns/output")
    output_file.write(pickle_dumps(task1.func(10)), mode="wb")

    # Update mocks to indicate a job is done.
    executor.get_jobs.return_value = [
        {"Id": "potato", "Arguments": {"--job-hash": "hashbrowns"}, "JobRunState": "SUCCEEDED"}
    ]
    describe_jobs_mock.return_value = iter(
        [{"Id": "potato", "Arguments": {"--job-hash": "hashbrowns"}, "JobRunState": "SUCCEEDED"}]
    )

    scheduler.batch_wait([rjob.id])
    executor.stop()
    executor._monitor_thread.join()
    executor._submit_thread.join()

    assert not scheduler.job_errors.get(rjob.id)
    assert scheduler.job_results[rjob.id] == 20


@mock_redun_glue
@mock_s3
@patch("redun.executors.aws_glue.submit_glue_job")
def test_glue_submit_job(submit_job_mock) -> None:
    scheduler = mock_scheduler()
    executor = mock_executor(scheduler, code_package=True)

    job = Job(task1, task1(10))
    job.id = "123"
    job.eval_hash = "eval_hash"
    job.args = ((10,), {})

    executor.submit(job)
    assert executor.is_running
    assert File("s3://example-bucket/redun/jobs/eval_hash/input").exists()

    executor.stop()
    executor._monitor_thread.join()
    executor._submit_thread.join()
