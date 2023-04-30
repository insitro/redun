from typing import Optional, Type, cast
from unittest.mock import Mock, patch

import boto3
from google.cloud import batch_v1

from redun import File, task
from redun.config import Config
from redun.executors.gcp_batch import GCPBatchExecutor
from redun.expression import TaskExpression
from redun.scheduler import Job, Scheduler, Traceback
from redun.tests.utils import mock_s3, mock_scheduler, use_tempdir, wait_until
from redun.utils import pickle_dumps

PROJECT = "project"
IMAGE = "gcr.io/gcp-project/redun"
REGION = "region"
GCS_SCRATCH_PREFIX = "gs://example-bucket/redun"


def mock_executor(scheduler, debug=False, code_package=False):
    """
    Returns an GCPBatchExecutor with GCP API mocks.
    """

    config = Config(
        {
            "gcp_batch": {
                "gcs_scratch": GCS_SCRATCH_PREFIX,
                "project": PROJECT,
                "region": PROJECT,
                "image": IMAGE,
                "code_package": code_package,
                "debug": debug,
            }
        }
    )

    return GCPBatchExecutor("batch", scheduler, config["gcp_batch"])


def _test_executor_config(scheduler: Scheduler) -> None:
    """
    Executor should be able to parse its config.
    """
    # Setup executor.
    config = Config(
        {
            "gcp_batch": {
                "image": IMAGE,
                "project": PROJECT,
                "region": REGION,
                "gcs_scratch": GCS_SCRATCH_PREFIX,
            }
        }
    )
    executor = GCPBatchExecutor("batch", scheduler, config["gcp_batch"])
    assert executor.image == IMAGE
    assert executor.project == PROJECT
    assert executor.region == REGION
    assert executor.gcs_scratch_prefix == GCS_SCRATCH_PREFIX
    assert isinstance(executor.code_package, dict)
    assert executor.debug is False


@patch("redun.executors.gcp_utils.get_gcp_client")
def test_executor_creation(get_gcp_client) -> None:
    """
    Ensure we reunite with inflight jobs
    """
    assert get_gcp_client.call_count == 0

    scheduler = mock_scheduler()
    mock_executor(scheduler)

    assert get_gcp_client.call_count == 1


@task
def task1(x: int) -> int:
    return x


from redun.file import get_filesystem_class as get_filesystem_class_original
from redun.file import get_proto
from redun.tests.utils import GSFileSystemMock


def get_filesystem_class(
    proto: Optional[str] = None, url: Optional[str] = None
) -> Type["FileSystem"]:
    """
    Returns the corresponding FileSystem class for a given url or protocol.
    """
    if not proto:
        assert url, "Must give url or proto as argument."
        proto = get_proto(url)
    if proto == "gs":
        fs = GSFileSystemMock
        return fs
    else:
        return get_filesystem_class_original(proto, url)


@mock_s3
@patch("redun.file.get_filesystem_class", get_filesystem_class)
def test1():
    from redun.file import File

    client = boto3.client("s3", region_name="us-east-1")
    client.create_bucket(Bucket="example-bucket")

    file = File("s3://example-bucket/a")
    file.write("hello")

    file = File("gs://example-bucket/b")
    file.write("hello")


@use_tempdir
@mock_s3
@patch("redun.file.get_filesystem_class", get_filesystem_class)
@patch("redun.executors.gcp_utils.get_gcp_client")
@patch("redun.executors.gcp_utils.get_task")
@patch("redun.executors.gcp_utils.list_jobs")
@patch("redun.executors.gcp_utils.batch_submit")
@patch.object(Scheduler, "done_job")
@patch.object(Scheduler, "reject_job")
def test_executor(
    reject_job_mock: Mock,
    done_job_mock: Mock,
    batch_submit_mock: Mock,
    list_jobs_mock: Mock,
    get_task_mock: Mock,
    get_gcp_client_mock: Mock,
    scheduler: Scheduler,
) -> None:
    """
    GCPBatchExecutor should run jobs.

    In this test, we patch `threading.Thread` to prevent the monitor thread
    from starting and we call the monitor code ourselves. This removes the
    need to manage multiple threads during the test.
    """
    client = boto3.client("s3", region_name="us-east-1")
    client.create_bucket(Bucket="example-bucket")

    # Setup executor.
    config = Config(
        {
            "gcp_batch": {
                "gcs_scratch": GCS_SCRATCH_PREFIX,
                "project": PROJECT,
                "region": PROJECT,
                "image": IMAGE,
                "job_monitor_interval": 0.05,
                "job_stale_time": 0.01,
            }
        }
    )

    executor = GCPBatchExecutor("batch", scheduler, config["gcp_batch"])
    executor.start()

    def executor_start():
        executor.is_running = True
        executor._thread = Mock()

    executor._start = executor_start

    # Prepare API mocks for job submission.
    batch_job_id = "123"
    batch_job = batch_v1.Job(task_groups=[batch_v1.TaskGroup(name=batch_job_id)])
    batch_submit_mock.return_value = batch_job
    list_jobs_mock.return_value = []
    get_task_mock.return_value = batch_v1.Task(
        name=f"{batch_job_id}/tasks/0",
        status=batch_v1.TaskStatus(state=batch_v1.TaskStatus.State.SUCCEEDED),
    )

    # Create and submit a job.
    expr = cast(TaskExpression[int], task1(10))
    job = Job(task1, expr)
    job.eval_hash = "eval_hash"
    job.args = ((10,), {})
    executor.submit(job)

    # Let job arrayer submit job.
    wait_until(lambda: executor.arrayer.num_pending == 0)

    # Ensure job options were passed correctly to docker.
    scratch_dir = executor.gcs_scratch_prefix
    assert executor.code_file

    assert batch_submit_mock.call_args
    args = batch_submit_mock.call_args[1]
    args.pop("client")
    assert batch_submit_mock.call_args[1] == {
        "commands": [
            "redun",
            "--check-version",
            ">=0.4.1",
            "oneshot",
            "redun.tests.test_gcp_batch",
            "--code",
            executor.code_file.path,
            "--input",
            "gs://example-bucket/redun/jobs/eval_hash/input",
            "--output",
            "gs://example-bucket/redun/jobs/eval_hash/output",
            "--error",
            "gs://example-bucket/redun/jobs/eval_hash/error",
            "task1",
        ],
        "gcs_scratch_prefix": "gs://example-bucket/redun",
        "image": "gcr.io/gcp-project/redun",
        "job_name": f"redun-{job.id}",
        "labels": {"redun_hash": "eval_hash", "redun_job_type": "container"},
        "machine_type": "e2-standard-4",
        "memory": 16,
        "priority": 30,
        "project": "project",
        "region": "project",
        "retries": 2,
        "service_account_email": "",
        "task_count": 1,
        "vcpus": 2,
    }

    # Simulate output file created by job.
    output_file = File(f"{scratch_dir}/jobs/eval_hash/output")
    output_file.write(pickle_dumps(task1.func(10)), mode="wb")

    # Manually run monitor logic.
    executor._monitor()

    # Ensure job returns result to scheduler.
    scheduler.done_job.assert_called_with(job, 10)  # type: ignore

    # Prepare API mocks for job submission.
    batch_job_id = "456"
    batch_job = batch_v1.Job(task_groups=[batch_v1.TaskGroup(name=batch_job_id)])
    batch_submit_mock.return_value = batch_job
    list_jobs_mock.return_value = []
    get_task_mock.return_value = batch_v1.Task(
        name=f"{batch_job_id}/tasks/0",
        status=batch_v1.TaskStatus(state=batch_v1.TaskStatus.State.FAILED),
    )

    # Create and submit a job.
    expr = cast(TaskExpression[int], task1(11))
    job = Job(task1, expr)
    job.eval_hash = "eval_hash2"
    job.args = ((11,), {})
    executor.submit(job)

    # Let job arrayer submit job.
    wait_until(lambda: executor.arrayer.num_pending == 0)

    # Simulate the container job failing.
    error = ValueError("Boom")
    error_traceback = Traceback.from_error(error)
    error_file = File(f"{scratch_dir}/jobs/eval_hash2/error")
    error_file.write(pickle_dumps((error, error_traceback)), mode="wb")

    # Manually run monitor thread.
    executor._monitor()

    # Ensure job rejected to scheduler.
    scheduler.reject_job.call_args[:2] == (job, error)  # type: ignore
