from typing import cast
from unittest.mock import Mock, patch

import boto3
from google.cloud import batch_v1

from redun import File, task
from redun.config import Config
from redun.executors.gcp_batch import GCPBatchExecutor
from redun.expression import TaskExpression
from redun.scheduler import Job, Traceback
from redun.tests.utils import (
    get_filesystem_class_mock,
    mock_s3,
    mock_scheduler,
    use_tempdir,
    wait_until,
)
from redun.utils import pickle_dumps

PROJECT = "project"
IMAGE = "gcr.io/gcp-project/redun"
REGION = "region"
GCS_SCRATCH_PREFIX = "gs://example-bucket/redun"


@patch("redun.executors.gcp_utils.get_gcp_client", Mock())
def mock_executor(scheduler, debug=False, code_package=True) -> GCPBatchExecutor:
    """
    Returns an GCPBatchExecutor with GCP API mocks.
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
                "code_package": code_package,
                "debug": debug,
            }
        }
    )

    executor = GCPBatchExecutor("batch", scheduler, config["gcp_batch"])
    executor.start()

    def executor_start():
        executor.is_running = True
        executor._thread = Mock()

    executor._start = executor_start  # type: ignore

    return executor


@task
def task1(x: int) -> int:
    return x


@use_tempdir
@mock_s3
@patch("redun.file.get_filesystem_class", get_filesystem_class_mock)
@patch("redun.executors.gcp_utils.batch_submit")
@patch("redun.executors.gcp_utils.list_jobs")
@patch("redun.executors.gcp_utils.get_task")
def test_executor(
    get_task_mock: Mock,
    list_jobs_mock: Mock,
    batch_submit_mock: Mock,
) -> None:
    """
    GCPBatchExecutor should run jobs.
    """
    scheduler = mock_scheduler()
    executor = mock_executor(scheduler)

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
    assert scheduler.job_results[job.id] == 10

    # Prepare API mocks for job submission of a failed job.
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
    assert repr(scheduler.job_errors[job.id]) == "ValueError('Boom')"
