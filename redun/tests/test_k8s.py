import pickle
from unittest.mock import Mock, patch
import boto3
from kubernetes import client
from moto import mock_logs, mock_s3
from redun import File, task
from redun.config import Config
from redun.tests.utils import mock_scheduler
from redun.executors.k8s import (
    K8SExecutor,
    DockerResult,
    k8s_submit,
    get_k8s_job_name,
    get_hash_from_job_name,
    iter_k8s_job_log_lines,
    iter_k8s_job_logs,
    parse_task_error,
    submit_task,
)
from redun.scheduler import Execution, Job, Scheduler, Traceback
from redun.tests.utils import mock_scheduler, use_tempdir, wait_until
from redun.utils import pickle_dumps

def mock_executor(scheduler, debug=False, code_package=False):
    """
    Returns an AWSBatchExecutor with AWS API mocks.
    """
    image = "my-image"
    queue = "queue"
    s3_scratch_prefix = "s3://example-bucket/redun/"

    # Setup executor.
    config = Config(
        {
            "k8s": {
                "image": image,
                "queue": queue,
                "s3_scratch": s3_scratch_prefix,
                "job_monitor_interval": 0.05,
                "job_stale_time": 0.01,
                "code_package": code_package,
                "debug": debug,
            }
        }
    )
    executor = K8SExecutor("k8s", scheduler, config["k8s"])

    executor.get_jobs = Mock()
    class GetJobResponse:
        def __init__(self):
            self.items = []
    executor.get_jobs.return_value = GetJobResponse()

    executor.get_array_child_jobs = Mock()
    executor.get_array_child_jobs.return_value = []

    s3_client = boto3.client("s3", region_name="us-east-1")
    s3_client.create_bucket(Bucket="example-bucket")

    return executor


@task()
def task1(x):
    return x + 10

@mock_s3
@patch("redun.executors.k8s.parse_task_logs")
@patch("redun.executors.k8s.iter_k8s_job_status")
@patch("redun.executors.k8s.k8s_submit")
def test_executor(
    k8s_submit_mock, iter_k8s_job_status_mock, parse_task_logs_mock
) -> None:
    """
    Ensure that we can submit job to AWSBatchExecutor.
    """
    batch_job_id = "batch-job-id"
    batch_job2_id = "batch-job2-id"

    # Setup K8S mocks.
    iter_k8s_job_status_mock.return_value = iter([])
    parse_task_logs_mock.return_value = []

    scheduler = mock_scheduler()
    executor = mock_executor(scheduler)
    executor.start()

    # needs to have a .metadata.uid

    md = client.V1ObjectMeta()
    md.uid = batch_job_id
    md.name = "DoNotUse"
    k8s_job = client.V1Job(
        api_version="batch/v1",
        metadata = md,
        kind="Job")
    k8s_submit_mock.return_value = k8s_job


    # Submit redun job that will succeed.
    expr = task1(10)
    job = Job(expr)
    job.task = task1
    job.eval_hash = "eval_hash"
    executor.submit(job, [10], {})

    # Let job get stale so job arrayer actually submits it.
    wait_until(lambda: executor.arrayer.num_pending == 0)

    # # Ensure job options were passed correctly.
    assert k8s_submit_mock.call_args
    assert k8s_submit_mock.call_args[1] == {
        "image": "my-image",
        "job_name": "redun-job-eval_hash",
        "job_def_suffix": "-redun-jd",
        "array_size": 0,
        "vcpus": 1,
        "gpus": 0,
        "memory": 4,
        "role": None,
        "retries": 1,
    }

    k8s_job = client.V1Job(
        api_version="batch/v1",
        kind="Job")
    k8s_submit_mock_return_value = k8s_job

    # Submit redun job that will fail.
    expr2 = task1.options(memory=8)("a")
    job2 = Job(expr2)
    job2.task = task1
    job2.eval_hash = "eval_hash2"
    executor.submit(job2, ["a"], {})

    # Let job get stale so job arrayer actually submits it.
    wait_until(lambda: executor.arrayer.num_pending == 0)

    # # Ensure job options were passed correctly.
    assert k8s_submit_mock.call_args[1] == {
        "image": "my-image",
        "job_name": "redun-job-eval_hash2",
        "job_def_suffix": "-redun-jd",
        "array_size": 0,
        "vcpus": 1,
        "gpus": 0,
        "memory": 8,
        "role": None,
        "retries": 1,
    }

    # Simulate AWS Batch completing job.
    output_file = File("s3://example-bucket/redun/jobs/eval_hash/output")
    output_file.write(pickle_dumps(task1.func(10)), mode="wb")

    # Simulate AWS Batch failing.
    error = ValueError("Boom")
    error_file = File("s3://example-bucket/redun/jobs/eval_hash2/error")
    error_file.write(pickle_dumps((error, Traceback.from_error(error))), mode="wb")

    iter_k8s_job_status_mock.return_value = iter(
        [
            {"jobId": batch_job_id, "status": "SUCCEEDED", "container": {"logStreamName": "log1"}},
            {"jobId": batch_job2_id, "status": "FAILED", "container": {"logStreamName": "log2"}},
        ]
    )

    scheduler.batch_wait([job.id, job2.id])
    executor.stop()

    # Job results and errors should be sent back to scheduler.
    assert scheduler.job_results[job.id] == 20
    assert isinstance(scheduler.job_errors[job2.id], ValueError)

    # # Assert job tags.
    # job.job_tags == [("aws_batch_job", "batch-job-id"), ("aws_log_stream", "log1")]
    # job.job_tags == [("aws_batch_job", "batch-job2-id"), ("aws_log_stream", "log2")]


if __name__ == '__main__':
    test_executor()