from typing import cast
from unittest.mock import Mock, patch

import boto3
from google.cloud import batch_v1, compute_v1
from google.protobuf.json_format import MessageToDict  # type: ignore

from redun import File, task
from redun.config import Config
from redun.executors.gcp_batch import GCPBatchExecutor
from redun.expression import TaskExpression
from redun.scheduler import Job, Traceback
from redun.scripting import script, script_task
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


def mock_executor(scheduler, debug=False, code_package=True) -> GCPBatchExecutor:
    """
    Returns an GCPBatchExecutor with GCP API mocks.
    """
    # We use a mocked s3 backend to simulate gcs.
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
                "min_array_size": 2,
            }
        }
    )

    executor = GCPBatchExecutor("batch", scheduler, config["gcp_batch"])
    executor.start()

    def executor_start():
        executor.is_running = True
        # Prevent monitor thread from running.
        executor._thread = Mock()

    executor._start = executor_start  # type: ignore

    return executor


@task
def task1(x: int) -> int:
    return x


@use_tempdir
@mock_s3
@patch("redun.file.get_filesystem_class", get_filesystem_class_mock)
@patch("redun.executors.gcp_utils.get_gcp_compute_client")
@patch("redun.executors.gcp_utils.get_compute_machine_type")
@patch("redun.executors.gcp_utils.get_gcp_batch_client")
@patch("redun.executors.gcp_utils.list_jobs")
@patch("redun.executors.gcp_utils.get_task")
def test_executor(
    get_task_mock: Mock,
    list_jobs_mock: Mock,
    get_gcp_batch_client_mock: Mock,
    get_compute_machine_type_mock: Mock,
    get_gcp_compute_client_mock: Mock,
) -> None:
    """
    GCPBatchExecutor should run jobs.
    """
    scheduler = mock_scheduler()
    executor = mock_executor(scheduler)
    client = get_gcp_batch_client_mock()

    # Prepare API mocks for job submission.
    batch_job_id = "123"
    batch_job = batch_v1.Job(task_groups=[batch_v1.TaskGroup(name=batch_job_id)])
    client.create_job.return_value = batch_job
    list_jobs_mock.return_value = []
    get_task_mock.return_value = batch_v1.Task(
        name=f"{batch_job_id}/tasks/0",
        status=batch_v1.TaskStatus(state=batch_v1.TaskStatus.State.SUCCEEDED),
    )
    get_compute_machine_type_mock.return_value = compute_v1.types.MachineType(
        memory_mb=16384,
        guest_cpus=2,
    )

    # Create and submit a job.
    expr = cast(TaskExpression[int], task1(10))
    job = Job(task1, expr)
    job.eval_hash = "eval_hash"
    job.args = ((10,), {})
    executor.submit(job)

    # Let job arrayer submit job.
    wait_until(lambda: executor.arrayer.num_pending == 0)

    # Ensure job was submitted correctly to Google Batch.
    scratch_dir = executor.gcs_scratch_prefix
    assert executor.code_file
    assert MessageToDict(client.create_job.call_args[0][0]._pb) == {
        "job": {
            "allocationPolicy": {"instances": [{"policy": {"machineType": "e2-standard-4"}}]},
            "labels": {"redun_hash": "eval_hash", "redun_job_type": "container"},
            "logsPolicy": {"destination": "CLOUD_LOGGING"},
            "priority": "30",
            "taskGroups": [
                {
                    "taskCount": "1",
                    "taskSpec": {
                        "computeResource": {"cpuMilli": "2000", "memoryMib": "16384"},
                        "maxRetryCount": 2,
                        "runnables": [
                            {
                                "container": {
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
                                    "imageUri": "gcr.io/gcp-project/redun",
                                }
                            }
                        ],
                    },
                }
            ],
        },
        "jobId": f"redun-{job.id}",
        "parent": "projects/project/locations/project",
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
    client.create_job.return_value = batch_job
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

    # Simulate the job failing.
    error = ValueError("Boom")
    error_traceback = Traceback.from_error(error)
    error_file = File(f"{scratch_dir}/jobs/eval_hash2/error")
    error_file.write(pickle_dumps((error, error_traceback)), mode="wb")

    # Manually run monitor thread.
    executor._monitor()

    # Ensure job rejected to scheduler.
    assert repr(scheduler.job_errors[job.id]) == "ValueError('Boom')"


@use_tempdir
@mock_s3
@patch("redun.file.get_filesystem_class", get_filesystem_class_mock)
@patch("redun.executors.gcp_utils.get_gcp_compute_client")
@patch("redun.executors.gcp_utils.get_compute_machine_type")
@patch("redun.executors.gcp_utils.get_gcp_batch_client")
@patch("redun.executors.gcp_utils.list_jobs")
@patch("redun.executors.gcp_utils.get_task")
def test_executor_array(
    get_task_mock: Mock,
    list_jobs_mock: Mock,
    get_gcp_batch_client_mock: Mock,
    get_compute_machine_type_mock: Mock,
    get_gcp_compute_client_mock: Mock,
) -> None:
    """
    GCPBatchExecutor should be able to submit array jobs.
    """
    scheduler = mock_scheduler()
    executor = mock_executor(scheduler)
    client = get_gcp_batch_client_mock()

    # Suppress inflight jobs check.
    list_jobs_mock.return_value = []

    # Prepare API mocks for job submission.
    client.create_job.return_value = batch_v1.Job(
        task_groups=[
            batch_v1.TaskGroup(
                name="123",
                task_count=2,
            )
        ]
    )

    def get_task(client, task_name):
        if task_name.endswith("0"):
            state = batch_v1.TaskStatus.State.SUCCEEDED
        else:
            state = batch_v1.TaskStatus.State.FAILED

        return batch_v1.Task(
            name=task_name,
            status=batch_v1.TaskStatus(state=state),
        )

    get_task_mock.side_effect = get_task
    get_compute_machine_type_mock.return_value = compute_v1.types.MachineType(
        memory_mb=16384,
        guest_cpus=2,
    )

    # Submit two jobs in order to trigger array submission.
    # Create and submit a job.
    expr = cast(TaskExpression[int], task1(10))
    job1 = Job(task1, expr)
    job1.eval_hash = "eval_hash"
    job1.args = ((10,), {})
    executor.submit(job1)

    # Create and submit a job.
    expr = cast(TaskExpression[int], task1(11))
    job2 = Job(task1, expr)
    job2.eval_hash = "eval_hash2"
    job2.args = ((11,), {})
    executor.submit(job2)

    # Let job arrayer submit job.
    wait_until(lambda: executor.arrayer.num_pending == 0)

    # Ensure job was submitted to correctly to Google Batch.
    scratch_dir = executor.gcs_scratch_prefix
    assert executor.code_file

    response = MessageToDict(client.create_job.call_args[0][0]._pb)
    array_uuid = response["jobId"].replace("redun-array-", "")
    assert response == {
        "job": {
            "allocationPolicy": {"instances": [{"policy": {"machineType": "e2-standard-4"}}]},
            "labels": {"redun_hash": array_uuid, "redun_job_type": "container"},
            "logsPolicy": {"destination": "CLOUD_LOGGING"},
            "priority": "30",
            "taskGroups": [
                {
                    "taskCount": "2",
                    "taskSpec": {
                        "computeResource": {"cpuMilli": "2000", "memoryMib": "16384"},
                        "maxRetryCount": 2,
                        "runnables": [
                            {
                                "container": {
                                    "commands": [
                                        "redun",
                                        "--check-version",
                                        ">=0.4.1",
                                        "oneshot",
                                        "redun.tests.test_gcp_batch",
                                        "--code",
                                        executor.code_file.path,
                                        "--array-job",
                                        "--input",
                                        f"gs://example-bucket/redun/array_jobs/{array_uuid}/input",  # noqa: E501
                                        "--output",
                                        f"gs://example-bucket/redun/array_jobs/{array_uuid}/output",  # noqa: E501
                                        "--error",
                                        f"gs://example-bucket/redun/array_jobs/{array_uuid}/error",  # noqa: E501
                                        "task1",
                                    ],
                                    "imageUri": "gcr.io/gcp-project/redun",
                                }
                            }
                        ],
                    },
                }
            ],
        },
        "jobId": f"redun-array-{array_uuid}",
        "parent": "projects/project/locations/project",
    }

    # Simulate output file created by job1.
    output_file = File(f"{scratch_dir}/jobs/eval_hash/output")
    output_file.write(pickle_dumps(task1.func(10)), mode="wb")

    # Simulate job2 failing.
    error = ValueError("Boom")
    error_traceback = Traceback.from_error(error)
    error_file = File(f"{scratch_dir}/jobs/eval_hash2/error")
    error_file.write(pickle_dumps((error, error_traceback)), mode="wb")

    # Manually run monitor logic.
    executor._monitor()

    # Ensure job returns results and errors to scheduler.
    assert scheduler.job_results[job1.id] == 10
    assert repr(scheduler.job_errors[job2.id]) == "ValueError('Boom')"


@use_tempdir
@mock_s3
@patch("redun.file.get_filesystem_class", get_filesystem_class_mock)
@patch("redun.executors.gcp_utils.get_gcp_compute_client")
@patch("redun.executors.gcp_utils.get_compute_machine_type")
@patch("redun.executors.gcp_utils.get_gcp_batch_client")
@patch("redun.executors.gcp_utils.list_jobs")
@patch("redun.executors.gcp_utils.get_task")
def test_executor_script(
    get_task_mock: Mock,
    list_jobs_mock: Mock,
    get_gcp_batch_client_mock: Mock,
    get_compute_machine_type_mock: Mock,
    get_gcp_compute_client_mock: Mock,
) -> None:
    """
    GCPBatchExecutor should run script jobs.
    """
    scheduler = mock_scheduler()
    executor = mock_executor(scheduler)
    client = get_gcp_batch_client_mock()

    # Prepare API mocks for job submission.
    batch_job_id = "123"
    batch_job = batch_v1.Job(task_groups=[batch_v1.TaskGroup(name=batch_job_id)])
    client.create_job.return_value = batch_job
    list_jobs_mock.return_value = []
    get_task_mock.return_value = batch_v1.Task(
        name=f"{batch_job_id}/tasks/0",
        status=batch_v1.TaskStatus(state=batch_v1.TaskStatus.State.SUCCEEDED),
    )
    get_compute_machine_type_mock.return_value = compute_v1.types.MachineType(
        memory_mb=16384,
        guest_cpus=2,
    )
    # Create and submit a script job.
    # Simulate the call to script_task().
    expr = script(
        "cat input > output",
        inputs=[File("gs://example-bucket/input").stage("input")],
        output=File("gs://example-bucket/output").stage("output"),
    )
    command = expr.args[0]
    expr = script_task(command)
    job = Job(script_task, expr)
    job.eval_hash = "eval_hash"
    job.args = (expr.args, expr.kwargs)
    executor.submit_script(job)

    # Let job arrayer submit job.
    wait_until(lambda: executor.arrayer.num_pending == 0)

    # Ensure job was submitted correctly to Google Batch.
    assert MessageToDict(client.create_job.call_args[0][0]._pb) == {
        "job": {
            "allocationPolicy": {"instances": [{"policy": {"machineType": "e2-standard-4"}}]},
            "labels": {"redun_hash": "eval_hash", "redun_job_type": "script"},
            "logsPolicy": {"destination": "CLOUD_LOGGING"},
            "priority": "30",
            "taskGroups": [
                {
                    "taskCount": "1",
                    "taskSpec": {
                        "computeResource": {"cpuMilli": "2000", "memoryMib": "16384"},
                        "maxRetryCount": 2,
                        "runnables": [
                            {
                                "container": {
                                    "commands": [
                                        "bash",
                                        "/mnt/disks/example-bucket/redun/jobs/eval_hash/.redun_job.sh",  # noqa: E501
                                    ],
                                    "imageUri": "gcr.io/gcp-project/redun",
                                    "volumes": [
                                        "/mnt/disks/example-bucket:/mnt/disks/example-bucket"
                                    ],
                                }
                            }
                        ],
                        "volumes": [
                            {
                                "gcs": {"remotePath": "example-bucket"},
                                "mountPath": "/mnt/disks/example-bucket",
                            }
                        ],
                    },
                }
            ],
        },
        "jobId": f"redun-{job.id}",
        "parent": "projects/project/locations/project",
    }

    # Simulate output file created by job.
    scratch_dir = executor.gcs_scratch_prefix
    output_file = File(f"{scratch_dir}/jobs/eval_hash/output")
    output_file.write("done")

    # Manually run monitor logic.
    executor._monitor()

    # Ensure job returns result to scheduler.
    assert scheduler.job_results[job.id] == [0, b"done"]

    # Prepare API mocks for job submission.
    batch_job_id = "456"
    batch_job = batch_v1.Job(task_groups=[batch_v1.TaskGroup(name=batch_job_id)])
    client.create_job.return_value = batch_job
    list_jobs_mock.return_value = []
    get_task_mock.return_value = batch_v1.Task(
        name=f"{batch_job_id}/tasks/0",
        status=batch_v1.TaskStatus(state=batch_v1.TaskStatus.State.FAILED),
    )

    # Submit a failing script task.
    job2 = Job(script_task, expr)
    job2.eval_hash = "eval_hash2"
    job2.args = (expr.args, expr.kwargs)
    executor.submit_script(job2)

    # Let job arrayer submit job.
    wait_until(lambda: executor.arrayer.num_pending == 0)

    # Simulate output file created by job.
    error_file = File(f"{scratch_dir}/jobs/eval_hash2/error")
    error_file.write("boom")

    # Manually run monitor logic.
    executor._monitor()

    # Ensure job returns result to scheduler.
    assert repr(scheduler.job_errors[job2.id]) == "ScriptError('Last line: boom')"
