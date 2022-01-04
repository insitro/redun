import json
import os
import pickle
import uuid
from typing import cast
from unittest.mock import Mock, patch

import boto3
import pytest
from freezegun import freeze_time
from moto import mock_logs

import redun.executors.aws_batch
import redun.tests.utils
from redun import File, job_array, task
from redun.cli import RedunClient, import_script
from redun.config import Config
from redun.executors.aws_batch import (
    BATCH_JOB_STATUSES,
    BATCH_LOG_GROUP,
    FAILED,
    SUCCEEDED,
    AWSBatchError,
    AWSBatchExecutor,
    batch_submit,
    get_batch_job_name,
    get_hash_from_job_name,
    get_job_definition,
    iter_batch_job_log_lines,
    iter_batch_job_logs,
    make_job_def_name,
    parse_task_error,
    submit_task,
)
from redun.executors.aws_utils import (
    REDUN_REQUIRED_VERSION,
    create_tar,
    extract_tar,
    find_code_files,
    get_array_scratch_file,
    get_job_scratch_file,
    package_code,
    parse_code_package_config,
)
from redun.file import Dir
from redun.scheduler import Execution, Job, Scheduler, Traceback
from redun.scripting import ScriptError
from redun.tests.utils import mock_s3, mock_scheduler, use_tempdir, wait_until
from redun.utils import pickle_dumps


def test_make_job_def_name() -> None:
    """
    Job definition names should autogenerate from docker image names.
    """
    assert make_job_def_name("my-image") == "my-image-jd"
    assert make_job_def_name("my-image", "-job-def") == "my-image-job-def"
    assert make_job_def_name("my.image") == "myimage-jd"
    assert make_job_def_name("a" * 200) == ("a" * 125) + "-jd"


@patch("redun.executors.aws_utils.get_aws_client")
def test_get_job_definition(get_aws_client_mock) -> None:
    """
    Most recent active revision should be returned or empty dict if no matching job defs.
    """
    # Simulate case where no matching jobs are returned from batch API.
    get_aws_client_mock.return_value.describe_job_definitions.return_value = {"jobDefinitions": []}
    assert get_job_definition("JOB_DEF_1") == {}

    # Simulate case where there are multiple revisions, confirm newest is returned.
    get_aws_client_mock.return_value.describe_job_definitions.return_value = {
        "jobDefinitions": [
            {"revision": 1, "jobDefinitionArn": "ARN1"},
            {"revision": 3, "jobDefinitionArn": "ARN3"},
            {"revision": 2, "jobDefinitionArn": "ARN2"},
        ]
    }
    # Need to change the job def name here from the first case above to avoid the lru cache on
    # get_job_definition.
    job_def = get_job_definition("JOB_DEF_2")
    assert job_def["jobDefinitionArn"] == "ARN3"


@patch("redun.executors.aws_utils.get_aws_client")
@patch("redun.executors.aws_batch.get_job_definition")
def test_required_job_def_name(get_job_definition_mock, _) -> None:
    """
    Confirm that job_def_name is required when autocreate_job is False.
    """
    # A job_def_name is required when autocreate is False.
    with pytest.raises(AssertionError):
        batch_submit(["command"], "queue", "image", autocreate_job=False)

    # When the required job_def_name is supplied, an error should be raised if a matching
    # definition cannot be found.
    get_job_definition_mock.return_value = {}
    with pytest.raises(ValueError):
        batch_submit(
            ["command"], "queue", "image", job_def_name="NONEXISTENT", autocreate_job=False
        )


@pytest.mark.parametrize("array,suffix", [(False, ""), (True, "-array")])
def test_get_hash_from_job_name(array, suffix) -> None:
    """
    Returns the Job hash from a AWS Batch job name.
    """
    prefix = "my-job-prefix"
    job_hash = "c000d7f9b6275c58aff9d5466f6a1174e99195ca"
    job_name = get_batch_job_name(prefix, job_hash, array=array)
    assert job_name.startswith(prefix)
    assert job_name.endswith(suffix)

    job_hash2 = get_hash_from_job_name(job_name)
    assert job_hash2 == job_hash


def test_batch_tags(scheduler: Scheduler) -> None:
    """
    Executor should be able to determine batch tags for a batch job.
    """
    # Setup executor.
    config = Config(
        {
            "batch": {
                "image": "image",
                "queue": "queue",
                "s3_scratch": "s3_scratch_prefix",
                "batch_tags": '{"team": "team1", "foo": "bar"}',
            }
        }
    )
    executor = AWSBatchExecutor("batch", scheduler, config["batch"])
    executor._aws_user = "alice"

    @task(batch_tags={"step": "final", "project": "acme"}, namespace="test")
    def task1(x):
        return x

    exec1 = Execution("123")
    job = Job(task1(10), execution=exec1)
    job.task = task1

    batch_tags = executor._get_job_options(job)["batch_tags"]
    assert batch_tags == {
        "redun_aws_user": "alice",
        "redun_execution_id": "123",
        "redun_job_id": job.id,
        "redun_project": "test",
        "redun_task_name": "test.task1",
        "step": "final",
        "team": "team1",
        "project": "acme",
        "foo": "bar",
    }


def test_batch_tags_no_default(scheduler: Scheduler) -> None:
    """
    Executor config should be able to turn off default batch tags.
    """
    # Setup executor.
    config = Config(
        {
            "batch": {
                "image": "image",
                "queue": "queue",
                "s3_scratch": "s3_scratch_prefix",
                "default_batch_tags": "false",
            }
        }
    )
    executor = AWSBatchExecutor("batch", scheduler, config["batch"])

    @task(batch_tags={"step": "final", "project": "acme"}, namespace="test")
    def task1(x):
        return x

    exec1 = Execution("123")
    job = Job(task1(10), execution=exec1)
    job.task = task1

    batch_tags = executor._get_job_options(job)["batch_tags"]
    assert batch_tags == {
        "step": "final",
        "project": "acme",
    }


def test_executor_config(scheduler: Scheduler) -> None:
    """
    Executor should be able to parse its config.
    """
    # Setup executor.
    config = Config(
        {
            "batch": {
                "image": "image",
                "queue": "queue",
                "s3_scratch": "s3_scratch_prefix",
                "code_includes": "*.txt",
            }
        }
    )
    executor = AWSBatchExecutor("batch", scheduler, config["batch"])

    assert executor.image == "image"
    assert executor.queue == "queue"
    assert executor.s3_scratch_prefix == "s3_scratch_prefix"
    assert isinstance(executor.code_package, dict)
    assert executor.code_package["includes"] == ["*.txt"]
    assert executor.debug is False


@task()
def task1(x):
    return x + 10


@task(load_module="custom.module")
def task1_custom_module(x):
    return x + 10


@use_tempdir
@mock_s3
@patch("redun.executors.aws_batch.batch_submit")
@pytest.mark.parametrize(
    "custom_module, expected_load_module, a_task",
    [
        (None, "redun.tests.test_aws_batch", task1),
        ("custom.module", "custom.module", task1_custom_module),
    ],
)
def test_submit_task(batch_submit_mock, custom_module, expected_load_module, a_task):
    job_id = "123"
    image = "my-image"
    queue = "queue"
    s3_scratch_prefix = "s3://example-bucket/redun/"

    client = boto3.client("s3", region_name="us-east-1")
    client.create_bucket(Bucket="example-bucket")

    redun.executors.aws_batch.batch_submit.return_value = {"jobId": "batch-job-id"}

    # Create example workflow script to be packaged.
    File("workflow.py").write(
        f"""
@task(load_module={custom_module})
def task1(x):
    return x + 10
    """
    )

    job = Job(a_task())
    job.id = job_id
    job.eval_hash = "eval_hash"
    code_file = package_code(s3_scratch_prefix)
    resp = submit_task(
        image,
        queue,
        s3_scratch_prefix,
        job,
        a_task,
        args=[10],
        kwargs={},
        code_file=code_file,
    )

    # We should get a AWS Batch job id back.
    assert resp["jobId"] == "batch-job-id"

    # Input files should be made.
    assert File("s3://example-bucket/redun/jobs/eval_hash/input").exists()
    [code_file] = list(Dir("s3://example-bucket/redun/code"))

    # We should have submitted a job to AWS Batch.
    redun.executors.aws_batch.batch_submit.assert_called_with(
        [
            "redun",
            "--check-version",
            REDUN_REQUIRED_VERSION,
            "oneshot",
            expected_load_module,
            "--code",
            code_file.path,
            "--input",
            "s3://example-bucket/redun/jobs/eval_hash/input",
            "--output",
            "s3://example-bucket/redun/jobs/eval_hash/output",
            "--error",
            "s3://example-bucket/redun/jobs/eval_hash/error",
            a_task.name,
        ],
        "queue",
        image="my-image",
        job_def_suffix="-redun-jd",
        job_name="batch-job-eval_hash",
        array_size=0,
        aws_region="us-west-2",
    )


@use_tempdir
@mock_s3
@patch("redun.executors.aws_batch.batch_submit")
def test_submit_task_deep_file(batch_submit_mock):
    """
    Executor should be able to submit a task defined in a deeply nested file path.
    """
    job_id = "123"
    image = "my-image"
    queue = "queue"
    s3_scratch_prefix = "s3://example-bucket/redun/"

    client = boto3.client("s3", region_name="us-east-1")
    client.create_bucket(Bucket="example-bucket")

    redun.executors.aws_batch.batch_submit.return_value = {"jobId": "batch-job-id"}

    # Create example workflow script to be packaged.
    File("path/to/workflow.py").write(
        """
from redun import task

@task()
def task1(x):
    return x + 10
    """
    )

    module = import_script("path/to/workflow.py")

    job = Job(module.task1())
    job.id = job_id
    job.eval_hash = "eval_hash"
    code_file = package_code(s3_scratch_prefix)
    resp = submit_task(
        image,
        queue,
        s3_scratch_prefix,
        job,
        module.task1,
        args=[10],
        kwargs={},
        code_file=code_file,
    )

    # We should get a AWS Batch job id back.
    assert resp["jobId"] == "batch-job-id"

    # Input files should be made.
    assert File("s3://example-bucket/redun/jobs/eval_hash/input").exists()
    [code_file] = list(Dir("s3://example-bucket/redun/code"))

    # We should have submitted a job to AWS Batch.
    redun.executors.aws_batch.batch_submit.assert_called_with(
        [
            "redun",
            "--check-version",
            REDUN_REQUIRED_VERSION,
            "oneshot",
            "workflow",
            "--import-path",
            "path/to",
            "--code",
            code_file.path,
            "--input",
            "s3://example-bucket/redun/jobs/eval_hash/input",
            "--output",
            "s3://example-bucket/redun/jobs/eval_hash/output",
            "--error",
            "s3://example-bucket/redun/jobs/eval_hash/error",
            "task1",
        ],
        "queue",
        image="my-image",
        job_def_suffix="-redun-jd",
        job_name="batch-job-eval_hash",
        array_size=0,
        aws_region="us-west-2",
    )


@mock_s3
def test_parse_task_error() -> None:
    """
    We should be able to parse the error of a failed task.
    """
    s3_scratch_prefix = "s3://example-bucket/redun/"

    client = boto3.client("s3", region_name="us-east-1")
    client.create_bucket(Bucket="example-bucket")

    @task()
    def task1(x):
        return x + 1

    @task(script=True)
    def task_script1():
        return "echo hello!"

    expr = task1(10)
    job = Job(expr)
    job.task = task1
    job.eval_hash = "eval_hash"

    # Normal task, no error file.
    error, error_traceback = parse_task_error(s3_scratch_prefix, job)
    assert isinstance(error, AWSBatchError)
    assert isinstance(error_traceback, Traceback)

    # Simulate AWS Batch job failing.
    error = ValueError("Boom")
    error_file = File("s3://example-bucket/redun/jobs/eval_hash/error")
    error_file.write(pickle_dumps((error, Traceback.from_error(error))), mode="wb")

    # Normal task, error file exists.
    error, error_traceback = parse_task_error(s3_scratch_prefix, job)
    assert isinstance(error, ValueError)
    assert isinstance(error_traceback, Traceback)

    # Create a script task and job.
    expr2 = task_script1()
    job2 = Job(expr2)
    job2.task = task_script1
    job2.eval_hash = "eval_hash2"

    # Script task without an error file should retutn a generic error.
    error, error_traceback = parse_task_error(s3_scratch_prefix, job2)
    assert isinstance(error, AWSBatchError)
    assert isinstance(error_traceback, Traceback)

    # Create error file for script task.
    error_file2 = File("s3://example-bucket/redun/jobs/eval_hash2/error")
    error_file2.write("Boom")

    # Script task with an error file should return a specific error.
    error, error_traceback = parse_task_error(s3_scratch_prefix, job2)
    assert isinstance(error, ScriptError)
    assert error.message == "Boom"
    assert isinstance(error_traceback, Traceback)


@freeze_time("2020-01-01 00:00:00", tz_offset=-7)
@mock_logs
@patch("redun.executors.aws_batch.aws_describe_jobs")
def test_iter_batch_job_logs(aws_describe_jobs_mock) -> None:
    """
    We should be able to iterate through the logs of a Batch Job.
    """
    stream_name = "redun_aws_batch_example-redun-jd/default/6c939514f4054fdfb5ee65acc8aa4b07"
    aws_describe_jobs_mock.side_effect = lambda *args, **kwargs: iter(
        [
            {
                "container": {
                    "logStreamName": stream_name,
                }
            }
        ]
    )

    # Setup logs mocks.
    logs_client = boto3.client("logs", region_name="us-west-2")
    logs_client.create_log_group(logGroupName=BATCH_LOG_GROUP)
    logs_client.create_log_stream(logGroupName=BATCH_LOG_GROUP, logStreamName=stream_name)
    resp = logs_client.put_log_events(
        logGroupName=BATCH_LOG_GROUP,
        logStreamName=stream_name,
        logEvents=[
            {"timestamp": 1602596831000, "message": "A message 1."},
            {"timestamp": 1602596832000, "message": "A message 2."},
        ],
    )
    resp = logs_client.put_log_events(
        logGroupName=BATCH_LOG_GROUP,
        logStreamName=stream_name,
        logEvents=[
            {"timestamp": 1602596833000, "message": "A message 3."},
            {"timestamp": 1602596834000, "message": "A message 4."},
        ],
        sequenceToken=resp["nextSequenceToken"],
    )

    expected_events = [
        {"message": "A message 1.", "timestamp": 1602596831000},
        {"message": "A message 2.", "timestamp": 1602596832000},
        {"message": "A message 3.", "timestamp": 1602596833000},
        {"message": "A message 4.", "timestamp": 1602596834000},
    ]

    # Fetch log events.
    job_id = "123"
    events = iter_batch_job_logs(job_id, limit=1)
    event_list = [
        {"message": event["message"], "timestamp": event["timestamp"]} for event in events
    ]
    assert event_list == expected_events

    # Fetch log events in reverse.
    events = iter_batch_job_logs(job_id, limit=1, reverse=True)
    event_list = [
        {"message": event["message"], "timestamp": event["timestamp"]} for event in events
    ]
    assert event_list == list(reversed(expected_events))

    # Fetch log events in reverse with larger page size.
    events = iter_batch_job_logs(job_id, limit=2, reverse=True)
    event_list = [
        {"message": event["message"], "timestamp": event["timestamp"]} for event in events
    ]
    assert event_list == list(reversed(expected_events))

    # Fetch log lines.
    lines = list(iter_batch_job_log_lines(job_id))
    assert lines == [
        "2020-10-13 06:47:11  A message 1.",
        "2020-10-13 06:47:12  A message 2.",
        "2020-10-13 06:47:13  A message 3.",
        "2020-10-13 06:47:14  A message 4.",
    ]

    # Fetch logs from unknown job.
    aws_describe_jobs_mock.side_effect = lambda *args, **kwargs: iter([])
    assert list(iter_batch_job_logs("unknown_job_id")) == []

    # Fetch logs from job with missing logs.
    aws_describe_jobs_mock.side_effect = lambda *args, **kwargs: iter(
        [
            {
                "container": {
                    "logStreamName": "bad_logs",
                }
            }
        ]
    )
    assert list(iter_batch_job_logs(job_id, required=False)) == []

    with pytest.raises(Exception):
        list(iter_batch_job_logs(job_id, required=True))

    # Fetch logs from job with no logs at all.
    aws_describe_jobs_mock.side_effect = lambda *args, **kwargs: iter([{"container": {}}])
    assert list(iter_batch_job_logs(job_id, required=False)) == []


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
            "batch": {
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
    executor = AWSBatchExecutor("batch", scheduler, config["batch"])

    executor.get_jobs = Mock()
    executor.get_jobs.return_value = []

    executor.get_array_child_jobs = Mock()
    executor.get_array_child_jobs.return_value = []

    s3_client = boto3.client("s3", region_name="us-east-1")
    s3_client.create_bucket(Bucket="example-bucket")

    return executor


@mock_s3
@patch("redun.executors.aws_utils.get_aws_user", return_value="alice")
@patch("redun.executors.aws_batch.parse_task_logs")
@patch("redun.executors.aws_batch.iter_batch_job_status")
@patch("redun.executors.aws_batch.batch_submit")
def test_executor(
    batch_submit_mock, iter_batch_job_status_mock, parse_task_logs_mock, get_aws_user_mock
) -> None:
    """
    Ensure that we can submit job to AWSBatchExecutor.
    """
    batch_job_id = "batch-job-id"
    batch_job2_id = "batch-job2-id"

    # Setup AWS Batch mocks.
    iter_batch_job_status_mock.return_value = iter([])
    parse_task_logs_mock.return_value = []

    scheduler = mock_scheduler()
    executor = mock_executor(scheduler)
    executor.start()

    batch_submit_mock.return_value = {
        "jobId": batch_job_id,
    }

    # Submit redun job that will succeed.
    expr = task1(10)
    job = Job(expr)
    job.task = task1
    job.eval_hash = "eval_hash"
    executor.submit(job, [10], {})

    # Let job get stale so job arrayer actually submits it.
    wait_until(lambda: executor.arrayer.num_pending == 0)

    # Ensure job options were passed correctly.
    assert batch_submit_mock.call_args
    assert batch_submit_mock.call_args[1] == {
        "image": "my-image",
        "job_name": "redun-job-eval_hash",
        "job_def_suffix": "-redun-jd",
        "array_size": 0,
        "vcpus": 1,
        "gpus": 0,
        "memory": 4,
        "role": None,
        "retries": 1,
        "aws_region": "us-west-2",
        "batch_tags": {
            "redun_aws_user": "alice",
            "redun_execution_id": "",
            "redun_job_id": job.id,
            "redun_project": "",
            "redun_task_name": "task1",
        },
    }

    batch_submit_mock.return_value = {
        "jobId": batch_job2_id,
    }

    # Submit redun job that will fail.
    expr2 = task1.options(memory=8)("a")
    job2 = Job(expr2)
    job2.task = task1
    job2.eval_hash = "eval_hash2"
    executor.submit(job2, ["a"], {})

    # Let job get stale so job arrayer actually submits it.
    wait_until(lambda: executor.arrayer.num_pending == 0)

    # Ensure job options were passed correctly.
    assert batch_submit_mock.call_args[1] == {
        "image": "my-image",
        "job_name": "redun-job-eval_hash2",
        "job_def_suffix": "-redun-jd",
        "array_size": 0,
        "vcpus": 1,
        "gpus": 0,
        "memory": 8,
        "role": None,
        "retries": 1,
        "aws_region": "us-west-2",
        "batch_tags": {
            "redun_aws_user": "alice",
            "redun_execution_id": "",
            "redun_job_id": job2.id,
            "redun_project": "",
            "redun_task_name": "task1",
        },
    }

    # Simulate AWS Batch completing job.
    output_file = File("s3://example-bucket/redun/jobs/eval_hash/output")
    output_file.write(pickle_dumps(task1.func(10)), mode="wb")

    # Simulate AWS Batch failing.
    error = ValueError("Boom")
    error_file = File("s3://example-bucket/redun/jobs/eval_hash2/error")
    error_file.write(pickle_dumps((error, Traceback.from_error(error))), mode="wb")

    iter_batch_job_status_mock.return_value = iter(
        [
            {"jobId": batch_job_id, "status": SUCCEEDED, "container": {"logStreamName": "log1"}},
            {"jobId": batch_job2_id, "status": FAILED, "container": {"logStreamName": "log2"}},
        ]
    )

    scheduler.batch_wait([job.id, job2.id])
    executor.stop()

    # Job results and errors should be sent back to scheduler.
    assert scheduler.job_results[job.id] == 20
    assert isinstance(scheduler.job_errors[job2.id], ValueError)

    # Assert job tags.
    job.job_tags == [("aws_batch_job", "batch-job-id"), ("aws_log_stream", "log1")]
    job.job_tags == [("aws_batch_job", "batch-job2-id"), ("aws_log_stream", "log2")]


@mock_s3
@patch("redun.executors.aws_utils.get_aws_user", return_value="alice")
@patch("redun.executors.aws_batch.parse_task_logs")
@patch("redun.executors.aws_batch.iter_local_job_status")
@patch("redun.executors.aws_batch.run_docker")
def test_executor_docker(
    run_docker_mock,
    iter_local_job_status_mock,
    parse_task_logs_mock,
    get_aws_user_mock,
) -> None:
    """
    Ensure that we can submit job to AWSBatchExecutor with debug=True.
    """
    batch_job_id = "batch-job-id"
    batch_job2_id = "batch-job2-id"

    # Setup Docker mocks.
    iter_local_job_status_mock.return_value = iter([])
    parse_task_logs_mock.return_value = []

    # Setup redun mocks.
    scheduler = mock_scheduler()
    executor = mock_executor(scheduler, debug=True)
    executor.start()

    run_docker_mock.return_value = batch_job_id

    # Submit redun job that will succeed.
    expr = task1(10)
    job = Job(expr)
    job.task = task1
    job.eval_hash = "eval_hash"
    executor.submit(job, [10], {})

    # Let job get stale so job arrayer actually submits it.
    wait_until(lambda: executor.arrayer.num_pending == 0)

    # Ensure job options were passed correctly.
    assert run_docker_mock.call_args[1] == {
        "image": "my-image",
    }

    run_docker_mock.reset_mock()
    run_docker_mock.return_value = batch_job2_id

    # Hand create Job and submit.
    expr2 = task1("a")
    job2 = Job(expr2)
    job2.task = task1
    job2.eval_hash = "eval_hash2"
    executor.submit(job2, ["a"], {})

    # Let job get stale so job arrayer actually submits it.
    wait_until(lambda: executor.arrayer.num_pending == 0)

    # Ensure job options were passed correctly.
    assert run_docker_mock.call_args[1] == {
        "image": "my-image",
    }

    # Simulate output file created by job.
    output_file = File("s3://example-bucket/redun/jobs/eval_hash/output")
    output_file.write(pickle_dumps(task1.func(10)), mode="wb")

    # Simulate AWS Batch failing.
    error = ValueError("Boom")
    error_file = File("s3://example-bucket/redun/jobs/eval_hash2/error")
    error_file.write(pickle_dumps((error, Traceback.from_error(error))), mode="wb")

    iter_local_job_status_mock.return_value = iter(
        [
            {"jobId": batch_job_id, "status": SUCCEEDED, "logs": ""},
            {"jobId": batch_job2_id, "status": FAILED, "logs": ""},
        ]
    )

    scheduler.batch_wait([job.id, job2.id])
    executor.stop()

    # Job results and errors should be sent back to scheduler.
    assert scheduler.job_results[job.id] == 20
    assert isinstance(scheduler.job_errors[job2.id], ValueError)


@mock_s3
@patch("redun.executors.aws_utils.get_aws_user", return_value="alice")
@patch("redun.executors.aws_batch.parse_task_logs")
@patch("redun.executors.aws_batch.iter_batch_job_status")
@patch("redun.executors.aws_batch.batch_submit")
def test_executor_error_override(
    batch_submit_mock, iter_batch_job_status_mock, parse_task_logs_mock, get_aws_user_mock
) -> None:
    """
    Some AWS Batch errors should be overridden.
    """

    @task()
    def task1(x):
        return x + 10

    @task(script=True)
    def task_script1(x):
        return "ls"

    batch_job_id = "batch-job-id"
    batch_job_script_id = "batch-job-script-id"

    # Setup AWS Batch mocks.
    iter_batch_job_status_mock.return_value = iter([])
    parse_task_logs_mock.return_value = []

    scheduler = mock_scheduler()
    executor = mock_executor(scheduler)
    executor.start()

    batch_submit_mock.return_value = {
        "jobId": batch_job_id,
    }

    # Submit redun job that will succeed at the redun-level.
    expr = task1.options(memory=8)("a")
    job = Job(expr)
    job.task = task1
    job.eval_hash = "eval_hash"
    executor.submit(job, ["a"], {})

    # Let job get stale so job arrayer actually submits it.
    wait_until(lambda: executor.arrayer.num_pending == 0)

    batch_submit_mock.return_value = {
        "jobId": batch_job_script_id,
    }

    # Submit redun job that will succeed at the redun-level.
    expr_script = task_script1.options(memory=8)("a")
    job_script = Job(expr_script)
    job_script.task = task_script1
    job_script.eval_hash = "eval_script_hash"
    executor.submit(job_script, ["a"], {})

    # Let job get stale so job arrayer actually submits it.
    wait_until(lambda: executor.arrayer.num_pending == 0)

    # Simulate output file created by job.
    output_file = File("s3://example-bucket/redun/jobs/eval_hash/output")
    output_file.write(pickle_dumps(task1.func(10)), mode="wb")

    # Simulate output file created by job.
    output_file = File("s3://example-bucket/redun/jobs/eval_script_hash/output")
    output_file.write(pickle_dumps("done"), mode="wb")
    File("s3://example-bucket/redun/jobs/eval_script_hash/status").write("ok")

    # But simulate AWS Batch failing.
    reason = "CannotInspectContainerError: Could not transition to inspecting."
    iter_batch_job_status_mock.return_value = iter(
        [
            {
                "jobId": batch_job_id,
                "status": FAILED,
                "attempts": [
                    {
                        "container": {
                            "reason": reason,
                        },
                    },
                ],
            },
            {
                "jobId": batch_job_script_id,
                "status": FAILED,
                "attempts": [
                    {
                        "container": {
                            "reason": reason,
                        },
                    },
                ],
            },
        ]
    )

    scheduler.batch_wait([job.id, job_script.id])
    executor.stop()

    # Despite AWS Batch error, redun job should succeed and
    # results should be sent back to scheduler.
    assert scheduler.job_results[job.id] == 20


@mock_s3
@patch("redun.executors.aws_utils.get_aws_user", return_value="alice")
@patch("redun.executors.aws_batch.iter_local_job_status")
@patch("redun.executors.aws_batch.run_docker")
def test_executor_multiple_start(
    run_docker_mock, iter_local_job_status_mock, get_aws_user_mock
) -> None:
    """
    Ensure that we can start executor multiple times.
    """
    # Setup Docker mocks.
    iter_local_job_status_mock.return_value = iter([])

    # Setup redun mocks.
    scheduler = mock_scheduler()
    executor = mock_executor(scheduler, debug=True)
    executor.start()
    executor._start()
    executor._start()
    executor.stop()


@mock_s3
@patch("redun.executors.aws_utils.get_aws_user", return_value="alice")
@patch("redun.executors.aws_batch.iter_local_job_status")
@patch("redun.executors.aws_batch.run_docker")
def test_interactive(run_docker_mock, iter_local_job_status_mock, get_aws_user_mock) -> None:
    """
    The interactive task option should be passed to run_docker.
    """
    # Setup Docker mocks.
    iter_local_job_status_mock.return_value = iter([])
    run_docker_mock.return_value = "batch-job-id"

    # Setup redun mocks.
    scheduler = mock_scheduler()
    executor = mock_executor(scheduler, debug=True)
    executor.start()

    # Hand create Job and submit.
    expr = task1.options(interactive=True)(10)
    job = Job(expr)
    job.task = task1
    job.eval_hash = "eval_hash"
    executor.submit(job, [10], {})

    # Let job get stale so job arrayer actually submits it
    wait_until(lambda: executor.arrayer.num_pending == 0)

    # Ensure job options were passed correctly.
    assert run_docker_mock.call_args[1] == {
        "image": "my-image",
        "interactive": True,
    }

    # Cleanly stop executor.
    executor.stop()


@mock_s3
def test_executor_handles_unrelated_jobs() -> None:
    """
    Regression test for https://insitro.atlassian.net/browse/DE-2632

    There is an expanding pattern of using a "headnode" running in batch to trigger redun
    pipelines. If the headnode and redun jobs that it spawns have a shared job_name_prefix then
    the headnode job can get gathered in the `get_jobs` call and we will try to extract the hash.
    However, since the headnode job is not a redun job, it will not have a hash and previously
    caused execution failures.

    This test confirms that jobs without hashes in their names are ignored which allows headnode
    jobs(triggered via lambda or otherwise) to share job name prefixes with the redun jobs that
    they spawn.
    """
    scheduler = mock_scheduler()
    executor = mock_executor(scheduler)

    prefix = "liveratlas_spearmancor"
    hash1 = "123456789"
    hash2 = "987654321"

    # Set up mocks to include a headnode job(no hash) and some redun jobs that it "spawned".
    executor.get_jobs.return_value = [
        # The headnode job. Note the job name has not hash in it as the hash appears after the "-"
        # in a redun job name.
        {"jobId": "headnode", "jobName": f"{prefix}_automation_headnode"},
        # Redun jobs that were triggered by the "redun run" in the headnode.
        {"jobId": "preprocess", "jobName": f"{prefix}_preprocess-{hash1}"},
        {"jobId": "decode", "jobName": f"{prefix}_decode-{hash2}"},
    ]

    executor.gather_inflight_jobs()

    assert executor.preexisting_batch_jobs == {
        hash1: "preprocess",
        hash2: "decode",
    }


@mock_s3
def test_executor_inflight_array_job() -> None:
    """
    Ensure we reunite with an inflight array job
    """
    scheduler = mock_scheduler()
    executor = mock_executor(scheduler)

    # Set up mocks to indicate an array job is inflight.
    array_uuid = str(uuid.uuid4()).replace("-", "")
    executor.get_jobs.return_value = [
        {"jobId": "carrots", "jobName": f"redun-job-{array_uuid}-array"}
    ]
    executor.get_array_child_jobs.return_value = [
        {"jobId": "carrots:1", "arrayProperties": {"index": 1}},
        {"jobId": "carrots:0", "arrayProperties": {"index": 0}},
        {"jobId": "carrots:2", "arrayProperties": {"index": 2}},
    ]

    # Set up hash scratch file
    eval_file = File(f"s3://example-bucket/redun/array_jobs/{array_uuid}/eval_hashes")
    with eval_file.open("w") as eval_f:
        eval_f.write("zero\none\ntwo")

    # Force the scheduler to gather the inflight jobs. This normally happens on
    # first job submission but we want to just check that we can join here.
    executor.gather_inflight_jobs()

    # Check query for child job happened.
    assert executor.get_array_child_jobs.call_args
    assert executor.get_array_child_jobs.call_args[0] == ("carrots", BATCH_JOB_STATUSES.inflight)

    # Make sure child jobs (and not parent) ended up in pending batch jobs.
    assert executor.preexisting_batch_jobs == {
        "zero": "carrots:0",
        "one": "carrots:1",
        "two": "carrots:2",
    }


@mock_s3
@patch("redun.executors.aws_utils.get_aws_user", return_value="alice")
@patch("redun.executors.aws_utils.package_code")
def test_code_packaging(package_code_mock, get_aws_user_mock) -> None:
    """
    Ensure that code packaging only happens on first submission.
    """
    package_code_mock.return_value = "s3://fake-bucket/code.tar.gz"

    scheduler = mock_scheduler()
    executor = mock_executor(scheduler, debug=True, code_package=True)
    executor.start()

    # Starting the executor should not have triggered code packaging.
    assert executor.code_file is None
    assert package_code_mock.call_count == 0

    # Hand create jobs.
    job1 = Job(task1(10))
    job1.id = "1"
    job1.task = task1
    job1.eval_hash = "eval_hash"

    job2 = Job(task1(20))
    job2.id = "2"
    job2.task = task1
    job2.eval_hash = "eval_hash"

    # Submit a job and ensure that the code was packaged.
    executor.submit(job1, [10], {})
    assert executor.code_file == "s3://fake-bucket/code.tar.gz"
    assert package_code_mock.call_count == 1

    # Submit another job and ensure that code was not packaged again.
    executor.submit(job2, [20], {})
    assert package_code_mock.call_count == 1

    executor.stop()


@mock_s3
@patch("redun.executors.aws_utils.get_aws_user", return_value="alice")
def test_inflight_join_disabled_in_debug(get_aws_user_mock) -> None:
    """
    Ensure that debug=True disables inflight job gathering as it is unnecessary.
    """
    scheduler = mock_scheduler()
    executor = mock_executor(scheduler, debug=True)
    executor.start()

    # Hand create job.
    job = Job(task1(10))
    job.id = "123"
    job.task = task1
    job.eval_hash = "eval_hash"

    # Submit redun job.
    executor.submit(job, [10], {})

    # Ensure that inflight jobs were not gathered.
    assert executor.get_jobs.call_count == 0

    executor.stop()


@mock_s3
@patch("redun.executors.aws_utils.get_aws_user", return_value="alice")
@patch("redun.executors.aws_batch.aws_describe_jobs")
def test_inflight_join_only_on_first_submission(aws_describe_jobs_mock, get_aws_user_mock) -> None:
    """
    Ensure that inflight jobs are only gathered once and not on every job submission.
    """
    scheduler = mock_scheduler()
    executor = mock_executor(scheduler)

    executor.start()

    # Hand create jobs.
    job1 = Job(task1(10))
    job1.id = "1"
    job1.task = task1
    job1.eval_hash = "eval_hash"

    job2 = Job(task1(20))
    job2.id = "2"
    job2.task = task1
    job2.eval_hash = "eval_hash"

    # Submit redun job.
    executor.submit(job1, [10], {})

    # Ensure that inflight jobs were gathered.
    assert executor.get_jobs.call_count == 1

    # Submit the second job and confirm that job reuniting was not done again.
    executor.submit(job2, [20], {})
    assert executor.get_jobs.call_count == 1

    executor.stop()


@mock_s3
@patch("redun.executors.aws_utils.get_aws_user", return_value="alice")
@patch("redun.executors.aws_batch.aws_describe_jobs")
@patch("redun.executors.aws_batch.iter_batch_job_status")
@patch("redun.executors.aws_batch.batch_submit")
def test_executor_inflight_job(
    batch_submit_mock,
    iter_batch_job_status_mock,
    aws_describe_jobs_mock,
    get_aws_user_mock,
) -> None:
    """
    Ensure we reunite with an inflight job.
    """
    batch_job_id = "333"

    # Setup AWS Batch mocks.
    iter_batch_job_status_mock.return_value = iter([])
    aws_describe_jobs_mock.return_value = iter(
        [
            {
                "jobId": batch_job_id,
            }
        ]
    )

    scheduler = mock_scheduler()
    executor = mock_executor(scheduler)
    executor.get_jobs.return_value = [{"jobId": batch_job_id, "jobName": "redun-job-eval_hash"}]
    executor.start()

    # Hand create job.
    job = Job(task1(10))
    job.id = "123"
    job.task = task1
    job.eval_hash = "eval_hash"

    # Submit redun job.
    executor.submit(job, [10], {})

    # Ensure no batch jobs were submitted.
    assert batch_submit_mock.call_count == 0

    # Simulate AWS Batch completing with valid value.
    output_file = File("s3://example-bucket/redun/jobs/eval_hash/output")
    output_file.write(pickle_dumps(task1.func(10)), mode="wb")

    iter_batch_job_status_mock.return_value = iter([{"jobId": batch_job_id, "status": SUCCEEDED}])

    scheduler.batch_wait([job.id])

    # Simulate pre-existing job output.
    output_file = File("s3://example-bucket/redun/jobs/eval_hash/output")
    output_file.write(pickle_dumps(task1.func(10)), mode="wb")

    # Ensure redun job is completed.
    assert scheduler.job_results[job.id] == 20

    executor.stop()


@use_tempdir
def test_find_code_files():
    # Creating python files.
    File("workflow.py").write("")
    File("lib/lib.py").write("")
    File("lib/module/lib.py").write("")

    # Create unrelated files.
    File("unrelated.txt").write("")
    File("lib/unrelated.txt").write("")

    # Create python files in hidden directories.
    File(".venv/lib.py").write("")

    # Create python files we want excluded.
    File("lib2/module/lib.py").write("")

    files = find_code_files()
    assert files == {
        "./workflow.py",
        "./lib/lib.py",
        "./lib/module/lib.py",
        "./lib2/module/lib.py",
    }

    files = find_code_files(excludes=["lib2/**/**"])
    assert files == {"./workflow.py", "./lib/lib.py", "./lib/module/lib.py"}

    files = find_code_files(includes=["lib/**/**.py", "lib2/**/**.py"])
    assert files == {"./lib/lib.py", "./lib/module/lib.py", "./lib2/module/lib.py"}


@use_tempdir
def test_tar_code_files():
    # Creating python files.
    File("workflow.py").write("")
    File("lib/lib.py").write("")
    File("lib/module/lib.py").write("")

    # Create unrelated files.
    File("unrelated.txt").write("")
    File("lib/unrelated.txt").write("")

    # Create python files in hidden directories.
    File(".venv/lib.py").write("")

    # Create python files we want excluded.
    File("lib2/module/lib.py").write("")

    tar_path = "code.tar.gz"
    file_paths = find_code_files()
    tar_file = create_tar(tar_path, file_paths)

    os.makedirs("dest")
    extract_tar(tar_file, "dest")

    files2 = {file.path for file in Dir("dest")}
    assert files2 == {
        "dest/lib/module/lib.py",
        "dest/workflow.py",
        "dest/lib2/module/lib.py",
        "dest/lib/lib.py",
    }


@use_tempdir
def test_package_job_code() -> None:
    """
    package_code() should include the right files and use the right tar filename.
    """

    # Creating python files.
    File("workflow.py").write("")
    File("lib/lib.py").write("")
    File("lib/module/lib.py").write("")

    # Create unrelated files.
    File("unrelated.txt").write("")
    File("lib/unrelated.txt").write("")

    # Create python files in hidden directories.
    File(".venv/lib.py").write("")

    # Create python files we want excluded.
    File("lib2/module/lib.py").write("")

    # Package up code.
    s3_scratch_prefix = "s3/"
    code_package = {"include": ["**/*.py"]}
    code_file = package_code(s3_scratch_prefix, code_package)

    # Code file should have the right path.
    assert code_file.path.startswith(os.path.join(s3_scratch_prefix, "code"))
    assert code_file.path.endswith(".tar.gz")

    # code_file should contain the right files.
    os.makedirs("dest")
    extract_tar(code_file, "dest")

    files = {file.path for file in Dir("dest")}
    assert files == {
        "dest/lib/module/lib.py",
        "dest/workflow.py",
        "dest/lib2/module/lib.py",
        "dest/lib/lib.py",
    }


def test_parse_code_package_config():
    # Parse default code_package patterns.
    config = Config({"batch": {}})
    assert parse_code_package_config(config["batch"]) == {"excludes": [], "includes": ["**/*.py"]}

    # Disable code packaging.
    config = Config({"batch": {"code_package": False}})
    assert parse_code_package_config(config["batch"]) is False

    # Custom include exclude.
    config = Config({"batch": {"code_includes": "**/*.txt", "code_excludes": ".venv/**"}})
    assert parse_code_package_config(config["batch"]) == {
        "includes": ["**/*.txt"],
        "excludes": [".venv/**"],
    }

    # Multiple patterns with special chars.
    config = Config(
        {"batch": {"code_includes": '**/*.txt "my file.txt" *.py', "code_excludes": ".venv/**"}}
    )
    assert parse_code_package_config(config["batch"]) == {
        "includes": ["**/*.txt", "my file.txt", "*.py"],
        "excludes": [".venv/**"],
    }


@task(limits={"cpu": 1}, random_option=5)
def array_task(x):
    return x + 10


@task()
def other_task(x, y):
    return x - y


# Tests begin here
def test_job_descrs():
    """Tests the JobDescription class used to determine if Jobs are equivalent"""
    j1 = Job(array_task(1))
    j1.task = array_task

    j2 = Job(array_task(2))
    j2.task = array_task

    a = job_array.JobDescription(j1)
    b = job_array.JobDescription(j2)

    assert hash(a) == hash(b)
    assert a == b

    # JobDescription should validate that Job has a task set.
    j3 = Job(other_task(1, y=2))
    with pytest.raises(AssertionError):
        c = job_array.JobDescription(j3)
    j3.task = other_task
    c = job_array.JobDescription(j3)

    assert a != c


@mock_s3
def test_job_staleness():
    """Tests staleness criteria for array'ing jobs"""
    j1 = Job(array_task(1))
    j1.task = array_task
    d = job_array.JobDescription(j1)

    sched = mock_scheduler()
    exec = mock_executor(sched)
    arr = job_array.JobArrayer(exec, submit_interval=10000.0, stale_time=0.05, min_array_size=5)

    for i in range(10):
        arr.add_job(j1, args=(i), kwargs={})

    assert arr.get_stale_descrs() == []
    wait_until(lambda: arr.get_stale_descrs() == [d])


@mock_s3
def test_arrayer_thread():
    """Tests that the arrayer monitor thread can be restarted after exit"""
    j1 = Job(array_task(1))
    j1.task = array_task

    sched = mock_scheduler()
    exec = mock_executor(sched)
    arr = job_array.JobArrayer(exec, submit_interval=10000.0, stale_time=0.05, min_array_size=5)

    arr.add_job(j1, args=(1), kwargs={})
    assert arr._monitor_thread.is_alive()

    # Stop the monitoring thread.
    arr.stop()
    assert not arr._monitor_thread.is_alive()

    # Submitting an additional job should restart the thread.
    arr.add_job(j1, args=(2), kwargs={})
    assert arr._monitor_thread.is_alive()

    arr.stop()


@mock_s3
@patch("redun.executors.aws_utils.get_aws_user", return_value="alice")
@patch("redun.executors.aws_batch.aws_describe_jobs")
@patch("redun.executors.aws_batch.submit_task")
def test_jobs_are_arrayed(submit_task_mock, aws_describe_jobs_mock, get_aws_user_mock):
    """
    Tests repeated jobs are submitted as a single array job. Checks that
    job ID for the array job and child jobs end up tracked
    """
    scheduler = mock_scheduler()
    executor = mock_executor(scheduler)
    executor.arrayer.min_array_size = 3
    executor.arrayer.max_array_size = 7

    aws_describe_jobs_mock.return_value = iter([])
    redun.executors.aws_batch.submit_task.side_effect = [
        {"jobId": "first-array-job", "arrayProperties": {"size": 7}},
        {"jobId": "second-array-job", "arrayProperties": {"size": 3}},
        {"jobId": "single-job"},
    ]

    test_jobs = []
    for i in range(10):
        job = Job(array_task(i))
        job.id = f"task_{i}"
        job.task = array_task
        job.eval_hash = f"eval_hash_{i}"

        executor.submit(job, (i), {})
        test_jobs.append(job)

    # Wait for jobs to get submitted from arrayer to executor.
    wait_until(lambda: len(executor.pending_batch_jobs) == 10)

    # Two array jobs, of size 7 and 3, should have been submitted.
    pending_correct = {
        f"first-array-job:{i}": test_jobs[i] for i in range(executor.arrayer.max_array_size)
    }
    pending_correct.update(
        {
            f"second-array-job:{i}": j
            for i, j in enumerate(test_jobs[executor.arrayer.max_array_size :])
        }
    )
    assert executor.pending_batch_jobs == pending_correct

    # Two array jobs should have been submitted
    assert submit_task_mock.call_count == 2

    # Submit a different kind of job now.
    j = Job(other_task(3, 5))
    j.id = "other_task"
    j.task = other_task
    j.eval_hash = "hashbrowns"
    executor.submit(j, (3, 5), {})

    assert len(executor.arrayer.pending) == 1
    pending_correct["single-job"] = j
    wait_until(lambda: executor.pending_batch_jobs == pending_correct)

    # Make monitor thread exit correctly
    executor.stop()


@use_tempdir
@mock_s3
@patch("redun.executors.aws_utils.get_aws_user", return_value="alice")
@patch("redun.executors.aws_batch.AWSBatchExecutor._submit_single_job")
def test_array_disabling(submit_single_mock, get_aws_user_mock):
    """
    Tests setting `min_array_size=0` disables job arraying.
    """
    # Setup executor.
    config = Config(
        {
            "batch": {
                "image": "image",
                "queue": "queue",
                "s3_scratch": "s3_scratch_prefix",
                "code_includes": "*.txt",
                "min_array_size": 0,
            }
        }
    )
    scheduler = mock_scheduler()

    executor = AWSBatchExecutor("batch", scheduler, config["batch"])
    executor.get_jobs = Mock()
    executor.get_jobs.return_value = []

    # Submit one test job.
    job = Job(other_task(5, 3))
    job.id = "carrots"
    job.task = other_task
    job.eval_hash = "why do i always say carrots in test cases idk"
    executor.submit(job, [5, 3], {})

    # Job should be submitted immediately.
    assert submit_single_mock.call_args
    assert submit_single_mock.call_args[0] == (job, [5, 3], {})

    # Monitor thread should not run.
    assert not executor.arrayer._monitor_thread.is_alive()
    executor.stop()


@mock_s3
@use_tempdir
@patch("redun.executors.aws_batch.batch_submit")
def test_array_job_s3_setup(batch_submit_mock):
    """
    Tests that args, kwargs, and output file paths end up
    in the correct locations in S3 as the right data structure
    """
    scheduler = mock_scheduler()
    executor = mock_executor(scheduler)
    executor.s3_scratch_prefix = "./evil\ndirectory"

    redun.executors.aws_batch.batch_submit.return_value = {
        "jobId": "array-job-id",
        "arrayProperties": {"size": "10"},
    }

    test_jobs = []
    for i in range(10):
        job = Job(other_task(i, y=2 * i))
        job.id = f"task_{i}"
        job.task = other_task
        job.eval_hash = f"hash_{i}"
        test_jobs.append(job)

    pending_jobs = [job_array.PendingJob(test_jobs[i], (i), {"y": 2 * i}) for i in range(10)]
    array_uuid = executor.arrayer.submit_array_job(pending_jobs)

    # Check input file is on S3 and contains list of (args, kwargs) tuples
    input_file = File(
        get_array_scratch_file(
            executor.s3_scratch_prefix, array_uuid, redun.executors.aws_utils.S3_SCRATCH_INPUT
        )
    )
    assert input_file.exists()

    with input_file.open("rb") as infile:
        arglist, kwarglist = pickle.load(infile)
    assert arglist == [(i) for i in range(10)]
    assert kwarglist == [{"y": 2 * i} for i in range(10)]

    # Check output paths file is on S3 and contains correct output paths
    output_file = File(
        get_array_scratch_file(
            executor.s3_scratch_prefix, array_uuid, redun.executors.aws_utils.S3_SCRATCH_OUTPUT
        )
    )
    assert output_file.exists()
    ofiles = json.load(output_file)

    assert ofiles == [
        get_job_scratch_file(
            executor.s3_scratch_prefix, j, redun.executors.aws_utils.S3_SCRATCH_OUTPUT
        )
        for j in test_jobs
    ]

    # Error paths are the same as output, basically
    error_file = File(
        get_array_scratch_file(
            executor.s3_scratch_prefix, array_uuid, redun.executors.aws_utils.S3_SCRATCH_ERROR
        )
    )
    assert error_file.exists()
    efiles = json.load(error_file)

    assert efiles == [
        get_job_scratch_file(
            executor.s3_scratch_prefix, j, redun.executors.aws_utils.S3_SCRATCH_ERROR
        )
        for j in test_jobs
    ]

    # Child job eval hashes should be present as well.
    eval_file = File(
        get_array_scratch_file(
            executor.s3_scratch_prefix, array_uuid, redun.executors.aws_utils.S3_SCRATCH_HASHES
        )
    )
    with eval_file.open("r") as evfile:
        hashes = evfile.read().splitlines()

    assert hashes == [job.eval_hash for job in test_jobs]

    # Make monitor thread exit correctly
    executor.stop()


@mock_s3
@use_tempdir
@patch("redun.executors.aws_batch.batch_submit")
def test_array_oneshot(batch_submit_mock):
    """
    Checks array child jobs can fetch their args and kwargs, and
    put their (correct) output in the right place.
    """
    # Create a code file
    file = File("workflow.py")
    file.write(
        """
from redun import task

@task()
def other_task(x, y):
   return x - y
        """
    )
    create_tar("code.tar.gz", ["workflow.py"])
    file.remove()

    # Submit 10 jobs that will be arrayed
    scheduler = mock_scheduler()
    executor = mock_executor(scheduler)
    executor.s3_scratch_prefix = "."

    redun.executors.aws_batch.batch_submit.return_value = {
        "jobId": "array-job-id",
        "arrayProperties": {"size": "10"},
    }

    test_jobs = []
    for i in range(3):
        job = Job(other_task(i, y=2 * i))
        job.id = f"task_{i}"
        job.task = other_task
        job.eval_hash = f"hash_{i}"
        test_jobs.append(job)

    pending_jobs = [job_array.PendingJob(test_jobs[i], (i,), {"y": 2 * i}) for i in range(3)]
    array_uuid = executor.arrayer.submit_array_job(pending_jobs)

    # Now run 2 of those jobs and make sure they work ok
    client = RedunClient()
    array_dir = os.path.join(executor.s3_scratch_prefix, "array_jobs", array_uuid)
    input_path = os.path.join(array_dir, redun.executors.aws_utils.S3_SCRATCH_INPUT)
    output_path = os.path.join(array_dir, redun.executors.aws_utils.S3_SCRATCH_OUTPUT)
    error_path = os.path.join(array_dir, redun.executors.aws_utils.S3_SCRATCH_ERROR)
    executor.stop()

    for i in range(3):
        os.environ[job_array.AWS_ARRAY_VAR] = str(i)
        client.execute(
            [
                "redun",
                "oneshot",
                "workflow.py",
                "--code",
                "code.tar.gz",
                "--array-job",
                "--input",
                input_path,
                "--output",
                output_path,
                "--error",
                error_path,
                "other_task",
            ]
        )

        # Check output files are there
        output_file = File(
            get_job_scratch_file(
                executor.s3_scratch_prefix,
                test_jobs[i],
                redun.executors.aws_utils.S3_SCRATCH_OUTPUT,
            )
        )

        assert pickle.loads(cast(bytes, output_file.read("rb"))) == i - 2 * i
