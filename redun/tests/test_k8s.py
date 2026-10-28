import json
import os
import pickle
from collections.abc import Callable
from functools import wraps
from typing import cast
from unittest.mock import Mock, patch

import boto3
import pytest
from kubernetes import client
from kubernetes.client.exceptions import ApiException
from urllib3 import HTTPSConnectionPool
from urllib3.exceptions import (
    DecodeError,
    HTTPError,
    MaxRetryError,
    ProtocolError,
    ProxyError,
    SSLError,
)
from urllib3.exceptions import TimeoutError as URLLib3TimeoutError

import redun.executors.k8s
import redun.executors.k8s_utils
from redun import File, job_array, task
from redun.cli import RedunClient, import_script
from redun.config import Config
from redun.executors.code_packaging import create_tar, package_code
from redun.executors.command import REDUN_REQUIRED_VERSION
from redun.executors.k8s import (
    DEFAULT_LOG_FETCH_MAX_BACKOFF_TOTAL,
    FAILED,
    SUCCEEDED,
    K8SExecutor,
    LogFetchRetryBudget,
    _parse_completed_indexes,
    get_hash_from_job_name,
    get_k8s_job_name,
    get_pod_logs,
    is_transient_log_fetch_error,
    submit_task,
)
from redun.executors.k8s_utils import (
    DEFAULT_JOB_PREFIX,
    DEFAULT_TTL_SECONDS_AFTER_FINISHED,
    K8SClient,
    create_job_object,
    create_resources,
)
from redun.executors.scratch import (
    SCRATCH_ERROR,
    SCRATCH_HASHES,
    SCRATCH_INPUT,
    SCRATCH_OUTPUT,
    get_array_scratch_file,
    get_job_scratch_file,
)
from redun.file import Dir
from redun.scheduler import Job, Scheduler, Traceback
from redun.tests.utils import mock_s3, mock_scheduler, use_tempdir, wait_until
from redun.utils import pickle_dumps


def mock_k8s(func: Callable) -> Callable:
    """
    Mock k8s API clients during tests.
    """

    @wraps(func)
    def wrapped(*args, **kwargs):
        with (
            patch.object(K8SClient, "core"),
            patch.object(K8SClient, "batch"),
            patch.object(
                K8SClient,
                "version",
                return_value=(1, 23),
            ),
        ):
            return func(*args, **kwargs)

    return wrapped


@pytest.mark.parametrize("array,suffix", [(False, ""), (True, "-array")])
def test_get_hash_from_job_name(array: bool, suffix: str) -> None:
    """
    Returns the Job hash from a k8s job name.
    """
    prefix = "my-job-prefix"
    job_hash = "c000d7f9b6275c58aff9d5466f6a1174e99195ca"
    job_name = get_k8s_job_name(prefix, job_hash, array=array)
    assert job_name.startswith(prefix)
    assert job_name.endswith(suffix)

    job_hash2 = get_hash_from_job_name(job_name)
    assert job_hash2 == job_hash


def mock_executor(scheduler: Scheduler, code_package: bool = False) -> K8SExecutor:
    """
    Returns an K8SExecutor with AWS API mocks.
    """
    image = "my-image"
    scratch_prefix = "s3://example-bucket/redun/"

    # Setup executor.
    config = Config(
        {
            "k8s_test_name": {
                "type": "k8s",
                "image": image,
                "scratch": scratch_prefix,
                "job_monitor_interval": 0.05,
                "job_stale_time": 0.01,
                "code_package": code_package,
            },
        },
    )
    executor = K8SExecutor("k8s_test_name", scheduler, config["k8s_test_name"])

    executor.get_jobs = Mock()  # ty: ignore[invalid-assignment]
    executor.get_jobs.return_value = []

    s3_client = boto3.client("s3", region_name="us-east-1")
    s3_client.create_bucket(Bucket="example-bucket")

    return executor


# skipped batch tags tests here


@mock_k8s
def test_executor_config(scheduler: Scheduler) -> None:
    """
    Executor should be able to parse its config.
    """
    # Setup executor.
    node_affinity_json = json.dumps(
        {
            "required_during_scheduling_ignored_during_execution": [
                {
                    "match_expressions": [
                        {
                            "key": "karpenter.k8s.aws/instance-family",
                            "operator": "In",
                            "values": ["m5"],
                        },
                    ],
                },
            ],
        },
    )
    config = Config(
        {
            "k8s_test_name": {
                "type": "k8s",
                "image": "image",
                "scratch": "scratch_prefix",
                "code_includes": "*.txt",
                "ephemeral_storage": "10G",
                "node_affinity": node_affinity_json,
                "timeout": 123,
                "ttl_seconds_after_finished": 120,
            },
        },
    )
    executor = K8SExecutor("k8s_test_name", scheduler, config["k8s_test_name"])

    assert executor.image == "image"
    assert executor.scratch_prefix == "scratch_prefix"
    assert isinstance(executor.code_package, dict)
    assert executor.code_package["includes"] == ["*.txt"]
    assert executor.default_task_options["ephemeral_storage"] == "10G"
    assert executor.default_task_options["timeout"] == 123
    assert executor.default_task_options["ttl_seconds_after_finished"] == 120
    assert "node_affinity" in executor.default_task_options
    assert (
        executor.default_task_options["node_affinity"][
            "required_during_scheduling_ignored_during_execution"
        ][0]["match_expressions"][0]["key"]
        == "karpenter.k8s.aws/instance-family"
    )


@mock_k8s
def test_k8s_submit_cpu_limits() -> None:
    """
    k8s_submit should include CPU in requests but not in limits.

    This follows best practices from https://home.robusta.dev/blog/stop-using-cpu-limits
    """
    k8s_client = redun.executors.k8s_utils.K8SClient()

    # Configure the mock to return the job object that was passed to it
    k8s_client.batch.create_namespaced_job = Mock(side_effect=lambda body, namespace: body)  # ty: ignore[invalid-assignment]

    job = redun.executors.k8s.k8s_submit(
        k8s_client=k8s_client,
        command=["echo", "test"],
        image="test-image",
        namespace="default",
        job_name="test-job",
        memory=8,
        vcpus=2,
        gpus=1,
    )

    # Verify CPU is in requests
    assert "cpu" in job.spec.template.spec.containers[0].resources.requests
    assert job.spec.template.spec.containers[0].resources.requests["cpu"] == "2"

    # Verify CPU is NOT in limits
    assert "cpu" not in job.spec.template.spec.containers[0].resources.limits

    # Verify memory is in both requests and limits
    assert "memory" in job.spec.template.spec.containers[0].resources.requests
    assert job.spec.template.spec.containers[0].resources.requests["memory"] == "8G"
    assert "memory" in job.spec.template.spec.containers[0].resources.limits
    assert job.spec.template.spec.containers[0].resources.limits["memory"] == "8G"

    # Verify GPU is in both requests and limits
    assert "nvidia.com/gpu" in job.spec.template.spec.containers[0].resources.requests
    assert job.spec.template.spec.containers[0].resources.requests["nvidia.com/gpu"] == "1"
    assert "nvidia.com/gpu" in job.spec.template.spec.containers[0].resources.limits
    assert job.spec.template.spec.containers[0].resources.limits["nvidia.com/gpu"] == "1"


def test_create_job_object__non_string_metadata_values__converted_to_strings() -> None:
    """
    Labels, annotations, and env values may be numbers when read from JSON config.
    Newer kubernetes clients reject non-string values.
    """
    job = create_job_object(
        labels={"shard": 3, "team": None},
        annotations={"attempt": 2},
        env={"WORKERS": 4, "MODE": "fast", "DEBUG": True},
        resources=create_resources(requests={"ephemeral-storage": 10}),
    )

    container = job.spec.template.spec.containers[0]
    assert job.metadata.labels == {"shard": "3"}
    assert job.metadata.annotations == {"attempt": "2"}
    assert {var.name: var.value for var in container.env} == {
        "WORKERS": "4",
        "MODE": "fast",
        "DEBUG": "true",
    }
    assert container.resources.requests == {"ephemeral-storage": "10"}


@mock_k8s
def test_k8s_submit_ttl_seconds_after_finished() -> None:
    """
    K8S jobs are always assigned a cleanup TTL, which callers can override.
    """
    k8s_client = redun.executors.k8s_utils.K8SClient()
    k8s_client.batch.create_namespaced_job = Mock(side_effect=lambda body, namespace: body)  # ty: ignore[invalid-assignment]

    default_job = redun.executors.k8s.k8s_submit(
        k8s_client=k8s_client,
        command=["echo", "test"],
        image="test-image",
        namespace="default",
    )
    override_job = redun.executors.k8s.k8s_submit(
        k8s_client=k8s_client,
        command=["echo", "test"],
        image="test-image",
        namespace="default",
        ttl_seconds_after_finished=120,
    )

    assert default_job.spec.ttl_seconds_after_finished == DEFAULT_TTL_SECONDS_AFTER_FINISHED
    assert override_job.spec.ttl_seconds_after_finished == 120


@task()
def task1(x: int):
    return x + 10


@task(load_module="custom.module")
def task1_custom_module(x):
    return x + 10


@use_tempdir
@mock_s3
@mock_k8s
@patch("redun.executors.k8s.k8s_submit")
@pytest.mark.parametrize(
    "custom_module, expected_load_module, a_task",
    [
        (None, "redun.tests.test_k8s", task1),
        ("custom.module", "custom.module", task1_custom_module),
    ],
)
def test_submit_task(k8s_submit_mock, custom_module, expected_load_module, a_task):
    job_id = "123"
    image = "my-image"
    namespace = "default"
    scratch_prefix = "s3://example-bucket/redun/"

    s3_client = boto3.client("s3", region_name="us-east-1")
    s3_client.create_bucket(Bucket="example-bucket")
    k8s_client = Mock()

    redun.executors.k8s.k8s_submit.return_value = create_job_object(uid=job_id)

    # Create example workflow script to be packaged.
    File("workflow.py").write(
        f"""
@task(load_module={custom_module})
def task1(x):
    return x + 10
    """,
    )

    job = Job(a_task, a_task())
    job.id = job_id
    job.eval_hash = "eval_hash"
    code_file = package_code(scratch_prefix)
    resp = submit_task(
        k8s_client,
        image,
        namespace,
        scratch_prefix,
        job,
        a_task,
        args=(10,),
        kwargs={},
        code_file=code_file,
    )
    # # We should get a k8s job id back.
    assert resp.metadata.uid == job_id

    # # Input files should be made.
    assert File("s3://example-bucket/redun/jobs/eval_hash/input").exists()
    [code_file] = list(Dir("s3://example-bucket/redun/code"))

    # We should have submitted a job to K8S.
    redun.executors.k8s.k8s_submit.assert_called_with(
        k8s_client,
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
        image="my-image",
        namespace="default",
        job_name="k8s-job-eval_hash",
        array_size=0,
        secret_name=None,
    )


@use_tempdir
@mock_s3
@mock_k8s
@patch("redun.executors.k8s.k8s_submit")
def test_submit_task_deep_file(k8s_submit_mock):
    """
    Executor should be able to submit a task defined in a deeply nested file path.
    """
    job_id = "123"
    image = "my-image"
    namespace = "default"
    scratch_prefix = "s3://example-bucket/redun/"

    client = boto3.client("s3", region_name="us-east-1")
    client.create_bucket(Bucket="example-bucket")
    k8s_client = Mock()

    redun.executors.k8s.k8s_submit.return_value = {"jobId": "k8s-job-id"}

    # Create example workflow script to be packaged.
    File("path/to/workflow.py").write(
        """
from redun import task

@task()
def task1(x):
    return x + 10
    """,
    )

    module = import_script("path/to/workflow.py")

    job = Job(module.task1, module.task1())
    job.id = job_id
    job.eval_hash = "eval_hash"
    code_file = package_code(scratch_prefix)
    resp = submit_task(
        k8s_client,
        image,
        namespace,
        scratch_prefix,
        job,
        module.task1,
        args=(10,),
        kwargs={},
        code_file=code_file,
    )

    # We should get a k8s job id back.
    assert resp["jobId"] == "k8s-job-id"  # ty: ignore[not-subscriptable]

    # Input files should be made.
    assert File("s3://example-bucket/redun/jobs/eval_hash/input").exists()
    [code_file] = list(Dir("s3://example-bucket/redun/code"))

    # We should have submitted a job to k8s.
    redun.executors.k8s.k8s_submit.assert_called_with(
        k8s_client,
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
        image="my-image",
        namespace="default",
        job_name="k8s-job-eval_hash",
        array_size=0,
        secret_name=None,
    )


@mock_s3
@mock_k8s
@patch("redun.executors.k8s.k8s_utils.delete_job")
@patch("redun.executors.k8s.k8s_describe_jobs")
@patch("redun.executors.k8s.get_k8s_job_pods")
@patch("redun.executors.k8s.k8s_submit")
def test_executor(
    k8s_submit_mock: Mock,
    get_k8s_job_pods_mock: Mock,
    k8s_describe_jobs_mock: Mock,
    delete_job_mock: Mock,
) -> None:
    """
    Ensure that we can submit job to K8SExecutor.
    """
    k8s_job_id = "k8s-job-id"
    k8s_job2_id = "k8s-job2-id"

    # Setup K8S mocks.
    k8s_describe_jobs_mock.return_value = []
    get_k8s_job_pods_mock.return_value = []

    scheduler = mock_scheduler()
    executor = mock_executor(scheduler)
    executor.start()

    k8s_submit_mock.return_value = create_job_object(
        name=DEFAULT_JOB_PREFIX + "-eval_hash",
        uid=k8s_job_id,
    )
    # Submit redun job that will succeed.
    expr = task1(10)
    job = Job(task1, expr)
    job.eval_hash = "eval_hash"
    job.args = ((10,), {})
    executor.submit(job)

    # Wait until job arrayer actually submits it.
    wait_until(lambda: executor.arrayer.num_pending == 0)

    # Ensure job options were passed correctly.
    assert k8s_submit_mock.call_args
    assert k8s_submit_mock.call_args[1] == {
        "image": "my-image",
        "namespace": "default",
        "job_name": DEFAULT_JOB_PREFIX + "-eval_hash",
        "array_size": 0,
        "retries": 0,
        "ttl_seconds_after_finished": DEFAULT_TTL_SECONDS_AFTER_FINISHED,
        "service_account_name": "default",
        "secret_name": None,
        "vcpus": 1,
        "memory": 4,
        "k8s_labels": {
            "redun.insitro.com/execution_id": "",
            "redun.insitro.com/job_id": job.id,
            "redun.insitro.com/job_hash": job.eval_hash,
            "redun.insitro.com/executor_name": "k8s_test_name",
            "redun.insitro.com/executor_type": "k8s",
        },
        "annotations": {
            "redun.insitro.com/project": "",
            "redun.insitro.com/task_name": "task1",
        },
    }

    # Setup new mock return values.
    k8s_submit_mock.return_value = create_job_object(
        name=DEFAULT_JOB_PREFIX + "-eval_hash2",
        uid=k8s_job2_id,
    )

    # Submit redun job that will fail.
    expr2 = task1.options(memory=8)(11)
    job2 = Job(task1, expr2)
    job2.eval_hash = "eval_hash2"
    job2.args = ((11,), {})
    executor.submit(job2)

    # Let job get stale so job arrayer actually submits it.
    wait_until(lambda: executor.arrayer.num_pending == 0)

    # Ensure job options were passed correctly.
    assert k8s_submit_mock.call_args[1] == {
        "image": "my-image",
        "namespace": "default",
        "job_name": DEFAULT_JOB_PREFIX + "-eval_hash2",
        "array_size": 0,
        "retries": 0,
        "ttl_seconds_after_finished": DEFAULT_TTL_SECONDS_AFTER_FINISHED,
        "service_account_name": "default",
        "secret_name": None,
        "vcpus": 1,
        "memory": 8,
        "k8s_labels": {
            "redun.insitro.com/execution_id": "",
            "redun.insitro.com/job_id": job2.id,
            "redun.insitro.com/job_hash": job2.eval_hash,
            "redun.insitro.com/executor_name": "k8s_test_name",
            "redun.insitro.com/executor_type": "k8s",
        },
        "annotations": {
            "redun.insitro.com/project": "",
            "redun.insitro.com/task_name": "task1",
        },
    }

    # Setup new mock return values for ephemeral_storage test.
    k8s_submit_mock.return_value = create_job_object(
        name=DEFAULT_JOB_PREFIX + "-eval_hash3",
        uid="k8s-job3-id",
    )

    # Submit redun job with ephemeral_storage option.
    expr3 = task1.options(ephemeral_storage="20G", ttl_seconds_after_finished=120)(12)
    job3 = Job(task1, expr3)
    job3.eval_hash = "eval_hash3"
    job3.args = ((12,), {})
    executor.submit(job3)

    # Let job get stale so job arrayer actually submits it.
    wait_until(lambda: executor.arrayer.num_pending == 0)

    # Ensure ephemeral_storage option was passed correctly.
    assert k8s_submit_mock.call_args[1]["ephemeral_storage"] == "20G"
    assert k8s_submit_mock.call_args[1]["ttl_seconds_after_finished"] == 120

    # Simulate k8s completing job.
    output_file = File("s3://example-bucket/redun/jobs/eval_hash/output")
    output_file.write(pickle_dumps(task1.func(10)), mode="wb")

    # Simulate k8s failing.
    error = ValueError("Boom")
    error_file = File("s3://example-bucket/redun/jobs/eval_hash2/error")
    error_file.write(pickle_dumps((error, Traceback.from_error(error))), mode="wb")

    fake_k8s_job = create_job_object(uid=k8s_job_id, name=DEFAULT_JOB_PREFIX + "-eval_hash")
    fake_k8s_job.status = client.V1JobStatus(succeeded=1)
    fake_k8s_job2 = create_job_object(uid=k8s_job2_id, name=DEFAULT_JOB_PREFIX + "-eval_hash2")
    fake_k8s_job2.status = client.V1JobStatus(failed=1)
    # Return jobs once, then keep returning empty list to avoid processing them multiple times
    returned_jobs = [False]

    def describe_jobs_side_effect(*args, **kwargs):
        if not returned_jobs[0]:
            returned_jobs[0] = True
            return [fake_k8s_job, fake_k8s_job2]
        return []

    k8s_describe_jobs_mock.side_effect = describe_jobs_side_effect

    pod = client.V1Pod(
        metadata=client.V1ObjectMeta(
            name="name",
            namespace="default",
        ),
        status=client.V1PodStatus(),
    )
    get_k8s_job_pods_mock.return_value = [pod]

    scheduler.batch_wait([job.id, job2.id])
    executor.stop()

    # Job results and errors should be sent back to scheduler.
    assert scheduler.job_results[job.id] == 20
    assert isinstance(scheduler.job_errors[job2.id], ValueError)

    # Assert job tags.
    job.job_tags == [("k8s_job", "k8s-job-id"), ("aws_log_stream", "log1")]
    job.job_tags == [("k8s_job", "k8s-job2-id"), ("aws_log_stream", "log2")]
    delete_job_mock.assert_not_called()


@mock_s3
@mock_k8s
@patch("redun.executors.k8s.k8s_utils.delete_job")
@patch("redun.executors.k8s.get_k8s_job_pods", return_value=[])
def test_completed_array_job_is_retained(
    get_k8s_job_pods_mock: Mock,
    delete_job_mock: Mock,
) -> None:
    """
    Completed array jobs remain available until Kubernetes applies their TTL.
    """
    scheduler = mock_scheduler()
    executor = mock_executor(scheduler)
    redun_jobs = {}
    for index in range(2):
        redun_job = Job(task1, task1(index))
        redun_job.id = f"task-{index}"
        redun_job.eval_hash = f"hash-{index}"
        redun_jobs[index] = redun_job

    k8s_job = create_job_object(name="test-array", uid="test-array-uid")
    k8s_job.spec.parallelism = 2
    k8s_job.spec.completions = 2
    k8s_job.status = client.V1JobStatus(
        succeeded=2,
        completed_indexes="0,1",
        conditions=[client.V1JobCondition(type="Complete", status="True")],
    )
    executor.pending_k8s_jobs[k8s_job.metadata.name] = redun_jobs

    with patch.object(executor, "_process_redun_job") as process_redun_job_mock:
        executor._process_k8s_job_status(k8s_job)

    assert process_redun_job_mock.call_count == 2
    assert k8s_job.metadata.name not in executor.pending_k8s_jobs
    get_k8s_job_pods_mock.assert_called_once()
    delete_job_mock.assert_not_called()


@mock_s3
@mock_k8s
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
        create_job_object(uid="headnode", name=f"{prefix}_automation_headnode"),
        create_job_object(uid="preprocess", name=f"{prefix}_preprocess-{hash1}"),
        create_job_object(uid="decode", name=f"{prefix}_decode-{hash2}"),
    ]

    executor.gather_inflight_jobs()

    assert executor.preexisting_k8s_jobs == {
        hash1: "preprocess",
        hash2: "decode",
    }


@mock_s3
@mock_k8s
def test_failed_jobs_are_not_reunited() -> None:
    """A terminal-FAILED Job must not be gathered for reuniting.

    K8s keeps failed Jobs until their TTL garbage-collects them, so a failed
    publish can still be returned by ``get_jobs``. Reuniting with it would
    re-serve the stale failure and reject the redun job instead of re-running
    it. Only inflight and succeeded Jobs should be gathered: inflight so a
    restarted scheduler re-attaches instead of double-submitting, succeeded so
    it can recover the result. Failed Jobs are skipped so they are resubmitted.
    """
    scheduler = mock_scheduler()
    executor = mock_executor(scheduler)

    prefix = "liveratlas_spearmancor"
    running_hash = "111111111"
    failed_hash = "222222222"
    succeeded_hash = "333333333"

    running = create_job_object(uid="running", name=f"{prefix}_run-{running_hash}")
    running.status = client.V1JobStatus(active=1)
    failed = create_job_object(uid="failed", name=f"{prefix}_fail-{failed_hash}")
    failed.status = client.V1JobStatus(failed=1)
    succeeded = create_job_object(uid="succeeded", name=f"{prefix}_done-{succeeded_hash}")
    succeeded.status = client.V1JobStatus(succeeded=1)

    executor.get_jobs.return_value = [running, failed, succeeded]

    executor.gather_inflight_jobs()

    # The failed Job is dropped; the inflight and succeeded Jobs are retained.
    assert executor.preexisting_k8s_jobs == {
        running_hash: "running",
        succeeded_hash: "succeeded",
    }


@mock_s3
@mock_k8s
@patch("redun.executors.k8s.package_code")
@patch("redun.executors.k8s.k8s_describe_jobs")
@patch("redun.executors.k8s.k8s_submit")
def test_code_packaging(
    k8s_submit_mock: Mock,
    k8s_describe_jobs_mock: Mock,
    package_code_mock: Mock,
) -> None:
    """
    Ensure that code packaging only happens on first submission.
    """
    package_code_mock.return_value = File("s3://fake-bucket/code.tar.gz")

    k8s_describe_jobs_mock.return_value = []

    scheduler = mock_scheduler()
    executor = mock_executor(scheduler, code_package=True)
    executor.start()

    # Starting the executor should not have triggered code packaging.
    assert executor.code_file is None
    assert package_code_mock.call_count == 0

    k8s_job_id = "k8s-job-id"
    k8s_submit_mock.return_value = create_job_object(
        name=DEFAULT_JOB_PREFIX + "-eval_hash",
        uid=k8s_job_id,
    )

    # Hand create jobs.
    job1 = Job(task1, task1(10))
    job1.id = "1"
    job1.eval_hash = "eval_hash"
    job1.args = ((10,), {})

    k8s_job_id2 = "k8s-job-id-2"
    k8s_submit_mock.return_value = create_job_object(
        name=DEFAULT_JOB_PREFIX + "-eval_hash2",
        uid=k8s_job_id2,
    )

    job2 = Job(task1, task1(20))
    job2.id = "2"
    job2.eval_hash = "eval_hash2"
    job2.args = ((20,), {})

    # Submit a job and ensure that the code was packaged.
    executor.submit(job1)
    assert executor.code_file
    assert package_code_mock.call_count == 1

    # Submit another job and ensure that code was not packaged again.
    executor.submit(job2)
    assert package_code_mock.call_count == 1

    executor.stop()


@mock_s3
@mock_k8s
@patch("redun.executors.k8s.k8s_describe_jobs")
@pytest.mark.skip(reason="not working")
def test_inflight_join_only_on_first_submission(k8s_describe_jobs_mock: Mock) -> None:
    """
    Ensure that inflight jobs are only gathered once and not on every job submission.
    """
    scheduler = mock_scheduler()
    executor = mock_executor(scheduler)

    executor.start()

    # Hand create jobs.
    job1 = Job(task1, task1(10))
    job1.id = "1"
    job1.eval_hash = "eval_hash"

    job2 = Job(task1, task1(20))
    job2.id = "2"
    job2.eval_hash = "eval_hash"

    # Submit redun job.
    executor.submit(job1)

    # # Ensure that inflight jobs were gathered.
    assert executor.get_jobs.call_count == 1

    # # Submit the second job and confirm that job reuniting was not done again.
    executor.submit(job2)
    assert executor.get_jobs.call_count == 1

    executor.stop()


@mock_s3
@mock_k8s
@patch("redun.executors.k8s.k8s_describe_jobs")
@patch("redun.executors.k8s_utils.delete_job")
@patch("redun.executors.k8s.k8s_submit")
@pytest.mark.skip(reason="not working")
def test_executor_inflight_job(
    k8s_submit_mock: Mock,
    delete_job_mock: Mock,
    k8s_describe_jobs_mock: Mock,
) -> None:
    """
    Ensure we reunite with an inflight job.
    """
    k8s_job_id = "333"

    # Setup k8s mocks.

    k8s_describe_jobs_mock.return_value = iter([])
    k8s_job = create_job_object(uid=k8s_job_id, name=DEFAULT_JOB_PREFIX + "-eval_hash")
    k8s_describe_jobs_mock.return_value = [k8s_job]

    scheduler = mock_scheduler()
    executor = mock_executor(scheduler)
    executor.get_jobs.return_value = [k8s_job]
    executor.start()

    # Hand create job.
    job = Job(task1, task1(10))
    job.id = "123"
    job.eval_hash = "eval_hash"

    # Submit redun job.
    executor.submit(job)

    # Ensure no k8s jobs were submitted.
    assert k8s_submit_mock.call_count == 0

    # Simulate K8S completing with valid value.
    output_file = File("s3://example-bucket/redun/jobs/eval_hash/output")
    output_file.write(pickle_dumps(task1.func(10)), mode="wb")

    k8s_job.status = client.V1JobStatus(succeeded=1)
    k8s_describe_jobs_mock.return_value = [k8s_job]
    # k8s_describe_jobs_mock.return_value = [k8s_job]
    scheduler.batch_wait([job.id])
    # Simulate pre-existing job output.
    output_file = File("s3://example-bucket/redun/jobs/eval_hash/output")
    output_file.write(pickle_dumps(task1.func(10)), mode="wb")

    # Ensure redun job is completed.
    assert scheduler.job_results[job.id] == 20

    executor.stop()


@task(limits={"cpu": 1}, random_option=5)
def array_task(x):
    return x + 10


@task()
def other_task(x, y):
    return x - y


@mock_s3
@mock_k8s
@patch("redun.executors.k8s_utils.delete_job")
@patch("redun.executors.aws_utils.get_aws_user", return_value="alice")
@patch("redun.executors.k8s.k8s_describe_jobs")
@patch("redun.executors.k8s.submit_task")
@pytest.mark.skip(reason="not working")
def test_jobs_are_arrayed(
    submit_task_mock: Mock,
    k8s_describe_jobs_mock: Mock,
    get_aws_user_mock: Mock,
    delete_job_mock: Mock,
) -> None:
    """
    Tests repeated jobs are submitted as a single array job. Checks that
    job ID for the array job and child jobs end up tracked
    """
    scheduler = mock_scheduler()
    executor = mock_executor(scheduler)
    executor.arrayer.min_array_size = 3
    executor.arrayer.max_array_size = 7

    k8s_describe_jobs_mock.return_value = iter([])
    faj = create_job_object(uid="first-array-job")
    faj.spec.parallelism = 3
    faj.spec.completions = 3
    faj.spec.completion_mode = "Indexed"

    saj = create_job_object(uid="second-array-job")
    saj.spec.parallelism = 7
    saj.spec.completions = 7
    saj.spec.completion_mode = "Indexed"

    redun.executors.k8s.submit_task.side_effect = [
        faj,
        saj,
        create_job_object(uid="single-job"),
    ]

    test_jobs = []
    for i in range(10):
        job = Job(array_task, array_task(i))
        job.id = f"task_{i}"
        job.eval_hash = f"eval_hash_{i}"
        executor.submit(job)
        test_jobs.append(job)

    # Wait for jobs to get submitted from arrayer to executor.
    wait_until(lambda: len(executor.pending_k8s_jobs) == 10)

    # Two array jobs, of size 7 and 3, should have been submitted.
    pending_correct = {
        f"first-array-job:{i}": test_jobs[i] for i in range(executor.arrayer.max_array_size)
    }
    pending_correct.update(
        {
            f"second-array-job:{i}": j
            for i, j in enumerate(test_jobs[executor.arrayer.max_array_size :])
        },
    )
    assert executor.pending_k8s_jobs == pending_correct

    # Two array jobs should have been submitted
    assert submit_task_mock.call_count == 2

    # Submit a different kind of job now.
    j = Job(other_task, other_task(3, 5))
    j.id = "other_task"
    j.eval_hash = "hashbrowns"
    executor.submit(j)

    assert len(executor.arrayer.pending) == 1
    pending_correct["single-job"] = j
    wait_until(lambda: executor.pending_k8s_jobs == pending_correct)

    # Make monitor thread exit correctly
    executor.stop()


@use_tempdir
@mock_s3
@mock_k8s
@patch("redun.executors.aws_utils.get_aws_user", return_value="alice")
@patch("redun.executors.k8s.K8SExecutor._submit_single_job")
def test_array_disabling(submit_single_mock: Mock, get_aws_user_mock: Mock) -> None:
    """
    Tests setting `min_array_size=0` disables job arraying.
    """
    # Setup executor.
    config = Config(
        {
            "k8s_test_name": {
                "type": "k8s",
                "image": "image",
                "scratch": "scratch_prefix",
                "code_includes": "*.txt",
                "min_array_size": 0,
            },
        },
    )
    scheduler = mock_scheduler()

    executor = K8SExecutor("k8s_test_name", scheduler, config["k8s_test_name"])
    executor.get_jobs = Mock()  # ty: ignore[invalid-assignment]
    executor.get_jobs.return_value = []

    # Submit one test job.
    job = Job(other_task, other_task(5, 3))
    job.id = "carrots"
    job.eval_hash = "why do i always say carrots in test cases idk"
    job.args = ((5, 3), {})
    executor.submit(job)

    # Job should be submitted immediately.
    assert submit_single_mock.call_args
    assert submit_single_mock.call_args[0] == (job,)

    # Monitor thread should not run.
    assert not executor.arrayer._monitor_thread.is_alive()
    executor.stop()


@mock_s3
@mock_k8s
@use_tempdir
@patch("redun.executors.k8s.k8s_submit")
def test_array_job_scratch_setup(k8s_submit_mock: Mock) -> None:
    """
    Tests that args, kwargs, and output file paths end up
    in the correct scratch locations as the right data structure.
    """
    scheduler = mock_scheduler()
    executor = mock_executor(scheduler)
    executor.scratch_prefix = "./evil\ndirectory"

    k8s_job_id = "k8s-job-id"
    jo = create_job_object(name=DEFAULT_JOB_PREFIX + "-eval_hash", uid=k8s_job_id)
    jo.spec.parallelism = 3
    jo.spec.completions = 3
    jo.spec.completion_mode = "Indexed"
    k8s_submit_mock.return_value = jo

    test_jobs = []
    for i in range(10):
        job = Job(other_task, other_task(i, y=2 * i))
        job.id = f"task_{i}"
        job.eval_hash = f"hash_{i}"
        job.args = ((i,), {"y": 2 * i})
        test_jobs.append(job)

    array_uuid = executor._submit_array_job(test_jobs)

    # Check input file is on S3 and contains list of (args, kwargs) tuples
    input_file = File(get_array_scratch_file(executor.scratch_prefix, array_uuid, SCRATCH_INPUT))
    assert input_file.exists()

    with input_file.open("rb") as infile:
        arglist, kwarglist = pickle.load(infile)
    assert arglist == [(i,) for i in range(10)]
    assert kwarglist == [{"y": 2 * i} for i in range(10)]

    # Check output paths file is on S3 and contains correct output paths
    output_file = File(get_array_scratch_file(executor.scratch_prefix, array_uuid, SCRATCH_OUTPUT))
    assert output_file.exists()
    ofiles = json.loads(output_file.read())

    assert ofiles == [
        get_job_scratch_file(executor.scratch_prefix, j, SCRATCH_OUTPUT) for j in test_jobs
    ]

    # Error paths are the same as output, basically
    error_file = File(get_array_scratch_file(executor.scratch_prefix, array_uuid, SCRATCH_ERROR))
    assert error_file.exists()
    efiles = json.loads(error_file.read())

    assert efiles == [
        get_job_scratch_file(executor.scratch_prefix, j, SCRATCH_ERROR) for j in test_jobs
    ]

    # Child job eval hashes should be present as well.
    eval_file = File(get_array_scratch_file(executor.scratch_prefix, array_uuid, SCRATCH_HASHES))
    with eval_file.open("r") as evfile:
        hashes = evfile.read().splitlines()

    assert hashes == [job.eval_hash for job in test_jobs]

    # Make monitor thread exit correctly
    executor.stop()


@mock_s3
@mock_k8s
@use_tempdir
@patch("redun.executors.k8s.k8s_submit")
def test_array_oneshot(k8s_submit_mock: Mock) -> None:
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
        """,
    )
    create_tar("code.tar.gz", ["workflow.py"])
    file.remove()

    # Submit 10 jobs that will be arrayed
    scheduler = mock_scheduler()
    executor = mock_executor(scheduler)
    executor.scratch_prefix = "."

    jo = create_job_object(uid="array-job-id")
    jo.spec.parallelism = 10
    jo.spec.completions = 10
    jo.spec.completion_mode = "Indexed"
    k8s_submit_mock.return_value = jo

    test_jobs = []
    for i in range(3):
        job = Job(other_task, other_task(i, y=2 * i))
        job.id = f"task_{i}"
        job.eval_hash = f"hash_{i}"
        job.args = ((i,), {"y": 2 * i})
        test_jobs.append(job)

    array_uuid = executor._submit_array_job(test_jobs)

    # Now run 2 of those jobs and make sure they work ok
    client = RedunClient()
    array_dir = os.path.join(executor.scratch_prefix, "array_jobs", array_uuid)
    input_path = os.path.join(array_dir, SCRATCH_INPUT)
    output_path = os.path.join(array_dir, SCRATCH_OUTPUT)
    error_path = os.path.join(array_dir, SCRATCH_ERROR)
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
            ],
        )

        # Check output files are there
        output_file = File(
            get_job_scratch_file(
                executor.scratch_prefix,
                test_jobs[i],
                SCRATCH_OUTPUT,
            ),
        )

        assert pickle.loads(cast("bytes", output_file.read("rb"))) == i - 2 * i


@mock_s3
@mock_k8s
@patch("redun.executors.k8s.k8s_describe_jobs")
@patch("redun.executors.k8s.k8s_submit")
def test_executor_with_config_timeout(
    k8s_submit_mock: Mock,
    k8s_describe_jobs_mock: Mock,
) -> None:
    """
    Executor-level timeout should be passed to submitted k8s jobs.
    """
    k8s_describe_jobs_mock.return_value = []

    config = Config(
        {
            "k8s_test_name": {
                "type": "k8s",
                "image": "my-image",
                "scratch": "s3://example-bucket/redun/",
                "job_monitor_interval": 0.05,
                "job_stale_time": 0.01,
                "timeout": 123,
            },
        },
    )

    scheduler = mock_scheduler()
    executor = K8SExecutor("k8s_test_name", scheduler, config["k8s_test_name"])
    executor.get_jobs = Mock()  # ty: ignore[invalid-assignment]
    executor.get_jobs.return_value = []

    s3_client = boto3.client("s3", region_name="us-east-1")
    s3_client.create_bucket(Bucket="example-bucket")

    executor.start()

    k8s_submit_mock.return_value = create_job_object(
        name=DEFAULT_JOB_PREFIX + "-eval_hash",
        uid="k8s-job-id",
    )

    expr = task1(10)
    job = Job(task1, expr)
    job.eval_hash = "eval_hash"
    job.args = ((10,), {})
    executor.submit(job)

    wait_until(lambda: executor.arrayer.num_pending == 0)

    assert k8s_submit_mock.call_args
    assert k8s_submit_mock.call_args[1]["timeout"] == 123

    k8s_submit_mock.return_value = create_job_object(
        name=DEFAULT_JOB_PREFIX + "-eval_hash2",
        uid="k8s-job-id2",
    )

    expr2 = task1.options(timeout=456)(11)
    job2 = Job(task1, expr2)
    job2.eval_hash = "eval_hash2"
    job2.args = ((11,), {})
    executor.submit(job2)

    wait_until(lambda: executor.arrayer.num_pending == 0)

    assert k8s_submit_mock.call_args[1]["timeout"] == 456

    executor.stop()


@mock_k8s
def test_k8s_submit_with_node_affinity() -> None:
    """
    k8s_submit should properly configure node affinity for pod scheduling.
    """
    k8s_client = redun.executors.k8s_utils.K8SClient()

    # Configure the mock to return the job object that was passed to it
    k8s_client.batch.create_namespaced_job = Mock(side_effect=lambda body, namespace: body)  # ty: ignore[invalid-assignment]

    node_affinity = {
        "required_during_scheduling_ignored_during_execution": [
            {
                "match_expressions": [
                    {
                        "key": "karpenter.k8s.aws/instance-family",
                        "operator": "In",
                        "values": ["i3"],
                    },
                ],
            },
        ],
    }

    job = redun.executors.k8s.k8s_submit(
        k8s_client=k8s_client,
        command=["echo", "test"],
        image="test-image",
        namespace="default",
        job_name="test-job",
        node_affinity=node_affinity,
    )

    # Verify affinity is configured
    assert job.spec.template.spec.affinity is not None
    assert job.spec.template.spec.affinity.node_affinity is not None

    # Verify required scheduling is set
    required = job.spec.template.spec.affinity.node_affinity.required_during_scheduling_ignored_during_execution
    assert required is not None
    assert len(required.node_selector_terms) == 1

    # Verify match expressions
    term = required.node_selector_terms[0]
    assert len(term.match_expressions) == 1
    expr = term.match_expressions[0]
    assert expr.key == "karpenter.k8s.aws/instance-family"
    assert expr.operator == "In"
    assert expr.values == ["i3"]


@mock_s3
@mock_k8s
@patch("redun.executors.k8s.k8s_describe_jobs")
@patch("redun.executors.k8s.k8s_submit")
def test_executor_with_node_affinity(k8s_submit_mock: Mock, k8s_describe_jobs_mock: Mock) -> None:
    """
    Executor should pass node_affinity from task options to k8s_submit.
    """
    # Setup K8S mocks.
    k8s_describe_jobs_mock.return_value = []

    scheduler = mock_scheduler()
    executor = mock_executor(scheduler)
    executor.start()

    k8s_submit_mock.return_value = create_job_object(
        name=DEFAULT_JOB_PREFIX + "-eval_hash",
        uid="k8s-job-id",
    )

    # Submit redun job with node_affinity option
    node_affinity = {
        "required_during_scheduling_ignored_during_execution": [
            {
                "match_expressions": [
                    {
                        "key": "karpenter.k8s.aws/instance-family",
                        "operator": "In",
                        "values": ["i3"],
                    },
                ],
            },
        ],
    }

    expr = task1.options(node_affinity=node_affinity)(10)
    job = Job(task1, expr)
    job.eval_hash = "eval_hash"
    job.args = ((10,), {})
    executor.submit(job)

    # Wait until job arrayer actually submits it
    wait_until(lambda: executor.arrayer.num_pending == 0)

    # Ensure node_affinity was passed correctly
    assert k8s_submit_mock.call_args
    assert "node_affinity" in k8s_submit_mock.call_args[1]
    assert k8s_submit_mock.call_args[1]["node_affinity"] == node_affinity

    executor.stop()


@mock_s3
@mock_k8s
@patch("redun.executors.k8s.k8s_describe_jobs")
@patch("redun.executors.k8s.k8s_submit")
def test_executor_node_affinity_override(
    k8s_submit_mock: Mock,
    k8s_describe_jobs_mock: Mock,
) -> None:
    """
    Task-level node_affinity should override executor-level node_affinity.
    """
    # Setup K8S mocks.
    k8s_describe_jobs_mock.return_value = []

    # Setup executor with default node_affinity from config
    default_node_affinity = {
        "required_during_scheduling_ignored_during_execution": [
            {
                "match_expressions": [
                    {
                        "key": "karpenter.k8s.aws/instance-family",
                        "operator": "In",
                        "values": ["m5"],
                    },
                ],
            },
        ],
    }

    config = Config(
        {
            "k8s_test_name": {
                "type": "k8s",
                "image": "my-image",
                "scratch": "s3://example-bucket/redun/",
                "job_monitor_interval": 0.05,
                "job_stale_time": 0.01,
                "node_affinity": json.dumps(default_node_affinity),
            },
        },
    )

    scheduler = mock_scheduler()
    executor = K8SExecutor("k8s_test_name", scheduler, config["k8s_test_name"])
    executor.get_jobs = Mock()  # ty: ignore[invalid-assignment]
    executor.get_jobs.return_value = []

    s3_client = boto3.client("s3", region_name="us-east-1")
    s3_client.create_bucket(Bucket="example-bucket")

    executor.start()

    k8s_submit_mock.return_value = create_job_object(
        name=DEFAULT_JOB_PREFIX + "-eval_hash",
        uid="k8s-job-id",
    )

    # Submit redun job with different node_affinity that should override the default
    task_node_affinity = {
        "required_during_scheduling_ignored_during_execution": [
            {
                "match_expressions": [
                    {
                        "key": "karpenter.k8s.aws/instance-family",
                        "operator": "In",
                        "values": ["i3"],
                    },
                ],
            },
        ],
    }

    expr = task1.options(node_affinity=task_node_affinity)(10)
    job = Job(task1, expr)
    job.eval_hash = "eval_hash"
    job.args = ((10,), {})
    executor.submit(job)

    # Wait until job arrayer actually submits it
    wait_until(lambda: executor.arrayer.num_pending == 0)

    # Ensure task-level node_affinity was used, not executor-level
    assert k8s_submit_mock.call_args
    assert "node_affinity" in k8s_submit_mock.call_args[1]
    assert k8s_submit_mock.call_args[1]["node_affinity"] == task_node_affinity
    assert k8s_submit_mock.call_args[1]["node_affinity"] != default_node_affinity

    executor.stop()


@mock_k8s
def test_executor_config_env(scheduler: Scheduler) -> None:
    """
    Executor should be able to parse env config.
    """
    env_json = json.dumps({"DEBUG": "true", "API_URL": "http://api.example.com"})
    config = Config(
        {
            "k8s_test_name": {
                "type": "k8s",
                "image": "image",
                "scratch": "scratch_prefix",
                "env": env_json,
            },
        },
    )
    executor = K8SExecutor("k8s_test_name", scheduler, config["k8s_test_name"])

    assert "env" in executor.default_task_options
    assert executor.default_task_options["env"] == {
        "DEBUG": "true",
        "API_URL": "http://api.example.com",
    }


@mock_k8s
def test_k8s_submit_with_env() -> None:
    """
    k8s_submit should properly pass environment variables to the container.
    """
    k8s_client = redun.executors.k8s_utils.K8SClient()

    # Configure the mock to return the job object that was passed to it
    k8s_client.batch.create_namespaced_job = Mock(side_effect=lambda body, namespace: body)  # ty: ignore[invalid-assignment]

    env = {"DEBUG": "true", "API_URL": "http://api.example.com"}

    job = redun.executors.k8s.k8s_submit(
        k8s_client=k8s_client,
        command=["echo", "test"],
        image="test-image",
        namespace="default",
        job_name="test-job",
        env=env,
    )

    # Verify env vars are configured in the container
    container_env = job.spec.template.spec.containers[0].env
    assert container_env is not None

    # Find the user-provided env vars (filter out any AWS secret refs)
    user_env = [e for e in container_env if hasattr(e, "value") and e.value is not None]
    assert len(user_env) == 2
    # Convert to dict for order-independent checking
    user_env_dict = {e.name: e.value for e in user_env}
    assert user_env_dict["DEBUG"] == "true"
    assert user_env_dict["API_URL"] == "http://api.example.com"


@mock_s3
@mock_k8s
@patch("redun.executors.k8s.k8s_describe_jobs")
@patch("redun.executors.k8s.k8s_submit")
def test_executor_with_env(k8s_submit_mock: Mock, k8s_describe_jobs_mock: Mock) -> None:
    """
    Executor should pass env from task options to k8s_submit.
    """
    # Setup K8S mocks.
    k8s_describe_jobs_mock.return_value = []

    scheduler = mock_scheduler()
    executor = mock_executor(scheduler)
    executor.start()

    k8s_submit_mock.return_value = create_job_object(
        name=DEFAULT_JOB_PREFIX + "-eval_hash",
        uid="k8s-job-id",
    )

    # Submit redun job with env option
    env = {"DEBUG": "true"}

    expr = task1.options(env=env)(10)
    job = Job(task1, expr)
    job.eval_hash = "eval_hash"
    job.args = ((10,), {})
    executor.submit(job)

    # Wait until job arrayer actually submits it
    wait_until(lambda: executor.arrayer.num_pending == 0)

    # Ensure env was passed correctly
    assert k8s_submit_mock.call_args
    assert "env" in k8s_submit_mock.call_args[1]
    assert k8s_submit_mock.call_args[1]["env"] == env

    executor.stop()


@mock_s3
@mock_k8s
@patch("redun.executors.k8s.k8s_describe_jobs")
@patch("redun.executors.k8s.k8s_submit")
def test_executor_env_merge(
    k8s_submit_mock: Mock,
    k8s_describe_jobs_mock: Mock,
) -> None:
    """
    Task-level env should merge with and override executor-level env.
    """
    # Setup K8S mocks.
    k8s_describe_jobs_mock.return_value = []

    # Setup executor with default env from config
    config_env = {"CONFIG_VAR": "from_config", "OVERRIDE_VAR": "config_value"}

    config = Config(
        {
            "k8s_test_name": {
                "type": "k8s",
                "image": "my-image",
                "scratch": "s3://example-bucket/redun/",
                "job_monitor_interval": 0.05,
                "job_stale_time": 0.01,
                "env": json.dumps(config_env),
            },
        },
    )

    scheduler = mock_scheduler()
    executor = K8SExecutor("k8s_test_name", scheduler, config["k8s_test_name"])
    executor.get_jobs = Mock()  # ty: ignore[invalid-assignment]
    executor.get_jobs.return_value = []

    s3_client = boto3.client("s3", region_name="us-east-1")
    s3_client.create_bucket(Bucket="example-bucket")

    executor.start()

    k8s_submit_mock.return_value = create_job_object(
        name=DEFAULT_JOB_PREFIX + "-eval_hash",
        uid="k8s-job-id",
    )

    # Submit redun job with different env that should merge and override
    task_env = {"TASK_VAR": "from_task", "OVERRIDE_VAR": "task_value"}

    expr = task1.options(env=task_env)(10)
    job = Job(task1, expr)
    job.eval_hash = "eval_hash"
    job.args = ((10,), {})
    executor.submit(job)

    # Wait until job arrayer actually submits it
    wait_until(lambda: executor.arrayer.num_pending == 0)

    # Ensure env was merged correctly
    assert k8s_submit_mock.call_args
    assert "env" in k8s_submit_mock.call_args[1]
    merged_env = k8s_submit_mock.call_args[1]["env"]

    # Config var should be present
    assert merged_env["CONFIG_VAR"] == "from_config"
    # Task var should be present
    assert merged_env["TASK_VAR"] == "from_task"
    # Override var should have task value (task overrides config)
    assert merged_env["OVERRIDE_VAR"] == "task_value"

    executor.stop()


# ---------------------------------------------------------------------------
# Tests for _parse_completed_indexes
# ---------------------------------------------------------------------------


def test_parse_completed_indexes_empty():
    assert _parse_completed_indexes(None) == set()
    assert _parse_completed_indexes("") == set()


def test_parse_completed_indexes_single():
    assert _parse_completed_indexes("0") == {0}
    assert _parse_completed_indexes("5") == {5}


def test_parse_completed_indexes_list():
    assert _parse_completed_indexes("0,1,2") == {0, 1, 2}


def test_parse_completed_indexes_range():
    assert _parse_completed_indexes("0-5") == {0, 1, 2, 3, 4, 5}


def test_parse_completed_indexes_mixed():
    assert _parse_completed_indexes("0,2-4,7") == {0, 2, 3, 4, 7}


@mock_s3
def _make_executor():
    scheduler = mock_scheduler()
    return mock_executor(scheduler)


# ---------------------------------------------------------------------------
# Tests for _get_k8s_job_terminal_status
# ---------------------------------------------------------------------------


def _make_job(
    parallelism=1,
    completions=1,
    succeeded=None,
    failed=None,
    conditions=None,
):
    """Helper to build a V1Job with the given status fields."""
    job = create_job_object(name="test-job", uid="test-uid")
    job.spec.parallelism = parallelism
    job.spec.completions = completions
    job.status = client.V1JobStatus(
        succeeded=succeeded,
        failed=failed,
        conditions=conditions,
    )
    return job


class TestGetK8sJobTerminalStatus:
    @mock_k8s
    def test_singleton_succeeded(self):
        executor = _make_executor()
        job = _make_job(succeeded=1)
        status, reason = executor._get_k8s_job_terminal_status(job)
        assert status == SUCCEEDED

    @mock_k8s
    def test_singleton_failed(self):
        executor = _make_executor()
        job = _make_job(failed=1)
        status, reason = executor._get_k8s_job_terminal_status(job)
        assert status == FAILED

    @mock_k8s
    def test_singleton_running(self):
        executor = _make_executor()
        job = _make_job()
        status, reason = executor._get_k8s_job_terminal_status(job)
        assert status is None

    @mock_k8s
    def test_singleton_timeout(self):
        executor = _make_executor()
        job = _make_job(
            conditions=[client.V1JobCondition(type="Failed", status="True", message="timeout")]
        )
        status, reason = executor._get_k8s_job_terminal_status(job)
        assert status == FAILED
        assert reason == "Error: timeout"

    @mock_k8s
    def test_array_succeeded_counter_is_not_terminal(self):
        """For array jobs, succeeded > 0 does NOT mean the job is done."""
        executor = _make_executor()
        job = _make_job(parallelism=6, completions=6, succeeded=3)
        status, reason = executor._get_k8s_job_terminal_status(job)
        assert status is None

    @mock_k8s
    def test_array_complete_condition(self):
        executor = _make_executor()
        job = _make_job(
            parallelism=6,
            completions=6,
            succeeded=6,
            conditions=[client.V1JobCondition(type="Complete", status="True")],
        )
        status, reason = executor._get_k8s_job_terminal_status(job)
        assert status == SUCCEEDED

    @mock_k8s
    def test_array_failed_condition(self):
        executor = _make_executor()
        job = _make_job(
            parallelism=6,
            completions=6,
            succeeded=4,
            failed=2,
            conditions=[
                client.V1JobCondition(type="Failed", status="True", message="BackoffLimitExceeded")
            ],
        )
        status, reason = executor._get_k8s_job_terminal_status(job)
        assert status == FAILED
        assert "BackoffLimitExceeded" in reason

    @mock_k8s
    def test_array_partially_complete_no_conditions(self):
        """Array with some successes and some failures but no terminal condition yet."""
        executor = _make_executor()
        job = _make_job(parallelism=6, completions=6, succeeded=4, failed=1)
        status, reason = executor._get_k8s_job_terminal_status(job)
        assert status is None


# ---------------------------------------------------------------------------
# Tests for volumes and volume_mounts support
# ---------------------------------------------------------------------------


@mock_k8s
def test_k8s_submit_with_volumes() -> None:
    """
    k8s_submit should properly pass volumes and volume_mounts to the job.
    """
    k8s_client = redun.executors.k8s_utils.K8SClient()

    # Configure the mock to return the job object that was passed to it
    k8s_client.batch.create_namespaced_job = Mock(side_effect=lambda body, namespace: body)  # ty: ignore[invalid-assignment]

    volumes = [
        {"name": "dshm", "emptyDir": {"medium": "Memory", "sizeLimit": "16Gi"}},
    ]
    volume_mounts = [
        {"name": "dshm", "mountPath": "/dev/shm"},
    ]

    job = redun.executors.k8s.k8s_submit(
        k8s_client=k8s_client,
        command=["echo", "test"],
        image="test-image",
        namespace="default",
        job_name="test-job",
        volumes=volumes,
        volume_mounts=volume_mounts,
    )

    # Compare the serialized job, since newer kubernetes clients convert the dicts into models.
    pod_spec = client.ApiClient().sanitize_for_serialization(job)["spec"]["template"]["spec"]

    # Verify volumes are configured in the pod spec
    assert pod_spec["volumes"] == volumes

    # Verify volume mounts are configured in the container
    assert pod_spec["containers"][0]["volumeMounts"] == volume_mounts


@mock_s3
@mock_k8s
@patch("redun.executors.k8s.k8s_describe_jobs")
@patch("redun.executors.k8s.k8s_submit")
def test_executor_with_volumes(k8s_submit_mock: Mock, k8s_describe_jobs_mock: Mock) -> None:
    """
    Executor should pass volumes and volume_mounts from task options to k8s_submit.
    """
    # Setup K8S mocks.
    k8s_describe_jobs_mock.return_value = []

    scheduler = mock_scheduler()
    executor = mock_executor(scheduler)
    executor.start()

    k8s_submit_mock.return_value = create_job_object(
        name=DEFAULT_JOB_PREFIX + "-eval_hash",
        uid="k8s-job-id",
    )

    # Submit redun job with volumes and volume_mounts options
    volumes = [
        {"name": "dshm", "emptyDir": {"medium": "Memory", "sizeLimit": "16Gi"}},
    ]
    volume_mounts = [
        {"name": "dshm", "mountPath": "/dev/shm"},
    ]

    expr = task1.options(volumes=volumes, volume_mounts=volume_mounts)(10)
    job = Job(task1, expr)
    job.eval_hash = "eval_hash"
    job.args = ((10,), {})
    executor.submit(job)

    # Wait until job arrayer actually submits it
    wait_until(lambda: executor.arrayer.num_pending == 0)

    # Ensure volumes and volume_mounts were passed correctly
    assert k8s_submit_mock.call_args
    assert "volumes" in k8s_submit_mock.call_args[1]
    assert "volume_mounts" in k8s_submit_mock.call_args[1]
    assert k8s_submit_mock.call_args[1]["volumes"] == volumes
    assert k8s_submit_mock.call_args[1]["volume_mounts"] == volume_mounts

    executor.stop()


# ---------------------------------------------------------------------------
# Tests for resilient pod-log retrieval (COMP-3871)
# ---------------------------------------------------------------------------


def _make_log_pod(exit_code: int = 0) -> client.V1Pod:
    """Build a V1Pod with a terminated container status, as get_pod_logs expects."""
    return client.V1Pod(
        metadata=client.V1ObjectMeta(name="pod-1", namespace="default"),
        status=client.V1PodStatus(
            container_statuses=[
                client.V1ContainerStatus(
                    image="img",
                    image_id="id",
                    name="c",
                    ready=False,
                    restart_count=0,
                    state=client.V1ContainerState(
                        terminated=client.V1ContainerStateTerminated(exit_code=exit_code),
                    ),
                ),
            ],
        ),
    )


def _api_exception(status: int, body: str = "") -> ApiException:
    error = ApiException(status=status)
    error.body = body
    return error


@pytest.mark.parametrize(
    "error,expected",
    [
        (_api_exception(500), True),
        (_api_exception(503), True),
        (_api_exception(500, "remote error: tls: internal error"), True),
        (_api_exception(0), True),
        (ProtocolError("connection reset"), True),
        (URLLib3TimeoutError("read timed out"), True),
        (ProxyError("cannot connect to proxy", OSError("connection refused")), True),
        (SSLError("handshake failure"), True),
        (
            MaxRetryError(
                HTTPSConnectionPool(host="h", port=443),
                "/url",
                reason=ProtocolError("x"),
            ),
            True,
        ),
        (_api_exception(404), False),
        (_api_exception(400), False),
        # A generic HTTPError or a deterministic urllib3 subclass (e.g. a
        # malformed response body) is not a transient transport failure and
        # must not be retried.
        (HTTPError("generic"), False),
        (DecodeError("bad response body"), False),
        (ValueError("not a transport error"), False),
    ],
)
def test_is_transient_log_fetch_error(error: BaseException, expected: bool) -> None:
    assert is_transient_log_fetch_error(error) is expected


def test_get_pod_logs_handles_pod_without_container_statuses() -> None:
    """A pod that failed before any container started has no container statuses."""
    k8s_client = Mock()
    k8s_client.core.read_namespaced_pod_log.return_value = "line1\nline2"
    pod = client.V1Pod(
        metadata=client.V1ObjectMeta(name="pod-1", namespace="default"),
        status=client.V1PodStatus(container_statuses=None),
    )

    lines = get_pod_logs(k8s_client, pod)

    assert lines == ["line1", "line2"]


def test_get_pod_logs_appends_exit_line_when_container_statuses_present() -> None:
    """A non-zero exit code on the last container status is appended as a log line."""
    k8s_client = Mock()
    k8s_client.core.read_namespaced_pod_log.return_value = "line1\nline2"
    pod = client.V1Pod(
        metadata=client.V1ObjectMeta(name="pod-1", namespace="default"),
        status=client.V1PodStatus(
            container_statuses=[
                client.V1ContainerStatus(
                    image="img",
                    image_id="id",
                    name="c",
                    ready=False,
                    restart_count=0,
                    state=client.V1ContainerState(
                        terminated=client.V1ContainerStateTerminated(
                            exit_code=1,
                            reason="Error",
                            message="boom",
                        ),
                    ),
                ),
            ],
        ),
    )

    lines = get_pod_logs(k8s_client, pod)

    assert lines == ["line1", "line2", "Exit 1 (Error): boom"]


@patch("redun.executors.k8s.time.sleep")
def test_get_pod_logs_retries_transient_then_succeeds(sleep_mock: Mock) -> None:
    """A transient 5xx is retried and, on eventual success, logs are returned."""
    k8s_client = Mock()
    k8s_client.core.read_namespaced_pod_log.side_effect = [
        _api_exception(500, "remote error: tls: internal error"),
        _api_exception(500, "remote error: tls: internal error"),
        "line1\nline2",
    ]

    lines = get_pod_logs(k8s_client, _make_log_pod(), max_retries=5, backoff=0.0)

    assert lines == ["line1", "line2"]
    assert k8s_client.core.read_namespaced_pod_log.call_count == 3
    assert sleep_mock.call_count == 2


@patch("redun.executors.k8s.time.sleep")
def test_get_pod_logs_does_not_retry_non_transient(sleep_mock: Mock) -> None:
    """A 404 (definitive "no such log") is raised immediately, without retry."""
    k8s_client = Mock()
    k8s_client.core.read_namespaced_pod_log.side_effect = _api_exception(404)

    with pytest.raises(ApiException):
        get_pod_logs(k8s_client, _make_log_pod(), max_retries=5, backoff=0.0)

    assert k8s_client.core.read_namespaced_pod_log.call_count == 1
    sleep_mock.assert_not_called()


@patch("redun.executors.k8s.random.random")
@patch("redun.executors.k8s.time.sleep")
def test_get_pod_logs_non_transient_error_does_not_spend_shared_budget(
    sleep_mock: Mock,
    random_mock: Mock,
) -> None:
    """
    A non-transient failure (e.g. a 404 for a genuinely missing log) must not
    spend any of the shared per-cycle retry budget, otherwise one pod with no
    log would reduce the retry runway available to every other pod sharing
    the budget this cycle. ``backoff`` is deliberately non-zero here (unlike
    ``test_get_pod_logs_does_not_retry_non_transient``) so that, were the
    transient-error guard ever bypassed for a non-transient error, the
    resulting non-zero delay would actually be deducted from ``budget`` and
    this test would catch it even if call-count/sleep assertions did not.
    """
    random_mock.return_value = 1.0  # Deterministic max-jitter backoff.
    k8s_client = Mock()
    k8s_client.core.read_namespaced_pod_log.side_effect = _api_exception(404)
    budget = LogFetchRetryBudget(DEFAULT_LOG_FETCH_MAX_BACKOFF_TOTAL)

    with pytest.raises(ApiException):
        get_pod_logs(k8s_client, _make_log_pod(), max_retries=5, backoff=1.0, budget=budget)

    assert budget.remaining == DEFAULT_LOG_FETCH_MAX_BACKOFF_TOTAL
    assert k8s_client.core.read_namespaced_pod_log.call_count == 1
    sleep_mock.assert_not_called()


@patch("redun.executors.k8s.logger")
@patch("redun.executors.k8s.time.sleep")
def test_get_pod_logs_reraises_after_exhausting_retries(
    sleep_mock: Mock, logger_mock: Mock
) -> None:
    """After max_retries transient failures, the last error propagates."""
    k8s_client = Mock()
    k8s_client.core.read_namespaced_pod_log.side_effect = _api_exception(503)

    with pytest.raises(ApiException):
        get_pod_logs(k8s_client, _make_log_pod(), max_retries=2, backoff=0.0)

    # 1 initial attempt + 2 retries.
    assert k8s_client.core.read_namespaced_pod_log.call_count == 3
    assert sleep_mock.call_count == 2

    # The give-up event is logged once retries are exhausted, in the same
    # place as the retry attempts.
    give_up_messages = [
        call.args[0]
        for call in logger_mock.warning.call_args_list
        if call.args[0].startswith("Exhausted")
    ]
    assert len(give_up_messages) == 1


@patch("redun.executors.k8s.random.random")
@patch("redun.executors.k8s.time.sleep")
def test_get_pod_logs_shared_budget_bounds_cumulative_retries_across_flaky_pods(
    sleep_mock: Mock,
    random_mock: Mock,
) -> None:
    """
    A partially degraded node doesn't necessarily fail every log fetch
    outright - each pod's fetch may fail a few times before recovering. A
    retry-backoff budget shared across pods (as ``K8SExecutor`` shares one
    across all pods processed within a monitor cycle) must bound the
    *cumulative* retry time paid across all of them, not just cap retries
    per pod: once earlier pods have spent most of the shared budget, a
    later pod gets less runway even on the exact same transient error.
    """
    random_mock.return_value = 1.0  # Deterministic max-jitter backoff.
    budget = LogFetchRetryBudget(DEFAULT_LOG_FETCH_MAX_BACKOFF_TOTAL)  # 15.0

    # Pods A and B: each fails 3 times (1s + 2s + 4s = 7s of backoff)
    # then recovers on the 4th attempt, well within the shared budget.
    for _ in range(2):
        k8s_client = Mock()
        k8s_client.core.read_namespaced_pod_log.side_effect = [
            _api_exception(503),
            _api_exception(503),
            _api_exception(503),
            "line1",
        ]
        lines = get_pod_logs(k8s_client, _make_log_pod(), budget=budget)
        assert lines == ["line1"]

    # 2 pods x 7s of cumulative backoff already spent this cycle.
    assert sum(call.args[0] for call in sleep_mock.call_args_list) == 14.0

    # Pod C hits the exact same transient error, but the shared budget is
    # nearly spent: it only gets 1 retry (spending the last 1s of budget)
    # before giving up, instead of the 3 retries A and B got.
    sleep_mock.reset_mock()
    k8s_client = Mock()
    k8s_client.core.read_namespaced_pod_log.side_effect = _api_exception(503)
    with pytest.raises(ApiException):
        get_pod_logs(k8s_client, _make_log_pod(), budget=budget)

    assert k8s_client.core.read_namespaced_pod_log.call_count == 2
    assert sum(call.args[0] for call in sleep_mock.call_args_list) == 1.0
    assert budget.remaining == 0.0


@patch("redun.executors.k8s.random.random")
@patch("redun.executors.k8s.time.sleep")
def test_get_pod_logs_truncates_delay_to_remaining_budget_instead_of_refusing(
    sleep_mock: Mock,
    random_mock: Mock,
) -> None:
    """
    A retry whose requested delay exceeds what remains in the shared budget
    must still be granted a truncated sleep for whatever *does* remain,
    rather than being refused outright. Refusing the request while budget
    remains would both abandon that unspent runway and make a subsequently
    granted, smaller request from another pod contradict an "exhausted"
    warning that hasn't actually fired yet.
    """
    random_mock.return_value = 1.0  # Deterministic max-jitter backoff.
    # Covers attempt 0's 1.0s delay in full, but only part of attempt 1's 2.0s
    # delay.
    budget = LogFetchRetryBudget(1.5)
    k8s_client = Mock()
    k8s_client.core.read_namespaced_pod_log.side_effect = [
        _api_exception(503),
        _api_exception(503),
        "line1",
    ]

    lines = get_pod_logs(k8s_client, _make_log_pod(), backoff=1.0, budget=budget)

    assert lines == ["line1"]
    # The second sleep is truncated to the 0.5s actually remaining, not the
    # 2.0s requested, and the fetch still proceeds to a successful retry
    # instead of giving up.
    assert [call.args[0] for call in sleep_mock.call_args_list] == [1.0, 0.5]
    assert budget.remaining == 0.0


@patch("redun.executors.k8s.random.random")
@patch("redun.executors.k8s.logger")
@patch("redun.executors.k8s.time.sleep")
def test_get_pod_logs_logs_budget_exhaustion_once_across_pods(
    sleep_mock: Mock,
    logger_mock: Mock,
    random_mock: Mock,
) -> None:
    """
    A depleted shared budget is a single per-cycle fact, not a per-pod one: an
    array job where many pods each hit a transient error after the budget is
    already spent must log "budget exhausted" once total for the cycle, not
    once per affected pod.
    """
    random_mock.return_value = 1.0  # Deterministic max-jitter backoff.
    budget = LogFetchRetryBudget(0.0)  # Already exhausted.

    for _ in range(2):
        k8s_client = Mock()
        k8s_client.core.read_namespaced_pod_log.side_effect = _api_exception(503)
        with pytest.raises(ApiException):
            get_pod_logs(k8s_client, _make_log_pod(), budget=budget)

    exhausted_messages = [
        call.args[0]
        for call in logger_mock.warning.call_args_list
        if call.args[0].startswith("Log-fetch retry budget for this monitor cycle is exhausted")
    ]
    assert len(exhausted_messages) == 1


@patch("redun.executors.k8s.logger")
@patch("redun.executors.k8s.time.sleep")
def test_get_pod_logs_zero_backoff_against_full_budget_retries_without_exhaustion_warning(
    sleep_mock: Mock,
    logger_mock: Mock,
) -> None:
    """
    A zero-length requested delay (e.g. from ``backoff=0.0``) against a
    non-exhausted budget is a legitimate no-op grant, not evidence the pool
    is spent. ``get_pod_logs`` must still retry normally instead of giving
    up, and must not log the once-per-cycle exhaustion warning: doing so
    would both burn the ``warned`` flag (silencing the real exhaustion
    later in the cycle) and abort a retry that should have proceeded.
    """
    budget = LogFetchRetryBudget(DEFAULT_LOG_FETCH_MAX_BACKOFF_TOTAL)
    k8s_client = Mock()
    k8s_client.core.read_namespaced_pod_log.side_effect = [
        _api_exception(503),
        _api_exception(503),
        "line1",
    ]

    lines = get_pod_logs(k8s_client, _make_log_pod(), backoff=0.0, budget=budget)

    assert lines == ["line1"]
    exhausted_messages = [
        call.args[0]
        for call in logger_mock.warning.call_args_list
        if call.args[0].startswith("Log-fetch retry budget for this monitor cycle is exhausted")
    ]
    assert exhausted_messages == []
    # Zero-length requests never draw down the pool.
    assert budget.remaining == DEFAULT_LOG_FETCH_MAX_BACKOFF_TOTAL


@patch("redun.executors.k8s.logger")
def test_log_fetch_retry_budget_consume_zero_against_exhausted_budget_returns_none_and_warns_once(
    logger_mock: Mock,
) -> None:
    """
    Exhaustion is a one-way per-cycle fact determined by the pool's own
    state, not by whether this particular request came out to zero: unlike
    the mirrored case against a non-exhausted budget
    (``test_get_pod_logs_zero_backoff_against_full_budget_retries_without_exhaustion_warning``),
    a zero-length request against an *already-exhausted* budget must still be
    refused like any other request -- returning ``None`` -- and must still
    log the "budget exhausted" warning exactly once, not skip it just
    because the caller asked for 0.0 seconds.
    """
    budget = LogFetchRetryBudget(0.0)  # Already exhausted.

    granted = budget.consume(0.0)

    assert granted is None
    # A second zero-length request against the same spent pool is refused the
    # same way, and doesn't re-log: the zero-length path participates in the
    # once-per-cycle suppression rather than bypassing the `warned` flag.
    assert budget.consume(0.0) is None
    exhausted_messages = [
        call.args[0]
        for call in logger_mock.warning.call_args_list
        if call.args[0].startswith("Log-fetch retry budget for this monitor cycle is exhausted")
    ]
    assert len(exhausted_messages) == 1


@mock_s3
@mock_k8s
@patch("redun.executors.k8s.get_pod_logs")
def test_process_redun_job_shares_log_fetch_budget_across_pods_in_a_cycle(
    get_pod_logs_mock: Mock,
) -> None:
    """
    All pods processed within the same monitor cycle share one
    ``LogFetchRetryBudget`` instance: ``_monitor`` constructs it once at the
    top of the cycle, and every pod processed in that cycle reuses that same
    object rather than getting a fresh per-pod budget. A new monitor cycle
    constructs a fresh ``LogFetchRetryBudget``, discarding the old one.
    """
    scheduler = mock_scheduler()
    executor = mock_executor(scheduler)
    get_pod_logs_mock.side_effect = _api_exception(503)

    def make_job(index: int) -> Job:
        job = Job(task1, task1(index))
        job.id = f"task-{index}"
        job.eval_hash = f"hash-{index}"
        return job

    # Simulate the top of a monitor cycle, as `_monitor` does.
    executor._log_fetch_budget = LogFetchRetryBudget()
    first_budget = executor._log_fetch_budget

    executor._process_redun_job(make_job(0), _make_log_pod(), FAILED, "node bad")
    assert get_pod_logs_mock.call_args_list[0].kwargs["budget"] is first_budget

    # Second pod in the *same* cycle: reuses the same budget instance.
    executor._process_redun_job(make_job(1), _make_log_pod(), FAILED, "node bad")
    assert get_pod_logs_mock.call_args_list[1].kwargs["budget"] is first_budget

    # A new monitor cycle constructs a fresh budget (as `_monitor` does), so
    # the next pod gets a different instance.
    executor._log_fetch_budget = LogFetchRetryBudget()
    second_budget = executor._log_fetch_budget
    assert second_budget is not first_budget

    executor._process_redun_job(make_job(2), _make_log_pod(), FAILED, "node bad")
    assert get_pod_logs_mock.call_args_list[2].kwargs["budget"] is second_budget


@mock_s3
@mock_k8s
@patch("redun.executors.k8s.k8s_describe_jobs")
@patch("redun.executors.k8s.k8s_submit")
def test_monitor_constructs_log_fetch_budget(
    k8s_submit_mock: Mock,
    k8s_describe_jobs_mock: Mock,
) -> None:
    """
    A real ``_monitor`` cycle (as started by ``executor.submit()``) must wire
    up ``self._log_fetch_budget`` itself. Unlike
    ``test_process_redun_job_shares_log_fetch_budget_across_pods_in_a_cycle``,
    which hand-assigns ``executor._log_fetch_budget`` to test sharing
    semantics, this test never touches ``_log_fetch_budget`` directly, so it
    pins the ``self._log_fetch_budget = LogFetchRetryBudget()`` assignment
    inside ``_monitor``.
    """
    k8s_describe_jobs_mock.return_value = []

    scheduler = mock_scheduler()
    executor = mock_executor(scheduler)
    assert executor._log_fetch_budget is None

    executor.start()

    k8s_submit_mock.return_value = create_job_object(
        name=DEFAULT_JOB_PREFIX + "-eval_hash",
        uid="k8s-job-id",
    )

    job = Job(task1, task1(10))
    job.eval_hash = "eval_hash"
    job.args = ((10,), {})
    # `_submit` calls `arrayer.add_job(job)` before `self._start()`, so the
    # queue is already non-empty when the monitor thread starts: its loop
    # condition (`pending_k8s_jobs or arrayer.num_pending`) is guaranteed
    # true on the first check, so it can't exit before running a cycle.
    executor.submit(job)

    # Wait for a real `_monitor` cycle to run, constructing the budget.
    wait_until(lambda: executor._log_fetch_budget is not None)
    first_budget = executor._log_fetch_budget

    # A *fresh* budget each cycle, not a lazily-initialized or
    # refresh-only-when-spent one: the next cycle must hand out a new
    # instance regardless of how much of this one was spent.
    assert first_budget is not None
    wait_until(lambda: executor._log_fetch_budget is not first_budget)

    executor.stop()
