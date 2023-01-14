import json
import os
import pickle
from functools import wraps
from typing import Callable, cast
from unittest.mock import Mock, patch

import boto3
import pytest
from kubernetes import client

import redun.executors.k8s
from redun import File, job_array, task
from redun.cli import RedunClient, import_script
from redun.config import Config
from redun.executors.code_packaging import create_tar, package_code
from redun.executors.command import REDUN_REQUIRED_VERSION
from redun.executors.k8s import K8SExecutor, get_hash_from_job_name, get_k8s_job_name, submit_task
from redun.executors.k8s_utils import DEFAULT_JOB_PREFIX, K8SClient, create_job_object
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
        with patch.object(K8SClient, "core"), patch.object(K8SClient, "batch"), patch.object(
            K8SClient, "version", return_value=(1, 23)
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
            "k8s": {
                "image": image,
                "scratch": scratch_prefix,
                "job_monitor_interval": 0.05,
                "job_stale_time": 0.01,
                "code_package": code_package,
            }
        }
    )
    executor = K8SExecutor("k8s", scheduler, config["k8s"])

    executor.get_jobs = Mock()  # type: ignore
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
    config = Config(
        {
            "k8s": {
                "image": "image",
                "scratch": "scratch_prefix",
                "code_includes": "*.txt",
            }
        }
    )
    executor = K8SExecutor("k8s", scheduler, config["k8s"])

    assert executor.image == "image"
    assert executor.scratch_prefix == "scratch_prefix"
    assert isinstance(executor.code_package, dict)
    assert executor.code_package["includes"] == ["*.txt"]


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
    """
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
    """
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
    assert resp["jobId"] == "k8s-job-id"

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
    k8s_describe_jobs_mock.return_value = iter([])
    get_k8s_job_pods_mock.return_value = iter([])

    scheduler = mock_scheduler()
    executor = mock_executor(scheduler)
    executor.start()

    k8s_submit_mock.return_value = create_job_object(
        name=DEFAULT_JOB_PREFIX + "-eval_hash", uid=k8s_job_id
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
        "retries": 1,
        "service_account_name": "default",
        "secret_name": None,
        "vcpus": 1,
        "memory": 4,
        "k8s_labels": {
            "redun_execution_id": "",
            "redun_job_id": job.id,
            "redun_project": "",
            "redun_task_name": "task1",
        },
    }

    # Setup new mock return values.
    k8s_submit_mock.return_value = create_job_object(
        name=DEFAULT_JOB_PREFIX + "-eval_hash2", uid=k8s_job2_id
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
        "retries": 1,
        "service_account_name": "default",
        "secret_name": None,
        "vcpus": 1,
        "memory": 8,
        "k8s_labels": {
            "redun_execution_id": "",
            "redun_job_id": job2.id,
            "redun_project": "",
            "redun_task_name": "task1",
        },
    }

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
    k8s_describe_jobs_mock.return_value = [fake_k8s_job, fake_k8s_job2]

    pod = client.V1Pod(
        metadata=client.V1ObjectMeta(
            name="name",
            namespace="default",
        ),
        status=client.V1ContainerStatus(
            image=client.V1ContainerImage(),
            image_id="image_id",
            name="name",
            ready=True,
            restart_count=1,
        ),
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
    executor.get_jobs.return_value = [  # type: ignore
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
@patch("redun.executors.k8s.package_code")
def test_code_packaging(package_code_mock: Mock) -> None:
    """
    Ensure that code packaging only happens on first submission.
    """
    package_code_mock.return_value = "s3://fake-bucket/code.tar.gz"

    scheduler = mock_scheduler()
    executor = mock_executor(scheduler, code_package=True)
    executor.start()

    # Starting the executor should not have triggered code packaging.
    assert executor.code_file is None
    assert package_code_mock.call_count == 0

    # Hand create jobs.
    job1 = Job(task1, task1(10))
    job1.id = "1"
    job1.eval_hash = "eval_hash"
    job1.args = ((10,), {})

    job2 = Job(task1, task1(20))
    job2.id = "2"
    job2.eval_hash = "eval_hash"
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
    assert executor.get_jobs.call_count == 1  # type: ignore

    # # Submit the second job and confirm that job reuniting was not done again.
    executor.submit(job2)
    assert executor.get_jobs.call_count == 1  # type: ignore

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
    executor.get_jobs.return_value = [k8s_job]  # type: ignore
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

    redun.executors.k8s.submit_task.side_effect = [  # type: ignore
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
        }
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
            "k8s": {
                "image": "image",
                "scratch": "scratch_prefix",
                "code_includes": "*.txt",
                "min_array_size": 0,
            }
        }
    )
    scheduler = mock_scheduler()

    executor = K8SExecutor("k8s", scheduler, config["k8s"])
    executor.get_jobs = Mock()  # type: ignore
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
        """
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
            ]
        )

        # Check output files are there
        output_file = File(
            get_job_scratch_file(
                executor.scratch_prefix,
                test_jobs[i],
                SCRATCH_OUTPUT,
            )
        )

        assert pickle.loads(cast(bytes, output_file.read("rb"))) == i - 2 * i
