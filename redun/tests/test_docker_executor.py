from typing import cast
from unittest.mock import Mock, patch

from redun import File, task
from redun.config import Config
from redun.executors.docker import DockerExecutor
from redun.expression import TaskExpression
from redun.scheduler import Job, Scheduler, Traceback
from redun.tests.utils import use_tempdir
from redun.utils import pickle_dumps


@task
def task1(x: int) -> int:
    return x


@use_tempdir
@patch("redun.executors.docker.run_docker")
@patch("threading.Thread")
@patch("redun.executors.docker.iter_job_status")
@patch.object(Scheduler, "done_job")
@patch.object(Scheduler, "reject_job")
def test_executor_docker(
    reject_job_mock: Mock,
    done_job_mock: Mock,
    iter_job_status_mock: Mock,
    thread_mock: Mock,
    run_docker_mock: Mock,
    scheduler: Scheduler,
) -> None:
    """
    DockerExecutor should run jobs.

    In this test, we patch `threading.Thread` to prevent the monitor thread
    from starting and we call the monitor code ourselves. This removes the
    need to manage multiple threads during the test.
    """

    container_id = "container-1"
    run_docker_mock.return_value = container_id

    config = Config(
        {
            "docker": {
                "image": "my-image",
                "scratch": ".redun_scratch",
            }
        }
    )
    executor = DockerExecutor("docker", scheduler, config=config["docker"])

    # Create and submit a job.
    expr = cast(TaskExpression[int], task1(10))
    job = Job(task1, expr)
    job.eval_hash = "eval_hash"
    job.args = ((10,), {})
    executor.submit(job)

    # Ensure job options were passed correctly to docker.
    scratch_dir = executor._scratch_prefix
    assert executor._code_file
    assert run_docker_mock.call_args[0] == (
        [
            "redun",
            "--check-version",
            ">=0.4.1",
            "oneshot",
            "redun.tests.test_docker_executor",
            "--code",
            executor._code_file.path,
            "--input",
            f"{scratch_dir}/jobs/eval_hash/input",
            "--output",
            f"{scratch_dir}/jobs/eval_hash/output",
            "--error",
            f"{scratch_dir}/jobs/eval_hash/error",
            "task1",
        ],
    )
    assert run_docker_mock.call_args[1] == {
        "image": "my-image",
        "gpus": 0,
        "memory": 4,
        "shared_memory": None,
        "vcpus": 1,
        "volumes": [(scratch_dir, scratch_dir)],
        "interactive": False,
        "include_aws_env": True,
    }

    # Simulate output file created by job.
    output_file = File(f"{scratch_dir}/jobs/eval_hash/output")
    output_file.write(pickle_dumps(task1.func(10)), mode="wb")

    # Simulate container succeeding.
    iter_job_status_mock.return_value = [
        {"jobId": container_id, "status": "SUCCEEDED", "logs": ""},
    ]

    # Manually run monitor logic.
    executor._monitor()

    # Ensure job returns result to scheduler.
    scheduler.done_job.assert_called_with(job, 10)  # type: ignore

    container2_id = "container-2"
    run_docker_mock.return_value = container2_id

    # Create and submit a job.
    expr = cast(TaskExpression[int], task1(11))
    job = Job(task1, expr)
    job.eval_hash = "eval_hash2"
    job.args = ((11,), {})
    executor.submit(job)

    # Simulate the container job failing.
    error = ValueError("Boom")
    error_traceback = Traceback.from_error(error)
    error_file = File(f"{scratch_dir}/jobs/eval_hash2/error")
    error_file.write(pickle_dumps((error, error_traceback)), mode="wb")

    iter_job_status_mock.return_value = [
        {"jobId": container2_id, "status": "FAILED", "logs": ""},
    ]

    # Manually run monitor thread.
    executor._monitor()

    # Ensure job rejected to scheduler.
    scheduler.reject_job.call_args[:2] == (job, error)  # type: ignore
