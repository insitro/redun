import os
import subprocess

import boto3

from redun import File, task
from redun.executors.command import (
    get_oneshot_command,
    get_script_task_command,
)
from redun.job_array import AWS_ARRAY_VAR
from redun.scheduler import Job
from redun.scripting import get_task_command
from redun.tests.utils import mock_s3, use_tempdir


@task
def task1(x):
    return x


@task(script=True)
def script_task1(x):
    # The last child in the tests below fails.
    return f"echo out {x}; echo err {x} >&2; [ {x} -ne 2 ]"


def get_array_script_jobs(n: int) -> tuple[list[Job], list[str]]:
    """
    Returns `n` script task jobs and their commands, as an array job would receive them.
    """
    jobs = []
    for i in range(n):
        job = Job(script_task1, script_task1(i))
        job.eval_hash = f"eval_hash_{i}"
        jobs.append(job)
    commands = [get_task_command(script_task1, (i,), {}) for i in range(n)]
    return jobs, commands


def run_array_child(command: list[str], index: int, cwd: str) -> subprocess.CompletedProcess:
    """
    Runs an array job command as the child with the given array index.
    """
    os.makedirs(cwd, exist_ok=True)
    return subprocess.run(
        command,
        cwd=cwd,
        env={**os.environ, AWS_ARRAY_VAR: str(index)},
        capture_output=True,
        text=True,
    )


@use_tempdir
def test_get_oneshot_command() -> None:
    """
    Should be able to generate a shell command for redun oneshot.
    """

    expr = task1(10)
    job = Job(task1, expr)
    job.eval_hash = "eval_hash"
    code_file = File("scratch/code.tar.gz")

    command = get_oneshot_command(
        "scratch",
        job,
        task1,
        (10,),
        {},
        {},
        code_file=code_file,
    )
    assert command == [
        "redun",
        "--check-version",
        ">=0.4.1",
        "oneshot",
        "redun.tests.test_executor_command",
        "--code",
        "scratch/code.tar.gz",
        "--input",
        "scratch/jobs/eval_hash/input",
        "--output",
        "scratch/jobs/eval_hash/output",
        "--error",
        "scratch/jobs/eval_hash/error",
        "task1",
    ]


@use_tempdir
def test_get_script_task_command_local() -> None:
    """
    Should be able to generate a shell command for a redun script task.
    """
    expr = task1(10)
    job = Job(task1, expr)
    job.eval_hash = "eval_hash"

    command = get_script_task_command("scratch", job, "myprog -x 1 input.txt")
    assert command == [
        "bash",
        "-c",
        "-o",
        "pipefail",
        "\n"
        "cp scratch/jobs/eval_hash/input .task_command\n"
        "chmod +x .task_command\n"
        "(\n"
        "  ./.task_command   2> >(tee .task_error >&2) | tee .task_output\n"
        ") && (\n"
        "    cp .task_output scratch/jobs/eval_hash/output\n"
        "    cp .task_error scratch/jobs/eval_hash/error\n"
        "    echo ok | cat > scratch/jobs/eval_hash/status\n"
        ") || (\n"
        "    [ -f .task_output ] && cp .task_output scratch/jobs/eval_hash/output\n"
        "    [ -f .task_error ] && cp .task_error scratch/jobs/eval_hash/error\n"
        "    echo fail | cat > scratch/jobs/eval_hash/status\n"
        "    \n"
        ")\n",
    ]


@use_tempdir
@mock_s3
def test_get_script_task_command_s3() -> None:
    """
    Should be able to generate a shell command for a redun script task.
    """
    s3_client = boto3.client("s3", region_name="us-east-1")
    s3_client.create_bucket(Bucket="bucket")

    expr = task1(10)
    job = Job(task1, expr)
    job.eval_hash = "eval_hash"

    command = get_script_task_command("s3://bucket/scratch", job, "myprog -x 1 input.txt")
    assert command == [
        "bash",
        "-c",
        "-o",
        "pipefail",
        "\n"
        "aws s3 cp --no-progress s3://bucket/scratch/jobs/eval_hash/input .task_command\n"
        "chmod +x .task_command\n"
        "(\n"
        "  ./.task_command   2> >(tee .task_error >&2) | tee .task_output\n"
        ") && (\n"
        "    aws s3 cp --no-progress .task_output s3://bucket/scratch/jobs/eval_hash/output\n"
        "    aws s3 cp --no-progress .task_error s3://bucket/scratch/jobs/eval_hash/error\n"
        "    echo ok | aws s3 cp --no-progress - s3://bucket/scratch/jobs/eval_hash/status\n"
        ") || (\n"
        "    [ -f .task_output ] && aws s3 cp --no-progress .task_output "
        "s3://bucket/scratch/jobs/eval_hash/output\n"
        "    [ -f .task_error ] && aws s3 cp --no-progress .task_error "
        "s3://bucket/scratch/jobs/eval_hash/error\n"
        "    echo fail | aws s3 cp --no-progress - s3://bucket/scratch/jobs/eval_hash/status\n"
        "    \n"
        ")\n",
    ]


@use_tempdir
@mock_s3
def test_get_script_task_command_s3_array() -> None:
    """
    Should be able to generate a shell command for a redun script task with array jobs.
    """
    s3_client = boto3.client("s3", region_name="us-east-1")
    s3_client.create_bucket(Bucket="bucket")

    expr = task1(10)
    job = Job(task1, expr)
    job.eval_hash = "eval_hash"

    command = get_script_task_command(
        "s3://bucket/scratch",
        job,
        "myprog -x 1 input.txt",
        array_uuid="array_uuid",
        exit_command="exit 0",
    )
    assert command == [
        "bash",
        "-c",
        "-o",
        "pipefail",
        """
aws s3 cp --no-progress s3://bucket/scratch/array_jobs/array_uuid/input .task_commands.tar.gz
tar xOzf .task_commands.tar.gz "$AWS_BATCH_JOB_ARRAY_INDEX".sh > .task_command
REDUN_OUTPUT=$(tar xOzf .task_commands.tar.gz "$AWS_BATCH_JOB_ARRAY_INDEX".output.txt)
REDUN_ERROR=$(tar xOzf .task_commands.tar.gz "$AWS_BATCH_JOB_ARRAY_INDEX".error.txt)
REDUN_STATUS=$(tar xOzf .task_commands.tar.gz "$AWS_BATCH_JOB_ARRAY_INDEX".status.txt)
chmod +x .task_command
(
  ./.task_command   2> >(tee .task_error >&2) | tee .task_output
) && (
    aws s3 cp --no-progress .task_output "$REDUN_OUTPUT"
    aws s3 cp --no-progress .task_error "$REDUN_ERROR"
    echo ok | aws s3 cp --no-progress - "$REDUN_STATUS"
) || (
    [ -f .task_output ] && aws s3 cp --no-progress .task_output "$REDUN_OUTPUT"
    [ -f .task_error ] && aws s3 cp --no-progress .task_error "$REDUN_ERROR"
    echo fail | aws s3 cp --no-progress - "$REDUN_STATUS"
    exit 0
)
""",
    ]
