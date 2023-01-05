import boto3

from redun import File, task
from redun.executors.command import get_oneshot_command, get_script_task_command
from redun.scheduler import Job
from redun.tests.utils import mock_s3, use_tempdir


@task
def task1(x):
    return x


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
