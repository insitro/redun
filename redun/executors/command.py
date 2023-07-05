import os
from typing import Any, Dict, List, Optional, Tuple

from redun.executors.scratch import (
    SCRATCH_ERROR,
    SCRATCH_INPUT,
    SCRATCH_OUTPUT,
    SCRATCH_STATUS,
    get_array_scratch_file,
    get_job_scratch_file,
)
from redun.file import File, get_filesystem
from redun.scheduler import Job
from redun.task import CacheScope, Task
from redun.utils import get_import_paths, pickle_dump

REDUN_PROG = "redun"
REDUN_REQUIRED_VERSION = ">=0.4.1"


def get_oneshot_command(
    scratch_prefix: str,
    job: Job,
    a_task: Task,
    args: Tuple = (),
    kwargs: Dict[str, Any] = {},
    job_options: dict = {},
    code_file: Optional[File] = None,
    array_uuid: Optional[str] = None,
    input_path: Optional[str] = None,
    output_path: Optional[str] = None,
    error_path: Optional[str] = None,
) -> List[str]:
    """
    Returns a redun oneshot command for a Job.
    """
    if array_uuid:
        if not input_path:
            input_path = get_array_scratch_file(scratch_prefix, array_uuid, SCRATCH_INPUT)
        if not output_path:
            output_path = get_array_scratch_file(scratch_prefix, array_uuid, SCRATCH_OUTPUT)
        if not error_path:
            error_path = get_array_scratch_file(scratch_prefix, array_uuid, SCRATCH_ERROR)

        # Assume arguments are already serialized.
    else:
        if not input_path:
            input_path = get_job_scratch_file(scratch_prefix, job, SCRATCH_INPUT)
        if not output_path:
            output_path = get_job_scratch_file(scratch_prefix, job, SCRATCH_OUTPUT)
        if not error_path:
            error_path = get_job_scratch_file(scratch_prefix, job, SCRATCH_ERROR)

        # Serialize arguments to input file.
        # Array jobs set this up themselves.
        input_file = File(input_path)
        with input_file.open("wb") as out:
            pickle_dump([args, kwargs], out)

    # Determine additional python import paths.
    import_args = []
    base_path = os.getcwd()
    for abs_path in get_import_paths():
        # Use relative paths so that they work inside the docker container.
        rel_path = os.path.relpath(abs_path, base_path)
        import_args.append("--import-path")
        import_args.append(rel_path)

    # Build job command.
    code_arg = ["--code", code_file.path] if code_file else []
    array_arg = ["--array-job"] if array_uuid else []
    cache_arg = (
        []
        if CacheScope(job_options.get("cache_scope", CacheScope.BACKEND)) == CacheScope.BACKEND
        else ["--no-cache"]
    )
    command = (
        [
            REDUN_PROG,
            "--check-version",
            REDUN_REQUIRED_VERSION,
            "oneshot",
            a_task.load_module,
        ]
        + import_args
        + code_arg
        + array_arg
        + cache_arg
        + [
            "--input",
            input_path,
            "--output",
            output_path,
            "--error",
            error_path,
            a_task.fullname,
        ]
    )
    return command


def get_script_task_command(
    scratch_prefix: str,
    job: Job,
    command: str,
    exit_command: str = "",
    as_mount: bool = False,
) -> List[str]:
    """
    Returns a shell script to run a script task.
    """
    input_path = get_job_scratch_file(scratch_prefix, job, SCRATCH_INPUT)
    output_path = get_job_scratch_file(scratch_prefix, job, SCRATCH_OUTPUT)
    error_path = get_job_scratch_file(scratch_prefix, job, SCRATCH_ERROR)
    status_path = get_job_scratch_file(scratch_prefix, job, SCRATCH_STATUS)

    # Serialize arguments to input file.
    File(input_path).write(command)

    input_stage = File(input_path).stage(".task_command").render_stage(as_mount)
    output_unstage = File(output_path).stage(".task_output").render_unstage(as_mount)
    error_unstage = File(error_path).stage(".task_error").render_unstage(as_mount)
    status_unstage = get_filesystem(url=status_path).shell_copy(
        None, status_path, as_mount=as_mount
    )

    return [
        "bash",
        "-c",
        "-o",
        "pipefail",
        f"""
{input_stage}
chmod +x .task_command
(
  ./.task_command \
  2> >(tee .task_error >&2) | tee .task_output
) && (
    {output_unstage}
    {error_unstage}
    echo ok | {status_unstage}
) || (
    [ -f .task_output ] && {output_unstage}
    [ -f .task_error ] && {error_unstage}
    echo fail | {status_unstage}
    {exit_command}
)
""",
    ]
