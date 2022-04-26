"""
Redun Executor class for running tasks and scripts in a local conda environment.
"""
import os
import pickle
import subprocess
from concurrent.futures import ThreadPoolExecutor
from shlex import quote
from tempfile import TemporaryDirectory
from typing import Any, Dict, List, Optional, Tuple

from redun.config import create_config_section
from redun.executors import aws_utils
from redun.executors.base import Executor, register_executor
from redun.scheduler import Job, Scheduler, Traceback
from redun.scripting import ScriptError, get_task_command
from redun.utils import get_import_paths, pickle_dump


@register_executor("conda")
class CondaExecutor(Executor):
    """
    Executor that runs tasks and scripts in a local conda environment.
    """

    def __init__(self, name: str, scheduler: Scheduler = None, config=None):
        super().__init__(name, scheduler=scheduler)

        # Parse config.
        if not config:
            config = create_config_section()

        self.max_workers = config.getint("max_workers", 20)
        self.default_env = config.get("conda_environment")

        self._thread_executor: Optional[ThreadPoolExecutor] = None

    def _start(self) -> None:
        """
        Start pool on first Job submission.
        """
        if not self._thread_executor:
            self._thread_executor = ThreadPoolExecutor(max_workers=self.max_workers)

    def stop(self) -> None:
        """
        Stop Executor pools.
        """
        if self._thread_executor:
            self._thread_executor.shutdown()
            self._thread_executor = None

    def _submit(self, job: Job, args: Tuple, kwargs: dict) -> None:
        # Ensure pool are started.
        self._start()

        # Run job in a new thread or process.
        assert self._thread_executor
        assert job.task

        def on_done(future):
            success, result = future.result()
            if success:
                self.scheduler.done_job(job, result)
            else:
                error, traceback = result
                self.scheduler.reject_job(job, error, traceback)

        self._thread_executor.submit(
            execute, job, self.get_job_env(job), args, kwargs
        ).add_done_callback(on_done)

    def submit(self, job: Job, args: Tuple, kwargs: dict) -> None:
        assert job.task
        assert not job.task.script
        self._submit(job, args, kwargs)

    def submit_script(self, job: Job, args: Tuple, kwargs: dict) -> None:
        assert job.task
        assert job.task.script
        self._submit(job, args, kwargs)

    def get_job_env(self, job: Job) -> str:
        """
        Return the conda environment name for the given job.
        """
        result = job.get_option("conda", self.default_env)
        if result is None:
            raise RuntimeError('No conda environment name or default value provided.')
        return result

def execute(
    job: Job,
    env_name: str,
    args: Tuple = (),
    kwargs: Dict[str, Any] = None,
) -> Tuple[bool, Any]:
    """
    Run a job in a local conda environment.
    """
    assert job.task

    if kwargs is None:
        kwargs = {}
    with TemporaryDirectory() as task_files_dir:
        input_path = os.path.join(task_files_dir, "input")
        output_path = os.path.join(task_files_dir, "output")
        error_path = os.path.join(task_files_dir, "error")

        command_path = os.path.join(task_files_dir, "command")
        command_output_path = os.path.join(task_files_dir, "command_output")
        command_error_path = os.path.join(task_files_dir, "command_error")

        if job.task.script:
            # Careful. This will execute the body of the task in the host environment.
            # The rest of redun all works like this so I'm not changing it here.
            inner_command = [get_task_command(job.task, args, kwargs)]
        else:
            # Serialize arguments to input file.
            with open(input_path, "wb") as f:
                pickle_dump([args, kwargs], f)
            inner_command = get_oneshot_command(job, input_path, output_path, error_path)

        command = wrap_command(
            inner_command, env_name, command_path, command_output_path, command_error_path
        )
        cmd_result = subprocess.run(command, check=False, capture_output=False)

        if not job.task.script:
            return handle_oneshot_output(output_path, error_path, command_error_path)
        else:
            return handle_script_output(
                cmd_result.returncode, command_output_path, command_error_path
            )


def get_oneshot_command(job: Job, input_path: str, output_path: str, error_path: str) -> List[str]:
    """
    Build up a shell command for executing a redun task using `redun oneshot`.
    """
    assert job.task

    # Determine additional python import paths.
    import_args = []
    base_path = os.getcwd()
    for abs_path in get_import_paths():
        # Use relative paths so that they work inside the docker container.
        rel_path = os.path.relpath(abs_path, base_path)
        import_args.append("--import-path")
        import_args.append(rel_path)

    # Build job command.
    cache_arg = [] if job.get_option("cache", True) else ["--no-cache"]
    command = (
        [
            aws_utils.REDUN_PROG,
            "--check-version",
            aws_utils.REDUN_REQUIRED_VERSION,
            "oneshot",
            job.task.load_module,
        ]
        + import_args
        + cache_arg
        + [
            "--input",
            input_path,
            "--output",
            output_path,
            "--error",
            error_path,
            job.task.fullname,
        ]
    )
    return command


def wrap_command(
    command: List[str],
    env_name: str,
    command_path: str,
    command_output_path: str,
    command_error_path: str,
) -> List[str]:
    """
    Given a bash command:
    1. Wrap it in a `conda run` command.
    2. Write it to a file.
    3. Generate and return a command to execute the file, teeing stdout and stderr to files.
    """
    conda_command = ["conda", "run", "--no-capture-output", "-n", env_name, *command]
    inner_cmd_str = " ".join(quote(token) for token in conda_command)
    with open(command_path, "wt") as cmd_f:
        cmd_f.write(inner_cmd_str)

    wrapped_command = [
        "bash",
        "-c",
        "-o",
        "pipefail",
        """
chmod +x {command_path}
. {command_path} 2> >(tee {command_error_path} >&2) | tee {command_output_path}
""".format(
            command_path=quote(command_path),
            command_output_path=quote(command_output_path),
            command_error_path=quote(command_error_path),
        ),
    ]
    return wrapped_command


def handle_oneshot_output(
    output_path: str, error_path: str, command_error_path: str
) -> Tuple[bool, Any]:
    """
    Handle output of a oneshot command.
    """
    if os.path.exists(output_path):
        with open(output_path, "rb") as f:
            result = pickle.load(f)
        return True, result
    elif os.path.exists(error_path):
        with open(error_path, "rb") as f:
            error, error_traceback = pickle.load(f)
        return False, (error, error_traceback)
    else:
        # Cover the case where the oneshot command was not entered or it failed to start.
        # For conda this could happen if the specified environment name doesn't exist.
        with open(command_error_path, "rb") as f:
            error = ScriptError(f.read())
            error_traceback = Traceback.from_error(error)
        return False, (error, error_traceback)


def handle_script_output(
    return_code: int, command_output_path: str, command_error_path: str
) -> Tuple[bool, Any]:
    """
    Handle output of a script command.
    """
    if return_code == 0:
        with open(command_output_path, "rb") as f:
            result = f.read()
        return True, result
    else:
        with open(command_error_path, "rb") as f:
            error = ScriptError(f.read())
            error_traceback = Traceback.from_error(error)
        return False, (error, error_traceback)
