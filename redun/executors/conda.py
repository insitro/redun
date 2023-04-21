"""
Redun Executor class for running tasks and scripts in a local conda environment.
"""
import hashlib
import logging
import os
import pickle
import shutil
import subprocess
import tempfile
from concurrent.futures import ThreadPoolExecutor
from configparser import SectionProxy
from shlex import quote
from tempfile import mkdtemp
import time
from typing import Any, Dict, List, Optional, Tuple
from pathlib import Path

from redun.config import create_config_section
from redun.executors.base import Executor, register_executor
from redun.executors.command import get_oneshot_command
from redun.scheduler import Job, Scheduler, Traceback
from redun.scripting import ScriptError, prepare_command
from redun.task import Task

CONDA_ENV_CREATION_TIMEOUT = 5 * 60  # 5 minutes

class CondaEnvironment:
    """
    A class for managing a Conda environment.
    Creates a new environment if it does not exist, otherwise reuses an existing environment.
    """
    def __init__(
        self,
        env_name: Optional[str] = None,
        env_file: Optional[str] = None,
        env_dir: Optional[str] = None,
        pip_requirements_files: Optional[List[str]] = None,
        output_dir: Optional[str] = None,
    ):
        if not any([env_name, env_file, env_dir]):
            raise ValueError("At least one of env_name, env_file or env_dir must be specified.")

        self.env_name = env_name
        self.env_file = env_file
        self.env_dir = env_dir
        if self.env_dir and not os.path.isabs(self.env_dir):
            raise ValueError(f"env_dir must be an absolute path. Got: {self.env_dir}")
        self.output_dir = output_dir

        self.pip_requirements_files = pip_requirements_files
        self._ensure_env_exists()

    def __repr__(self):
        return f"CondaEnvironment(env_name={self.env_name}, env_file={self.env_file}, env_dir={self.env_dir}, output_dir={self.output_dir})"

    def _run_command(self, command: List[str], capture_output: bool) -> Tuple[int, str, str]:
        result = subprocess.run(command, capture_output=capture_output, text=True)
        return result.returncode, result.stdout, result.stderr

    def _ensure_env_exists(self):
        env_list_code, env_list_output, _ = self._run_command(["conda", "env", "list"], capture_output=True)
        if env_list_code != 0:
            raise RuntimeError("Failed to list Conda environments.")
        if self.env_file:
            # Calculate the hash of the environment file
            env_hash = self._hash_file(self.env_file, self.pip_requirements_files)
            env_path = os.path.abspath(self.env_file)
            if self.output_dir is None:
                self.output_dir = tempfile.mkdtemp(prefix="redun_conda_envs_")
            env_output_dir = os.path.join(self.output_dir, env_hash)
            # check if the environment already exists
            if os.path.exists(os.path.join(env_output_dir, "redun_initialized")):
                self.env_dir = os.path.join(env_output_dir, ".conda")
                return

            if os.path.exists(env_output_dir):
                # TODO: this can happen if the environment is currently being created, need to decide how to handle this
                # for now wait for the environment to be created
                for _ in range(CONDA_ENV_CREATION_TIMEOUT):
                    if os.path.exists(os.path.join(env_output_dir, "redun_initialized")):
                        self.env_dir = os.path.join(env_output_dir, ".conda")
                        return
                    time.sleep(1)
                raise RuntimeError(
                    f"Conda environment directory `{env_output_dir}` already exists, but is not a valid environment. Consider deleting it and trying again."
                )
            else:
                os.makedirs(env_output_dir)

            # Create the environment
            # either we have conda environment files or conda lock files
            if env_path.endswith(".yml") or env_path.endswith(".yaml"):
                create_env_code, _, create_env_error = self._run_command(
                    ["conda", "env", "create", "-f", env_path, "-p", os.path.join(env_output_dir, ".conda")],
                    capture_output=True
                )
                shutil.copy(env_path, os.path.join(env_output_dir, "environment.yml"))
            elif env_path.endswith(".lock"):
                create_env_code, _, create_env_error = self._run_command(
                    ["conda", "create", "--file", env_path, "-p", os.path.join(env_output_dir, ".conda")],
                    capture_output=True
                )
                shutil.copy(env_path, os.path.join(env_output_dir, "environment.lock"))

            if create_env_code != 0:
                # environment creation failed - remove it
                shutil.rmtree(env_output_dir)
                raise RuntimeError(
                    f"Failed to create Conda environment `{env_output_dir}` from file: {create_env_error}"
                )
            
            self.env_dir = os.path.join(env_output_dir, ".conda")

            # install extra pip requirements, if specified
            if self.pip_requirements_files:
                for pip_req_file in self.pip_requirements_files:
                    pip_install_command = ["pip", "install", "-r", pip_req_file]
                    if self.env_name:
                        pip_install_command = ["conda", "run", "-n", self.env_name, "--no-capture-output"] + pip_install_command
                    elif self.env_dir:
                        pip_install_command = ["conda", "run", "-p", self.env_dir, "--no-capture-output"] + pip_install_command

                    pip_install_code, _, pip_install_error = self._run_command(pip_install_command, capture_output=True)
                    pip_file_name = Path(pip_req_file).name
                    pip_file_name = os.path.splitext(pip_file_name)[0]
                    shutil.copy(pip_req_file, os.path.join(env_output_dir, f"pip_requirements.{pip_file_name}.txt"))
                    if pip_install_code != 0:
                        raise RuntimeError(f"Failed to install pip requirements for conda environment `{env_output_dir}: {pip_install_error}")
            
            # Create a file to indicate that the environment has been initialized
            (Path(env_output_dir) / "redun_initialized").touch()
            return

        if self.env_name and self.env_name not in env_list_output:
            raise RuntimeError(
                f"Conda environment {self.env_name} does not exist.\nFound environments: {env_list_output}"
            )

        elif self.env_dir and self.env_dir not in env_list_output:
            raise RuntimeError(
                f"Conda environment in directory {self.env_dir} does not exist.\nFound environments: {env_list_output}"
            )



    def run_command(self, command: List[str], capture_output: bool = False) -> str:
        env_command = self.get_conda_command() + command
        return_code, command_output, command_error = self._run_command(
            env_command, capture_output
        )
        if return_code != 0:
            raise RuntimeError(
                f"Failed to execute command in Conda environment: {command_error}"
            )
        return command_output
        
    def get_conda_command(self) -> List[str]:
        if self.env_name:
            return ["conda", "run", "--no-capture-output", "-n", self.env_name]
        if self.env_dir:
            return ["conda", "run", "--no-capture-output", "-p", self.env_dir]
        raise RuntimeError(f"Cannot get conda command without `env_name` or `env_dir`. {self}")

    def _hash_file(self, file_path: str, other_files: Optional[List[str]]) -> str:
        with open(file_path, "r") as file:
            # strip whitespaces and newlines
            file_content = sorted(line.strip() for line in file)
        if other_files:
            for f in other_files:
                with open(f, "r") as file:
                    file_content += sorted(line.strip() for line in file)
        file_hash = hashlib.md5(b"".join(line.encode() for line in file_content)).hexdigest()
        return file_hash


@register_executor("conda")
class CondaExecutor(Executor):
    """
    Executor that runs tasks and scripts in a local conda environment.
    Tasks accept the following options:
        - conda: Path to a conda environment file (either .yml or .lock), or a conda environment name
        - pip: Path to a pip requirements file, python package name, or a list of python package names. Optional.
    """

    def __init__(
        self,
        name: str,
        scheduler: Optional[Scheduler] = None,
        config: Optional[SectionProxy] = None,
    ):
        super().__init__(name, scheduler=scheduler)

        # Parse config.
        if not config:
            config = create_config_section()

        self.max_workers = config.getint("max_workers", 20)
        self.env_base = config.get("env_base", ".envs")
        self._scratch_prefix_rel = config.get("scratch", ".scratch_redun")
        self._scratch_prefix_abs: Optional[str] = None
        self._thread_executor: Optional[ThreadPoolExecutor] = None

    @property
    def _scratch_prefix(self) -> str:
        if not self._scratch_prefix_abs:
            if os.path.isabs(self._scratch_prefix_rel):
                self._scratch_prefix_abs = self._scratch_prefix_rel
            else:
                # TODO: Is there a better way to find the path of the current
                # config dir?
                try:
                    assert self._scheduler
                    base_dir = os.path.abspath(
                        self._scheduler.config["repos"]["default"]["config_dir"]
                    )
                except KeyError:
                    # Use current working directory as base_dir if default
                    # config_dir cannot be found.
                    base_dir = os.getcwd()

                self._scratch_prefix_abs = os.path.normpath(
                    os.path.join(base_dir, self._scratch_prefix_rel)
                )
        assert self._scratch_prefix_abs
        return self._scratch_prefix_abs

    def _start(self) -> None:
        """
        Start pool on first Job submission.
        """
        os.makedirs(self._scratch_prefix, exist_ok=True)
        if not self._thread_executor:
            self._thread_executor = ThreadPoolExecutor(max_workers=self.max_workers)

    def stop(self) -> None:
        """
        Stop Executor pools.
        """
        if self._thread_executor:
            self._thread_executor.shutdown()
            self._thread_executor = None

    def _submit(self, job: Job) -> None:
        # Ensure pool are started.
        self._start()

        # Run job in a new thread or process.
        assert self._thread_executor
        assert job.task

        def on_done(future):
            try:
                success, result = future.result()
                if success:
                    self._scheduler.done_job(job, result)
                else:
                    error, traceback = result
                    self._scheduler.reject_job(job, error, traceback)
            except Exception as e:
                self._scheduler.reject_job(job, error=e)

        assert job.args
        args, kwargs = job.args
        self._thread_executor.submit(
            execute,
            self._scratch_prefix,
            self.env_base,
            self._scheduler.logger.getEffectiveLevel(),
            job,
            job.task.fullname,
            args,
            kwargs,
        ).add_done_callback(on_done)

    def submit(self, job: Job) -> None:
        assert job.task
        assert not job.task.script
        self._submit(job)

    def submit_script(self, job: Job) -> None:
        assert job.task
        assert job.task.script
        self._submit(job)

def get_job_conda_environment(job: Job, env_base_path: Optional[str]) -> CondaEnvironment:
    """
    Return the conda environment for the given job.
    """
    conda_arg = job.get_option("conda")
    if conda_arg is None:
        raise RuntimeError("No conda environment name/dir/file provided.")

    pip_arg = job.get_option("pip")
    pip_requirements_files = None
    if pip_arg is not None:
        if isinstance(pip_arg, str):
            # either it's a file or a single package
            if os.path.exists(pip_arg) and os.path.isfile(pip_arg):
                pip_requirements_files = [pip_arg]
            else:
                with tempfile.NamedTemporaryFile(
                    mode="w", prefix="pip_requirements_", suffix=".txt", delete=False
                ) as f:
                    f.write(pip_arg)
                pip_requirements_files = [f.name]
        if isinstance(pip_arg, list):
            # determine format of pip_arg
            # either a list of packages or a list of lists of packages
            is_list_of_lists = all(isinstance(x, list) for x in pip_arg)
            is_list_of_str = all(isinstance(x, str) for x in pip_arg)
            if not is_list_of_lists and not is_list_of_str:
                raise ValueError(
                    f"pip option must be a string, a list of strings or a list of lists of strings: {pip_arg}"
                )

            if is_list_of_lists:
                pip_requirements_files = []
                for package_list in pip_arg:
                    with tempfile.NamedTemporaryFile(
                        mode="w", prefix="pip_requirements_", suffix=".txt", delete=False
                    ) as f:
                        if len(package_list) == 1 and os.path.exists(package_list[0]) and os.path.isfile(package_list[0]):
                            pip_requirements_files.append(package_list[0])
                        else:
                            for package in package_list:
                                if os.path.exists(package) and os.path.isfile(package):
                                    f.write(f"-r {package}\n")
                                else:
                                    f.write(f"{package}\n")
                            pip_requirements_files.append(f.name)
            else:
                with tempfile.NamedTemporaryFile(
                    mode="w", prefix="pip_requirements_", suffix=".txt", delete=False
                ) as f:
                    for package in pip_arg:
                        f.write(f"{package}\n")
                    pip_requirements_files = [f.name]


    if os.path.exists(conda_arg): # file or directory
        if os.path.isdir(conda_arg):
            return CondaEnvironment(env_dir=conda_arg, pip_requirements_files=pip_requirements_files, output_dir=env_base_path)
        else:
            return CondaEnvironment(env_file=conda_arg, pip_requirements_files=pip_requirements_files, output_dir=env_base_path)
    else:
        # assume conda_arg is an env name
        return CondaEnvironment(env_name=conda_arg, pip_requirements_files=pip_requirements_files, output_dir=env_base_path)

def get_task_command(task: Task, args: Tuple, kwargs: dict, log_level: int) -> str:
    """
    Get command from a script task.
    """
    command = task.func(*args, **kwargs)
    if log_level <= logging.INFO:
        shell = "#!/usr/bin/env bash\nset -euxo pipefail"
    else:
        shell = "#!/usr/bin/env bash\nset -euo pipefail"
    return prepare_command(command, shell)

def execute(
    scratch_path: str,
    env_base_path: Optional[str],
    log_level: int,
    job: Job,
    task_fullname: str,
    args: Tuple = (),
    kwargs: Dict[str, Any] = None,
) -> Tuple[bool, Any]:
    """
    Run a job in a local conda environment.
    """
    assert job.task

    # get conda environment
    conda_env = get_job_conda_environment(job, env_base_path)

    if kwargs is None:
        kwargs = {}
    task_files_dir = mkdtemp(prefix=f"{task_fullname}_", dir=scratch_path)
    input_path = os.path.join(task_files_dir, "input")
    output_path = os.path.join(task_files_dir, "output")
    error_path = os.path.join(task_files_dir, "error")

    command_path = os.path.join(task_files_dir, "command")
    command_output_path = os.path.join(task_files_dir, "command_output")
    command_error_path = os.path.join(task_files_dir, "command_error")

    if job.task.script:
        script_command = get_task_command(job.task, args, kwargs, log_level)
        script_command_path = os.path.join(task_files_dir, "script_command")
        with open(script_command_path, "w") as f:
            f.write(script_command)
        os.chmod(script_command_path, 0o755)
        inner_command = [script_command_path]
    else:
        inner_command = get_oneshot_command(
            scratch_path,
            job,
            job.task,
            args,
            kwargs,
            input_path=input_path,
            output_path=output_path,
            error_path=error_path,
        )

    command = wrap_command(
        inner_command, conda_env.get_conda_command(), command_path, command_output_path, command_error_path
    )
    cmd_result = subprocess.run(command, check=False, capture_output=False)

    if not job.task.script:
        return handle_oneshot_output(output_path, error_path, command_error_path)
    else:
        return handle_script_output(cmd_result.returncode, command_output_path, command_error_path)


def wrap_command(
    command: List[str],
    conda_run_cmd: List[str],
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
    conda_command = [*conda_run_cmd, *command]
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
