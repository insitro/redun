import logging
import os
import subprocess
import threading
import time
from collections import OrderedDict
from configparser import SectionProxy
from tempfile import mkstemp
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

from redun.config import create_config_section
from redun.executors import aws_utils
from redun.executors.base import Executor, register_executor
from redun.executors.code_packaging import package_code, parse_code_package_config
from redun.executors.command import get_oneshot_command, get_script_task_command
from redun.executors.scratch import (
    SCRATCH_OUTPUT,
    SCRATCH_STATUS,
    get_job_scratch_dir,
    get_job_scratch_file,
    parse_job_error,
    parse_job_result,
)
from redun.file import File
from redun.scheduler import Job, Scheduler
from redun.scripting import get_task_command
from redun.task import Task

SUCCEEDED = "SUCCEEDED"
FAILED = "FAILED"


class DockerError(Exception):
    pass


def get_docker_executor_config(config: SectionProxy) -> SectionProxy:
    """
    Returns a config for DockerExecutor.
    """
    keys = ["image", "scratch", "job_monitor_interval", "vcpus", "gpus", "memory"]
    return create_config_section({key: config[key] for key in keys if key in config})


def run_docker(
    command: List[str],
    image: str,
    volumes: Iterable[Tuple[str, str]] = [],
    interactive: bool = True,
    cleanup: bool = False,
    memory: int = 4,
    vcpus: int = 1,
    gpus: int = 0,
    shared_memory: Optional[int] = None,
    include_aws_env: bool = False,
) -> str:
    """
    Run a Docker container locally.

    Parameters
    ----------
    command : List[str]
        A shell command to run within the docker container (e.g. ["ls" "-la"]).
    image : str
        A Docker image.
    volumes : Iterable[Tuple[str, srt]]
        A list of ('host', 'container') path pairs for volume mounting.
    interactive : bool
        If True, the Docker container is run in interactive mode.
    cleanup : bool
        If True, remove the container after execution.
    memory : int
        Number of GB of memory to reserve for the container.
    vcpus : int
        Number of CPUs to reserve for the container.
    gpus : int
        Number of GPUs to reserve for the container.
    shared_memory : Optional[int]
        Number of GB of shared memory to reserve for the container.
    include_aws_env : bool
        If True, forward AWS environment variables to the container.
    """
    # Add AWS credentials to environment for docker command.
    common_args = []
    if cleanup:
        common_args.append("--rm")

    env = dict(os.environ)
    if include_aws_env and not aws_utils.is_ec2_instance():
        # Forward AWS environment args to docker container.
        env.update(aws_utils.get_aws_env_vars())
        common_args.extend(
            [
                "-e",
                "AWS_ACCESS_KEY_ID",
                "-e",
                "AWS_SECRET_ACCESS_KEY",
                "-e",
                "AWS_SESSION_TOKEN",
                "-e",
                "AWS_DEFAULT_REGION",
            ]
        )

    # Volume mounting args.
    for host, container in volumes:
        common_args.extend(["-v", f"{host}:{container}"])

    common_args.extend([f"--memory={memory}g", f"--cpus={vcpus}"])
    if gpus:
        # We can't easily assign a single gpu so we make all available if any GPUs are required.
        common_args.extend(["--gpus", "all"])
    if shared_memory is not None:
        common_args.append(f"--shm-size={shared_memory}g")

    common_args.append(image)
    common_args.extend(command)

    if interactive:
        # Adding this flag is necessary to prevent docker from hijacking the terminal and modifying
        # the tty settings. One of the modifications it makes when it hijacks the terminal is to
        # change the behavior of line endings which means output(like from logging) will be
        # malformed until the docker container exits and the hijacked connection is closed which
        # resets the tty settings.
        env["NORAW"] = "true"

        # Run Docker interactively.
        fd, cidfile = mkstemp()
        os.close(fd)
        os.remove(cidfile)

        docker_command = ["docker", "run", "-it", "--cidfile", cidfile] + common_args
        try:
            subprocess.check_call(docker_command, env=env)
        except subprocess.CalledProcessError as error:
            raise DockerError(error)
        with open(cidfile) as infile:
            container_id = infile.read().strip()
        os.remove(cidfile)
    else:
        # Run Docker in the background.
        docker_command = ["docker", "run", "-d"] + common_args
        try:
            container_id = subprocess.check_output(docker_command, env=env).strip().decode("utf8")
        except subprocess.CalledProcessError as error:
            raise DockerError(error)

    return container_id


def get_docker_job_options(job_options: dict, scratch_path: str) -> dict:
    """
    Returns Docker-specific job options from general job options.

    Adds the scratch_path as a volume mount.
    """
    keys = [
        "vcpus",
        "memory",
        "gpus",
        "shared_memory",
        "volumes",
        "interactive",
        "include_aws_env",
    ]
    options = {key: job_options[key] for key in keys if key in job_options}
    options["volumes"] = options.get("volumes", []) + [(scratch_path, scratch_path)]
    return options


def submit_task(
    image: str,
    scratch_prefix: str,
    job: Job,
    a_task: Task,
    args: Tuple = (),
    kwargs: Dict[str, Any] = {},
    job_options: dict = {},
    code_file: Optional[File] = None,
    include_aws_env: bool = False,
) -> Dict[str, Any]:
    """
    Submit a redun Task to Docker.
    """
    command = get_oneshot_command(
        scratch_prefix,
        job,
        a_task,
        args,
        kwargs,
        job_options=job_options,
        code_file=code_file,
    )
    container_id = run_docker(
        command,
        image=image,
        **get_docker_job_options(job_options, scratch_prefix),
    )
    return {"jobId": container_id, "redun_job_id": job.id}


def submit_command(
    image: str,
    scratch_prefix: str,
    job: Job,
    command: str,
    job_options: dict = {},
    include_aws_env: bool = False,
) -> dict:
    """
    Submit a shell command to Docker.
    """
    shell_command = get_script_task_command(scratch_prefix, job, command)
    container_id = run_docker(
        shell_command,
        image=image,
        **get_docker_job_options(job_options, scratch_prefix),
    )
    return {"jobId": container_id, "redun_job_id": job.id}


def iter_job_status(scratch_prefix: str, job_id2job: Dict[str, "Job"]) -> Iterator[dict]:
    """
    Returns local Docker jobs grouped by their status.
    """
    running_containers = subprocess.check_output(["docker", "ps", "--no-trunc"]).decode("utf8")

    for job_id, redun_job in job_id2job.items():
        if job_id not in running_containers:
            # Job is done running.
            status_file = File(get_job_scratch_file(scratch_prefix, redun_job, SCRATCH_STATUS))
            output_file = File(get_job_scratch_file(scratch_prefix, redun_job, SCRATCH_OUTPUT))

            # Get docker logs and remove container.
            logs = subprocess.check_output(["docker", "logs", job_id]).decode("utf8")
            logs += "Removing container...\n"
            logs += subprocess.check_output(["docker", "rm", job_id]).decode("utf8")

            # TODO: Simplify whether status file is always used or not.
            if status_file.exists():
                succeeded = status_file.read().strip() == "ok"
            else:
                succeeded = output_file.exists()

            status = SUCCEEDED if succeeded else FAILED
            yield {"jobId": job_id, "status": status, "logs": logs}


@register_executor("docker")
class DockerExecutor(Executor):
    """
    A redun Executor for running jobs on local Docker containers.
    """

    def __init__(
        self,
        name: str,
        scheduler: Optional[Scheduler] = None,
        config: Optional[SectionProxy] = None,
    ):
        super().__init__(name, scheduler=scheduler)
        if config is None:
            raise ValueError("DockerExecutor requires config.")

        # Required config.
        self._image = config["image"]
        self._scratch_prefix_rel = config["scratch"]
        self._scratch_prefix_abs: Optional[str] = None
        self._interval = config.getfloat("job_monitor_interval", fallback=0.2)

        # Optional config.
        self._code_package = parse_code_package_config(config)
        self._code_file: Optional[File] = None

        # Default task options.
        self._default_job_options = {
            "vcpus": config.getint("vcpus", fallback=1),
            "gpus": config.getint("gpus", fallback=0),
            "memory": config.getint("memory", fallback=4),
            "shared_memory": config.getint("shared_memory", fallback=None),
            "interactive": config.getboolean("interactive", fallback=False),
            "include_aws_env": config.getboolean("include_aws_env", fallback=True),
        }

        self._is_running = False
        self._pending_jobs: Dict[str, "Job"] = OrderedDict()
        self._thread: Optional[threading.Thread] = None

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
        Start monitoring thread.
        """
        os.makedirs(self._scratch_prefix, exist_ok=True)

        if not self._is_running:
            self._is_running = True
            self._thread = threading.Thread(target=self._monitor, daemon=False)
            self._thread.start()

    def stop(self) -> None:
        """
        Stop Executor and monitoring thread.
        """
        self._is_running = False

        # Stop monitor thread.
        if (
            self._thread
            and self._thread.is_alive()
            and threading.get_ident() != self._thread.ident
        ):
            self._thread.join()

    def _monitor(self) -> None:
        """
        Thread for monitoring local Docker containers.
        """
        assert self._scheduler

        try:
            while self._is_running and self._pending_jobs:
                # Copy pending_jobs since it can change due to new submissions.
                jobs = iter_job_status(self._scratch_prefix, dict(self._pending_jobs))
                for job in jobs:
                    self._process_job_status(job)
                time.sleep(self._interval)

        except Exception as error:
            # Since we run this is method at the top-level of a thread, we
            # need to catch all exceptions so we can properly report them to
            # the scheduler.
            self._scheduler.reject_job(None, error)

        self.log("Shutting down executor...", level=logging.DEBUG)
        self.stop()

    def _process_job_status(self, job: dict) -> None:
        """
        Process container job statuses.
        """
        assert self._scheduler

        if job["status"] == SUCCEEDED:
            # Assume a recently completed job has valid results.
            redun_job = self._pending_jobs.pop(job["jobId"])
            result, exists = parse_job_result(self._scratch_prefix, redun_job)
            if exists:
                self._scheduler.done_job(redun_job, result)
            else:
                # This can happen if job ended in an inconsistent state.
                self._scheduler.reject_job(
                    redun_job,
                    FileNotFoundError(
                        get_job_scratch_file(self._scratch_prefix, redun_job, SCRATCH_OUTPUT)
                    ),
                )
        elif job["status"] == FAILED:
            redun_job = self._pending_jobs.pop(job["jobId"])
            error, error_traceback = parse_job_error(self._scratch_prefix, redun_job)
            error_traceback.logs = [line + "\n" for line in job["logs"].split("\n")]
            self._scheduler.reject_job(redun_job, error, error_traceback=error_traceback)

    def _submit(self, job: Job) -> None:
        """
        Submit Job to executor.
        """
        assert self._scheduler
        assert job.args
        args, kwargs = job.args

        # Package code if necessary and we have not already done so. If code_package is False,
        # then we can skip this step. Additionally, if we have already packaged and set code_file,
        # then we do not need to repackage.
        if self._code_package is not False and self._code_file is None:
            code_package = self._code_package or {}
            assert isinstance(code_package, dict)
            self._code_file = package_code(self._scratch_prefix, code_package)

        job_options: dict = {
            **self._default_job_options,
            **job.get_options(),
        }
        image: str = job_options.pop("image", self._image)

        # Submit a new Batch job.
        try:
            if not job.task.script:
                docker_resp = submit_task(
                    image,
                    self._scratch_prefix,
                    job,
                    job.task,
                    args=args,
                    kwargs=kwargs,
                    job_options=job_options,
                    code_file=self._code_file,
                )
            else:
                command = get_task_command(job.task, args, kwargs)
                docker_resp = submit_command(
                    image,
                    self._scratch_prefix,
                    job,
                    command,
                    job_options=job_options,
                )
        except DockerError:
            error, error_traceback = parse_job_error(self._scratch_prefix, job)
            self._scheduler.reject_job(job, error, error_traceback=error_traceback)
            return

        job_dir = get_job_scratch_dir(self._scratch_prefix, job)
        self.log(
            "submit redun job {redun_job} as Docker container {container_id}:\n"
            "  container_id = {container_id}\n"
            "  scratch_path = {job_dir}\n".format(
                redun_job=job.id,
                container_id=docker_resp["jobId"],
                job_dir=job_dir,
            )
        )
        self._pending_jobs[docker_resp["jobId"]] = job
        self._start()

    def submit(self, job: Job) -> None:
        """
        Submit Job to executor.
        """
        return self._submit(job)

    def submit_script(self, job: Job) -> None:
        """
        Submit Job for script task to executor.
        """
        return self._submit(job)

    def scratch_root(self) -> str:
        return self._scratch_prefix
