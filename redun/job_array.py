import os
import threading
import time
from collections import defaultdict
from typing import Any, Callable, Dict, List, Optional, cast

from redun.scheduler import Job

# Per AWS documentation, see:
# https://docs.aws.amazon.com/batch/latest/userguide/service_limits.html
MAX_ARRAY_SIZE = 10000
# https://docs.aws.amazon.com/batch/latest/userguide/job_env_vars.html
AWS_ARRAY_VAR = "AWS_BATCH_JOB_ARRAY_INDEX"
# https://kubernetes.io/docs/tasks/job/indexed-parallel-processing-static/
K8S_ARRAY_VAR = "JOB_COMPLETION_INDEX"
# https://cloud.google.com/batch/docs/create-run-basic-job#predefined-environment-variables
GCP_ARRAY_VAR = "BATCH_TASK_INDEX"


def get_job_array_index(env: dict = cast(dict, os.environ)) -> Optional[int]:
    """
    Finds any Job Array Index environment variables and returns the index.
    """
    if AWS_ARRAY_VAR in env:
        return int(env[AWS_ARRAY_VAR])
    elif K8S_ARRAY_VAR in env:
        return int(env[K8S_ARRAY_VAR])
    elif GCP_ARRAY_VAR in env:
        return int(env[GCP_ARRAY_VAR])
    else:
        return None


class JobDescription:
    def __init__(self, job: Job):
        self.task_name = job.task.name
        self.options = job.get_options()
        self.key = self.task_name + " " + str(sorted(self.options.items()))

    def __hash__(self) -> int:
        return hash(self.key)

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, JobDescription) and self.key == other.key

    def __repr__(self) -> str:
        return f"NAME:{self.task_name}\tOPTIONS: {self.options}"


class JobArrayer:
    """
    A 'sink' for submitted jobs that detects when jobs can be submitted as an
    array job. Eligible jobs will have the same task, task options, and
    resource requirements, but can differ in the args and kwargs passed to the
    task.

    The method uses "staleness" of submitted jobs in the 'sink'.  Eligible jobs
    are grouped in the pool (really a dict). Grouped jobs that haven't had new
    ones added to the group for `stale_time` sec will be submitted.

    Parameters
    ----------
    submit_jobs: Callable[[List[Job]], None]
        Callback called when a group of jobs are ready to be submitted as an array.

    on_error: Callable[[Exception], None]
        Callback called when there is a top-level error.

    submit_interval: float
        How frequently the monitor thread will check for submittable stale
        jobs. Should be less than `stale_time`, ideally.

    stale_time: float
        Job groupings that haven't had new jobs added for this many seconds
        will be submitted.

    min_array_size: int
        Minimum number of jobs in a group to be submitted as an array job
        instead of individual jobs. Can be anywhere from 2 to
        `MAX_ARRAY_SIZE-1`.

    max_array_size: int
        Maximum number of jobs that can be submitted as an array job. Must
        be in (`min_array_size`, `MAX_ARRAY_SIZE`].
    """

    def __init__(
        self,
        submit_jobs: Callable[[List[Job]], None],
        on_error: Callable[[Exception], None],
        submit_interval: float,
        stale_time: float,
        min_array_size: int,
        max_array_size: int = MAX_ARRAY_SIZE,
    ):

        self.min_array_size = min_array_size
        self.max_array_size = min(max_array_size, MAX_ARRAY_SIZE)
        if self.max_array_size < self.min_array_size:
            raise ValueError("Maximum array size cannot be less than minimum.")

        self.pending: Dict[JobDescription, List[Job]] = defaultdict(list)
        self.pending_timestamps: Dict[JobDescription, float] = {}
        self._lock = threading.Lock()

        self._submit_jobs = submit_jobs
        self._on_error = on_error
        self.interval = submit_interval
        self.stale_time = stale_time

        # Monitor thread
        self._monitor_thread = threading.Thread(target=self._monitor_stale_jobs, daemon=True)
        self._exit_flag = threading.Event()
        self.num_pending = 0

    def _monitor_stale_jobs(self) -> None:
        """Monitoring thread batches up stale jobs"""
        try:
            while not self._exit_flag.wait(timeout=self.interval):
                stales = self.get_stale_descrs()

                for descr in stales:
                    self.submit_pending_jobs(descr)
        except Exception as error:
            # Since we run this method at the top level of a thread, we need to
            # catch all exceptions so we can properly report them to the
            # scheduler.
            self._on_error(error)

    def start(self) -> None:
        # Do not have a monitor thread when arraying is disabled.
        if not self.min_array_size:
            return

        if self._monitor_thread.is_alive():
            return

        # Initialize a new Thread here in case a previous one has completed,
        # since Threads can't be started more than once.
        self._exit_flag.clear()
        self._monitor_thread = threading.Thread(target=self._monitor_stale_jobs, daemon=True)
        self._monitor_thread.start()

    def stop(self) -> None:
        self._exit_flag.set()
        if self._monitor_thread.is_alive():
            self._monitor_thread.join()

    def add_job(self, job: Job) -> None:
        """Adds a new job"""

        # If arraying is turned off, just submit the job.
        # Script jobs are also not handled yet.
        if job.task.script or not self.min_array_size:
            self._submit_jobs([job])
            return

        descr = JobDescription(job)
        with self._lock:
            self.pending[descr].append(job)
            self.pending_timestamps[descr] = time.time()
            self.num_pending += 1

        self.start()

    def get_stale_descrs(self) -> List[JobDescription]:
        """Submits jobs that haven't been touched in a while"""
        currtime = time.time()
        stales = [
            descr
            for descr in self.pending
            if (currtime - self.pending_timestamps[descr] > self.stale_time)
        ]
        return stales

    def submit_pending_jobs(self, descr: JobDescription) -> None:
        # Lock, otherwise adding a job to this descr at the wrong time could
        # result in timestamps out of sync with jobs.
        with self._lock:
            jobs = self.pending.pop(descr)
            timestamp = self.pending_timestamps.pop(descr)

        # Submit at most max_array_size jobs and leave the remainder.
        if len(jobs) > self.max_array_size:
            remainder = jobs[self.max_array_size :]
            jobs = jobs[: self.max_array_size]
            self._submit_jobs(jobs)

            with self._lock:
                self.pending[descr].extend(remainder)
                self.pending_timestamps[descr] = timestamp

        elif len(jobs) < self.min_array_size:
            for job in jobs:
                self._submit_jobs([job])
        else:
            self._submit_jobs(jobs)

        self.num_pending -= len(jobs)
