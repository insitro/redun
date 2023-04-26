"""
Utilities for working with executor scratch directories.
"""

import os
import pickle
from typing import Any, Callable, Optional, Tuple, cast

from redun.file import File
from redun.scheduler import Job, Traceback
from redun.scripting import ScriptError

# Scratch filenames.
SCRATCH_INPUT = "input"
SCRATCH_OUTPUT = "output"
SCRATCH_ERROR = "error"
SCRATCH_HASHES = "eval_hashes"
SCRATCH_STATUS = "status"


class ScratchError(Exception):
    """
    Error when reading data from scratch directory.
    """

    pass


class ExceptionNotFoundError(ScratchError):
    """
    Error when serialized exception is not found in scratch directory.
    """

    pass


def get_job_scratch_dir(scratch_prefix: str, job: Job) -> str:
    """
    Returns scratch directory for a redun Job.
    """
    assert job.eval_hash
    return os.path.join(scratch_prefix, "jobs", job.eval_hash)


def get_job_scratch_file(scratch_prefix: str, job: Job, filename: str) -> str:
    """
    Returns scratch path for a file related to a redun Job.
    """
    assert job.eval_hash
    return os.path.join(scratch_prefix, "jobs", job.eval_hash, filename)


def get_code_scratch_file(scratch_prefix: str, tar_hash: str, use_zip: bool = False) -> str:
    """
    Returns scratch path for a code package tar file.
    """
    return os.path.join(scratch_prefix, "code", tar_hash + (".zip" if use_zip else ".tar.gz"))


def get_array_scratch_file(scratch_prefix: str, job_array_id: str, filename: str) -> str:
    """
    Returns an scratch path for a file related to an array of jobs.
    """
    return os.path.join(scratch_prefix, "array_jobs", job_array_id, filename)


def get_execution_scratch_file(scratch_prefix: str, execution_id: str, filename: str) -> str:
    """
    Returns an scratch path for a sending data to and from a remote execution.
    """
    return os.path.join(scratch_prefix, "execution", execution_id, filename)


def parse_job_result(
    scratch_prefix: str,
    job: Job,
    is_valid_value: Optional[Callable[[Any], bool]] = None,
) -> Tuple[Any, bool]:
    """
    Returns job output from scratch directory.

    Returns a tuple of (result, exists).
    """
    output_file = File(get_job_scratch_file(scratch_prefix, job, SCRATCH_OUTPUT))
    if output_file.exists():
        if not job.task.script:
            with output_file.open("rb") as infile:
                result = pickle.load(infile)
        else:
            result = [0, output_file.read(mode="rb")]  # TODO: Get real exitcode.

        if is_valid_value is None or is_valid_value(result):
            return result, True
    return None, False


def parse_job_error(scratch_prefix: str, job: Job) -> Tuple[Exception, Traceback]:
    """
    Returns job error from scratch directory.
    """
    error_path = get_job_scratch_file(scratch_prefix, job, SCRATCH_ERROR)
    error_file = File(error_path)

    if not job.task.script:
        # Normal Tasks (non-script) store errors as Pickled exception, traceback tuples.
        if error_file.exists():
            try:
                error, error_traceback = pickle.loads(cast(bytes, error_file.read("rb")))
            except Exception as parse_error:
                error = ScratchError(
                    f"Error could not be parsed from scratch directory. See logs. {parse_error}"
                )
                error_traceback = Traceback.from_error(error)
        else:
            error = ExceptionNotFoundError("Exception and traceback could not be found for Job.")
            error_traceback = Traceback.from_error(error)

    else:
        # Script task.
        if error_file.exists():
            error = ScriptError(cast(bytes, error_file.read("rb")))
        else:
            error = ExceptionNotFoundError("stderr could not be found for Job.")
        error_traceback = Traceback.from_error(error)

    return error, error_traceback
