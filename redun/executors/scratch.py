"""
Utilities for working with executor scratch directories.
"""

import json
import os
import pickle
from dataclasses import dataclass
from typing import Any, Callable, List, Optional, Tuple, cast

from redun.file import File
from redun.scheduler import Job, Traceback
from redun.scripting import ScriptError
from redun.utils import pickle_dump

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


@dataclass
class ArrayJobScratchFiles:
    input_file: str
    output_file: str
    error_file: str
    eval_file: Optional[str] = None


def write_array_job_scratch_files(
    jobs: List[Job],
    scratch_prefix: str,
    array_id: str,
    include_eval_hash: bool = True,
) -> ArrayJobScratchFiles:
    """
    Write the scratch files for an Array-style Job.
    """
    all_args = []
    all_kwargs = []
    for job in jobs:
        assert job.args
        all_args.append(job.args[0])
        all_kwargs.append(job.args[1])

    # Setup input, output and error path files.
    # Input file is a pickled list of args, and kwargs, for each child job.
    input_file = get_array_scratch_file(scratch_prefix, array_id, SCRATCH_INPUT)
    with File(input_file).open("wb") as out:
        pickle_dump([all_args, all_kwargs], out)

    # Output file is a plaintext list of output paths, for each child job.
    output_file = get_array_scratch_file(scratch_prefix, array_id, SCRATCH_OUTPUT)
    output_paths = [get_job_scratch_file(scratch_prefix, job, SCRATCH_OUTPUT) for job in jobs]
    with File(output_file).open("w") as ofile:
        json.dump(output_paths, ofile)

    # Error file is a plaintext list of error paths, one for each child job.
    error_file = get_array_scratch_file(scratch_prefix, array_id, SCRATCH_ERROR)
    error_paths = [get_job_scratch_file(scratch_prefix, job, SCRATCH_ERROR) for job in jobs]
    with File(error_file).open("w") as efile:
        json.dump(error_paths, efile)

    # Eval hash file is plaintext hashes of child jobs for matching for job reuniting.
    if include_eval_hash:
        eval_file = get_array_scratch_file(scratch_prefix, array_id, SCRATCH_HASHES)
        with File(eval_file).open("w") as eval_f:
            eval_f.write("\n".join([cast(str, job.eval_hash) for job in jobs]))
    else:
        eval_file = None

    return ArrayJobScratchFiles(
        input_file=input_file,
        output_file=output_file,
        error_file=error_file,
        eval_file=eval_file,
    )


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
