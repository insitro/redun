import glob
import os
import pickle
import shlex
import tarfile
import tempfile
import threading
import zipfile
from typing import Any, Dict, Iterable, List, NamedTuple, Optional, Set, Tuple, Union

import boto3

from redun.file import File
from redun.hashing import hash_stream
from redun.scheduler import Job

# Constants.
REDUN_PROG = "redun"
REDUN_REQUIRED_VERSION = ">=0.4.1"
DEFAULT_AWS_REGION = "us-west-2"

# S3 scratch filenames.
S3_SCRATCH_INPUT = "input"
S3_SCRATCH_OUTPUT = "output"
S3_SCRATCH_CODE = "code.tar.gz"
S3_SCRATCH_ERROR = "error"
S3_SCRATCH_HASHES = "eval_hashes"
S3_SCRATCH_STATUS = "status"

# Cache for AWS Clients.
_boto_clients: Dict[Tuple[int, str, str], boto3.Session] = {}


class JobStatus(NamedTuple):
    all: List[str]
    pending: List[str]
    inflight: List[str]
    success: List[str]
    failure: List[str]
    stopped: List[str]
    timeout: List[str]


def get_aws_client(service: str, aws_region: str = DEFAULT_AWS_REGION) -> boto3.Session:
    """
    Get an AWS Client with caching.
    """
    cache_key = (threading.get_ident(), service, aws_region)
    client = _boto_clients.get(cache_key)
    if not client:
        # Boto is not thread safe, so we create a client per thread using
        # `threading.get_ident()` as part of our cache key.
        # We need to create the client using a session to avoid all clients
        # sharing the same global session.
        # See: https://github.com/boto/boto3/issues/801#issuecomment-440942144
        session = boto3.session.Session()
        client = _boto_clients[cache_key] = session.client(service, region_name=aws_region)

    return client


def find_code_files(
    basedir: str = ".", includes: Optional[List[str]] = None, excludes: Optional[List[str]] = None
) -> Iterable[str]:
    """
    Find all the workflow code files consistent with the include/exclude patterns.
    """
    if includes is None:
        includes = ["**/*.py"]
    if excludes is None:
        excludes = []

    files: Set[str] = set()
    for pattern in includes:
        files |= set(glob.glob(os.path.join(basedir, pattern), recursive=True))

    for pattern in excludes:
        files -= set(glob.glob(os.path.join(basedir, pattern), recursive=True))
    return files


def create_tar(tar_path: str, file_paths: Iterable[str]) -> File:
    """
    Create a tar file from local file paths.
    """
    tar_file = File(tar_path)

    with tar_file.open("wb") as out:
        with tarfile.open(fileobj=out, mode="w|gz") as tar:
            for file_path in file_paths:
                tar.add(file_path)

    return tar_file


def extract_tar(tar_file: File, dest_dir: str = ".") -> None:
    """
    Extract a tar file to local paths.
    """
    with tar_file.open("rb") as infile:
        with tarfile.open(fileobj=infile, mode="r|gz") as tar:
            tar.extractall(dest_dir)


def create_zip(zip_path: str, base_path: str, file_paths: Iterable[str]) -> File:
    """
    Create a zip file from local file paths.
    """
    zip_file = File(zip_path)

    with zip_file.open("wb") as out:
        with zipfile.ZipFile(out, mode="w") as stream:
            for file_path in file_paths:
                arcname = os.path.relpath(file_path, base_path)
                stream.write(file_path, arcname)

    return zip_file


def get_job_scratch_dir(s3_scratch_prefix: str, job: Job) -> str:
    """
    Returns s3 scratch directory for a redun Job.
    """
    assert job.eval_hash
    return os.path.join(s3_scratch_prefix, "jobs", job.eval_hash)


def get_job_scratch_file(s3_scratch_prefix: str, job: Job, filename: str) -> str:
    """
    Returns s3 scratch path for a file related to a redun Job.
    """
    assert job.eval_hash
    return os.path.join(s3_scratch_prefix, "jobs", job.eval_hash, filename)


def get_code_scratch_file(s3_scratch_prefix: str, tar_hash: str, use_zip: bool = False) -> str:
    """
    Returns s3 scratch path for a code package tar file.
    """
    return os.path.join(s3_scratch_prefix, "code", tar_hash + (".zip" if use_zip else ".tar.gz"))


def get_array_scratch_file(s3_scratch_prefix: str, job_array_id: str, filename: str) -> str:
    """
    Returns an S3 scratch path for a file related to an AWS batch job
    """
    return os.path.join(s3_scratch_prefix, "array_jobs", job_array_id, filename)


def copy_to_s3(file_path: str, s3_scratch_dir: str) -> str:
    """
    Copies a file to the S3 scratch directory if it is not already on S3.
    Returns the path to the file on S3.
    """
    file = File(file_path)
    _, filename = os.path.split(file.path)

    s3_temp_file = File(f"{s3_scratch_dir.rstrip('/')}/{filename}")
    file.copy_to(s3_temp_file)
    return s3_temp_file.path


def get_default_region() -> str:
    """
    Returns the default AWS region.
    """
    return boto3.Session().region_name or DEFAULT_AWS_REGION


def get_aws_user(aws_region: str = DEFAULT_AWS_REGION) -> str:
    """
    Returns the current AWS user.
    """
    sts_client = get_aws_client("sts", aws_region=aws_region)
    response = sts_client.get_caller_identity()
    return response["Arn"]


def parse_code_package_config(config) -> Union[dict, bool]:
    """
    Parse the code package options from a AWSBatchExecutor config.
    """
    if not config.getboolean("code_package", fallback=True):
        return False

    include_config = config.get("code_includes", "**/*.py")
    exclude_config = config.get("code_excludes", "")

    return {"includes": shlex.split(include_config), "excludes": shlex.split(exclude_config)}


def package_code(s3_scratch_prefix: str, code_package: dict = {}, use_zip=False) -> File:
    """
    Package code to S3.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        file_paths = find_code_files(
            includes=code_package.get("includes"), excludes=code_package.get("excludes")
        )

        if use_zip:
            temp_file = File(os.path.join(tmpdir, "code.zip"))
            create_zip(temp_file.path, ".", file_paths)
        else:
            temp_file = File(os.path.join(tmpdir, "code.tar.gz"))
            create_tar(temp_file.path, file_paths)

        with temp_file.open("rb") as infile:
            tar_hash = hash_stream(infile)
        code_file = File(get_code_scratch_file(s3_scratch_prefix, tar_hash, use_zip=use_zip))
        if not code_file.exists():
            temp_file.copy_to(code_file)

    return code_file


def parse_task_result(s3_scratch_prefix: str, job: Job) -> Any:
    """
    Parse task result from s3 scratch path.
    """
    output_path = get_job_scratch_file(s3_scratch_prefix, job, S3_SCRATCH_OUTPUT)

    output_file = File(output_path)
    assert job.task
    if not job.task.script:
        with output_file.open("rb") as infile:
            result = pickle.load(infile)
    else:
        result = [0, output_file.read(mode="rb")]  # TODO: Get real exitcode.

    return result
