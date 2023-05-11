import importlib
import os
import pkgutil
import re
import shutil
import tempfile
import time
from contextlib import contextmanager
from functools import wraps
from inspect import getmembers, getmodule, isclass, isfunction, ismethod, ismodule
from itertools import zip_longest
from typing import IO, Any, Callable, Dict, Iterator, List, NamedTuple, Optional, Type
from unittest.mock import patch

import sqlalchemy.event
from aiobotocore.response import StreamingBody
from botocore.awsrequest import AWSResponse
from moto import mock_s3 as _mock_s3
from moto.core.botocore_stubber import MockRawResponse

# This should be public, and will be in v2.0. Until then, live with the private import.
from urllib3._collections import HTTPHeaderDict

from redun import Scheduler
from redun.file import FileSystem, GSFileSystem, S3FileSystem, get_filesystem_class, get_proto

"""
Current moto (1.3.16) is not fully compatible with the latest s3fs (2021.11.1),
because of s3fs's use of aiobotocore. Here we define a small monkey patch
to make moto compatible enough for our needs. If future versions of moto
gain full support, these patches can be removed.
"""


class AsyncMockedRawResponse(MockRawResponse):
    """
    Wraps a moto MockRawResponse in order to provide an async read method.
    """

    def __init__(self, response):
        self._response = response
        self._content = b""

    async def read(self, *args, **kwargs):
        # Provide an async version of the read method.
        return self._sync_read(*args, **kwargs)

    def _sync_read(self, *args, **kwargs):
        data = self._response.read(*args, **kwargs)
        self._content += data
        return data

    def stream(self, **kwargs):
        # We override stream in order to call the original synchronous read.
        contents = self._sync_read()
        while contents:
            yield contents
            contents = self._sync_read()

    @property
    def content(self):
        return self


class PatchedAWSResponse(AWSResponse):
    """
    Subclasses AWSRespnse in order to provide an async read and raw headers.

    Inspiration: https://github.com/aio-libs/aiobotocore/issues/755#issuecomment-807325931
    """

    def __init__(self, url, status_code, headers, raw):
        async_raw = StreamingBody(AsyncMockedRawResponse(raw), headers.get("content-length"))
        super().__init__(url, status_code, headers, async_raw)

    @property
    def raw_headers(self):
        # Raw headers are expected to be an iterable of pairs of bytes. We can
        # reconstruct the raw headers from the parsed ones.
        return [
            (key.encode("utf-8"), str(value).encode("utf-8"))
            for key, value in self.headers.items()
        ]

    async def read(self):
        return self.text

    @property
    async def content(self):
        return self.raw._content


def convert_to_response_dict_patch(http_response, operation_model):
    """
    Patched version of botocore.endpoint.convert_to_repsonse
    """
    response_dict = {
        # botocore converts keys to str, so make sure that they are in
        # the expected case. See detailed discussion here:
        # https://github.com/aio-libs/aiobotocore/pull/116
        # aiohttp's CIMultiDict camel cases the headers :(
        "headers": HTTPHeaderDict(
            {k.decode("utf-8").lower(): v.decode("utf-8") for k, v in http_response.raw_headers}
        ),
        "status_code": http_response.status_code,
        "context": {
            "operation_name": operation_model.name,
        },
    }
    if response_dict["status_code"] >= 300:
        response_dict["body"] = http_response.raw._sync_read()
    elif operation_model.has_event_stream_output:
        response_dict["body"] = http_response.raw
    elif operation_model.has_streaming_output:
        length = response_dict["headers"].get("content-length")
        response_dict["body"] = StreamingBody(http_response.raw, length)
    else:
        response_dict["body"] = http_response.raw._sync_read()
    return response_dict


async def async_convert_to_response_dict_patch(http_response, operation_model):
    """
    Patched version of aiobotocore.endpoint.convert_to_repsonse
    """
    return convert_to_response_dict_patch(http_response, operation_model)


def patch_aws(func: Callable) -> Callable:
    """
    Fix botocore and moto to work together.

    This patch is needed until moto is fully compatiable with latest botocore.
    """

    @wraps(func)
    def wrapped(*args, **kwargs):
        # Apply patches.
        with patch("botocore.awsrequest.AWSResponse", PatchedAWSResponse), patch(
            "moto.core.botocore_stubber.AWSResponse", PatchedAWSResponse
        ), patch(
            "botocore.endpoint.convert_to_response_dict", convert_to_response_dict_patch
        ), patch(
            "aiobotocore.endpoint.convert_to_response_dict", async_convert_to_response_dict_patch
        ):
            return func(*args, **kwargs)

    return wrapped


def mock_s3(func: Callable) -> Callable:
    """
    This is redun's wrapped mock_s3 with applies patch_aws.

    All uses of mock_s3 should use this one in order to make sure the moto
    compatibility fixes are in place.
    """
    return patch_aws(_mock_s3(func))


def get_filesystem_class_mock(
    proto: Optional[str] = None, url: Optional[str] = None
) -> Type[FileSystem]:
    """
    Mock redun filesystem lookup.
    """
    if not proto:
        assert url, "Must give url or proto as argument."
        proto = get_proto(url)
    if proto == "gs":
        return GSFileSystemMock
    else:
        return get_filesystem_class(proto, url)


class GSFileSystemMock(GSFileSystem):
    """
    This class mocks the Google Cloud Storage file system by proxying to s3 mocked by moto.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._s3fs = S3FileSystem()

    def convert_path(self, path: str) -> str:
        """
        Convert path from gs to s3.
        """
        if path.startswith("gs://"):
            return "s3://" + path[5:]
        else:
            return path

    def unconvert_path(self, path: str) -> str:
        """
        Convert path from s3 to gs.
        """
        if path.startswith("s3://"):
            return "gs://" + path[5:]
        else:
            return path

    def _open(self, path: str, mode: str, **kwargs: Any) -> IO:
        """
        Private open method for subclasses to implement.
        """
        path = self.convert_path(path)
        return self._s3fs._open(path, mode, **kwargs)

    def exists(self, path: str) -> bool:
        """
        Returns True if path exists on filesystem.
        """
        return self._s3fs.exists(self.convert_path(path))

    def remove(self, path: str) -> None:
        """
        Delete a path from the filesystem.
        """
        return self._s3fs.remove(self.convert_path(path))

    def mkdir(self, path: str) -> None:
        """
        Creates the directory in the filesystem.
        """
        return self._s3fs.mkdir(self.convert_path(path))

    def rmdir(self, path: str, recursive: bool = False) -> None:
        """
        Removes a directory from the filesystem.
        If `recursive`, removes all contents of the directory.
        Otherwise, raises OSError on non-empty directories
        """
        return self._s3fs.rmdir(self.convert_path(path))

    def get_hash(self, path: str) -> str:
        """
        Return a hash for the file at path.
        """
        return self._s3fs.get_hash(self.convert_path(path))

    def copy(self, src_path: str, dest_path: str) -> None:
        """
        Copy a file from src_path to dest_path.
        """
        return self._s3fs.copy(self.convert_path(src_path), self.convert_path(dest_path))

    def glob(self, pattern: str) -> List[str]:
        """
        Returns filenames matching pattern.
        """
        paths = self._s3fs.glob(self.convert_path(pattern))
        return [self.unconvert_path(path) for path in paths]

    def isfile(self, path: str) -> bool:
        """
        Returns True if path is a file.
        """
        return self._s3fs.isfile(self.convert_path(path))

    def isdir(self, path: str) -> bool:
        """
        Returns True if path is a directory.
        """
        return self._s3fs.isdir(self.convert_path(path))

    def filesize(self, path: str) -> int:
        """
        Returns file size of path in bytes.
        """
        return self._s3fs.filesize(self.convert_path(path))


def clean_dir(path: str) -> None:
    """
    Ensure path exists and is an empty directory.
    """
    if os.path.exists(path):
        shutil.rmtree(path)
    os.makedirs(path)


def get_test_file(filename: str) -> str:
    """
    Returns a file from test_data.
    """
    basedir = os.path.dirname(__file__)
    return os.path.join(basedir, filename)


def use_tempdir(func: Callable) -> Callable:
    """
    Run function within a temporary directory.
    """

    @wraps(func)
    def wrap(*args: Any, **kwargs: Any) -> Any:
        with tempfile.TemporaryDirectory() as tmpdir:
            original_dir = os.getcwd()
            os.chdir(tmpdir)

            try:
                result = func(*args, **kwargs)
            finally:
                os.chdir(original_dir)
        return result

    return wrap


def assert_match_lines(patterns: List[str], lines: List[str]) -> None:
    """
    Asserts whether `lines` match `patterns`.
    """
    assert len(patterns) == len(lines)
    for pattern, line in zip(patterns, lines):
        assert re.fullmatch(pattern, line), f"Could not match `{pattern}` to `{line}`"


def assert_match_text(pattern: str, text: str, wildcard: str = "*"):
    """
    Assert whether two strings are equal using wildcards.
    """
    for i, (a, b) in enumerate(zip_longest(pattern, text)):
        if a != b and a != wildcard:
            assert False, "mismatch on character {}: '{}' != '{}'".format(
                i, pattern[: i + 1], text[: i + 1]
            )


def wait_until(cond: Callable[[], bool], interval: float = 0.02, timeout: float = 2.0) -> None:
    """
    Wait until `cond()` is True or timeout is exceeded.
    """
    start = time.time()
    while not cond():
        if time.time() - start > timeout:
            raise RuntimeError("Timeout")
        time.sleep(interval)


class MatchEnv:
    """

    An environment for generating Match objects.
    """

    def __init__(self):
        self.vars: Dict[str, Any] = {}

    def match(self, *args, **kwargs) -> "Match":
        kwargs["env"] = self
        return Match(*args, **kwargs)


class Match:
    """
    Helper for asserting values have particular properties (types, etc).
    """

    def __init__(
        self,
        type: Optional[Type] = None,
        var: Optional[str] = None,
        regex: Optional[str] = None,
        any: bool = True,
        env: Optional[MatchEnv] = None,
    ):
        self.any = any
        self.type = type
        self.var = var
        self.regex = regex
        self.env = env

    def __repr__(self) -> str:
        if self.var:
            return "Match(var={})".format(self.var)
        elif self.type:
            return "Match(type={})".format(self.type.__name__)
        elif self.regex:
            return "Match(regex={})".format(self.regex)
        elif self.any:
            return "Match(any=True)"
        else:
            return "Match()"

    def __eq__(self, other: Any) -> bool:
        if self.env and self.var:
            # First instance of var will always return True.
            # Second instance of var has to match previous value.
            expected = self.env.vars.setdefault(self.var, other)
            if expected != other:
                return False

        if self.type:
            return isinstance(other, self.type)

        elif self.regex:
            return bool(re.fullmatch(self.regex, other))

        else:
            return self.any


class QueryStats(NamedTuple):
    """
    Stats for a recorded SQLAlchemy query.
    """

    statement: str
    parameters: tuple
    duration: float


@contextmanager
def listen_queries(engine: Any) -> Iterator[List[QueryStats]]:
    """
    Context for capturing SQLAlchemy queries.

    .. code-block:: python

        with listen_queries(engine) as queries:
            result = session.query(Model).filter(...)
            # More SQLAlchemy queries...

        # queries now has a list of statement and parameter tuples.
        assert len(queries) == 2
    """
    queries = []
    cursors = {}

    def before(conn, cursor, statement, parameters, context, executemany):
        cursors[cursor] = time.time()

    def after(conn, cursor, statement, parameters, context, executemany):
        duration = time.time() - cursors.pop(cursor)
        queries.append(QueryStats(statement, parameters, duration))

    sqlalchemy.event.listen(engine, "before_cursor_execute", before)
    sqlalchemy.event.listen(engine, "after_cursor_execute", after)

    yield queries

    sqlalchemy.event.remove(engine, "before_cursor_execute", before)
    sqlalchemy.event.remove(engine, "after_cursor_execute", after)


def import_all_modules(pkg):
    """Import (almost) all modules within a package.

    Ignores explicitly marked modules.
    """
    ignored_modules = (
        "redun.backends.db.alembic.env",
        "redun.visualize",
    )  # https://stackoverflow.com/a/52575218
    modules = []
    for _, module_name, is_pkg in pkgutil.iter_modules(pkg.__path__):
        full_name = f"{pkg.__name__}.{module_name}"
        if full_name in ignored_modules:
            continue

        module = importlib.import_module(full_name)
        if is_pkg:
            modules.extend(import_all_modules(module))
        else:
            modules.append(module)

    return modules


def get_docstring_owners_in_module(module):
    """Get all functions, classes and their methods defined within a python module.

    Returns
    -------
    docstring_owners : set
        Set of functions, classes and methods
    """
    assert ismodule(module), f"Passed {module.__name__} which is not a module."

    def is_valid(obj):
        if getmodule(obj) == module:
            if ismethod(obj) or isfunction(obj):
                return not obj.__name__.startswith("_")
            if isclass(obj):
                return True
        return False

    to_check = {obj for _, obj in getmembers(module) if is_valid(obj)}
    docstring_owners = set()
    seen = set()

    while to_check:
        candidate = to_check.pop()
        if candidate in seen:
            continue

        if isfunction(candidate) or ismethod(candidate):
            docstring_owners.add(candidate)

        if isclass(candidate):
            to_check.update({obj for _, obj in getmembers(candidate) if is_valid(obj)})

        seen.add(candidate)
    return docstring_owners


def docstring_owner_pretty_name(docstring_owner):
    return ".".join((docstring_owner.__module__, docstring_owner.__qualname__))


def mock_scheduler():
    """
    Returns a scheduler with mocks for job completion.
    """
    # Setup scheduler callbacks.
    scheduler = Scheduler()

    scheduler.job_results = {}
    scheduler.job_errors = {}

    def done_job(job, result, job_tags=[]):
        job.job_tags.extend(job_tags)
        scheduler.job_results[job.id] = result

    def reject_job(job, error, error_traceback=None, job_tags=[]):
        if job:
            job.job_tags.extend(job_tags)
            scheduler.job_errors[job.id] = error
        else:
            # Scheduler error, reraise it.
            scheduler.job_errors[None] = error
            raise error

    def batch_wait(job_ids):
        while not all(
            job_id in scheduler.job_results or job_id in scheduler.job_errors for job_id in job_ids
        ):
            time.sleep(0.1)

    scheduler.done_job = done_job
    scheduler.reject_job = reject_job
    scheduler.batch_wait = batch_wait

    return scheduler
