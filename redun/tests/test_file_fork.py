"""
Tests to confirm that redun.file backends support forking subprocesses, not just spawn.
"""

import multiprocessing as mp
import os
from concurrent.futures import ProcessPoolExecutor
from functools import partial
from io import BytesIO
from typing import List, Literal
from unittest.mock import MagicMock, Mock, patch

import adlfs
import boto3
import fsspec.asyn
import pytest
from tqdm import trange

import redun
from redun import File
from redun.tests.utils import mock_s3

NOT_SET = "NOT SET"
S3_PATH = os.environ.get("REDUN_TEST_FILE_FORK_S3_PATH", NOT_SET)
AZ_PATH = os.environ.get("REDUN_TEST_FILE_FORK_AZ_PATH", NOT_SET)


def mock_azure_blob_file_system():
    m = MagicMock(adlfs.AzureBlobFileSystem)
    m.return_value.open = lambda *args, **kwargs: BytesIO(b"hello")
    return m


def reset_redun_file():
    redun.file._filesystem_instances.clear()


@pytest.mark.parametrize(
    "path, multiprocessing_context",
    [(S3_PATH, "fork"), (S3_PATH, "spawn"), (AZ_PATH, "spawn"), (AZ_PATH, "fork")],
)
@patch("fsspec.asyn.reset_lock", side_effect=fsspec.asyn.reset_lock)
def test_supports_fork_integration(
    reset_lock: Mock, path: str, multiprocessing_context: Literal["spawn", "fork"]
):
    if path is NOT_SET:
        pytest.skip(
            "Please set environment variables REDUN_TEST_FILE_FORK_S3_PATH "
            "and REDUN_TEST_FILE_FORK_AZ_PATH to run this test."
        )

    reset_redun_file()

    num_processes = 2
    assert File(path).exists()

    actual = run_dataloaders(
        path, multiprocessing_context, num_processes=num_processes, num_items=2
    )
    assert actual == list(range(num_processes))


@mock_s3
@patch("fsspec.asyn.reset_lock", side_effect=fsspec.asyn.reset_lock)
def test_s3_supports_fork(reset_lock: Mock):
    reset_redun_file()

    s3_client = boto3.client("s3", region_name="us-east-1")
    s3_client.create_bucket(Bucket="example-bucket")
    path = "s3://example-bucket/mock.txt"
    file = File(path)
    file.write("hello")

    num_processes = 2
    assert File(path).exists()

    actual = run_dataloaders(path, "fork", num_processes=num_processes, num_items=2)
    assert actual == list(range(num_processes))


@pytest.mark.parametrize("multiprocessing_context", ["fork", "spawn", "none"])
@patch("adlfs.AzureBlobFileSystem")
@patch("fsspec.asyn.reset_lock", side_effect=fsspec.asyn.reset_lock)
@patch("redun.azure_utils.get_az_credential", return_value="mock_credential")
def test_az_supports_fork(
    mock_credential: Mock,
    reset_lock: Mock,
    azure_file_system: Mock,
    multiprocessing_context: Literal["fork", "spawn", "none"],
):
    reset_redun_file()

    azure_file_system.return_value.open = lambda *args, **kwargs: BytesIO(b"hello")

    path = "az://example-container@example-account.blob.core.windows.net/mock.txt"
    num_processes = 2
    assert File(path).exists()

    actual = run_dataloaders(
        path, multiprocessing_context, num_processes=num_processes, num_items=2
    )
    assert actual == (list(range(num_processes)) if multiprocessing_context != "none" else [-1])

    # The following assert tests that reset_lock is called at most once
    # If it's called multiple times, it means something's wrong. File access will work but be
    # slower than it should be. In reality, it's always going to be called at least once,
    # but on the CI/CD test run, it sometimes gets called on just the real method and
    # not the mock, so we miss it. If it actually never gets called, we'll know, because the
    # calls to open the file will hang!
    assert reset_lock.call_count <= 1

    azure_file_system.assert_called_once()


def run_single_dataloader(num_items: int, path: str, process_idx: int):
    if "mock" in path:
        return run_single_dataloader_mocked(num_items, path, process_idx)

    for item in trange(num_items):
        assert len(File(path).read("rb")) > 0

    return process_idx


@patch("adlfs.AzureBlobFileSystem")
@patch("fsspec.asyn.reset_lock", side_effect=fsspec.asyn.reset_lock)
@patch("redun.azure_utils.get_az_credential", return_value="mock_credential")
def run_single_dataloader_mocked(
    num_items: int,
    path: str,
    process_idx: int,
    mock_credential: Mock,
    reset_lock: Mock,
    azure_file_system: Mock,
):
    azure_file_system.return_value.open = lambda *args, **kwargs: BytesIO(b"hello")
    for item in trange(num_items):
        assert len(File(path).read("rb")) > 0
    if process_idx != -1:
        # See comments on the other reset_lock assert in test_az_supports_fork
        assert reset_lock.call_count <= 1
        if path.startswith("az://"):
            azure_file_system.call_count <= 1
    return process_idx


def run_dataloaders(
    path: str,
    multiprocessing_context: Literal["spawn", "fork", "none"],
    num_processes: int = 2,
    num_items: int = 100,
) -> List[int]:
    assert File(path).exists()

    if num_processes == 0 or multiprocessing_context == "none":
        return [run_single_dataloader(num_items, path, -1)]

    mp_context = dict(spawn=mp.context.SpawnContext, fork=mp.context.ForkContext)[
        multiprocessing_context
    ]()

    with ProcessPoolExecutor(max_workers=num_processes, mp_context=mp_context) as pool:
        fcn = partial(run_single_dataloader, num_items, path)
        results = list(pool.map(fcn, range(num_processes)))

    return results
