import sys
from unittest.mock import MagicMock, patch

import boto3
import pytest
from botocore.exceptions import ClientError

from redun import Dir, File
from redun.hashing import hash_struct
from redun.tests.utils import mock_s3


@mock_s3
@pytest.mark.skipif(
    sys.version_info < (3, 7), reason="moto requires python3.7 or higher for this test"
)
def test_file_api():
    client = boto3.client("s3", region_name="us-east-1")
    client.create_bucket(Bucket="example-bucket")

    path = "s3://example-bucket/tmp/"
    dir = Dir(path)
    empty_dir_hash = dir.hash

    assert not dir.exists()
    dir.rmdir()  # Should be safe when nonexistent

    assert not dir.exists()
    dir.mkdir()
    assert dir.exists()

    file = dir.file("hello.txt")

    dir_content_hash = dir.hash
    assert dir_content_hash != empty_dir_hash

    file.remove()
    assert not file.exists()

    # Removing a file that doesn't exist should be safe.
    file.remove()

    with file.open("w") as out:
        out.write("hello")

    assert file.exists()

    assert file.read() == "hello"
    assert file.size() == 5

    hash = file.get_hash()

    with file.open("w") as out:
        out.write("hello2")
    assert file.get_hash() != hash

    file2 = File("s3://example-bucket/tmp/hello2.txt")
    file.copy_to(file2)
    assert file2.read() == "hello2"

    assert dir.hash == dir_content_hash

    dir.rmdir(recursive=False)  # removes tmp/ but not contents
    assert file.exists()  # but tmp/hello.txt still exists
    assert dir.exists()  # so tmp/ does too

    dir.rmdir(recursive=True)
    assert not file2.exists()
    assert not dir.exists()


@mock_s3
def test_exists_cache() -> None:
    """
    File.exists() should not use s3fs cache.
    """
    s3_client = boto3.client("s3", region_name="us-east-1")
    s3_client.create_bucket(Bucket="example-bucket")

    # Object should not exist.
    assert not File("s3://example-bucket/a").exists()
    assert File("s3://example-bucket/a").get_hash() == "cb7880ecc11723b8b8cad37f6b5160251d7a765e"

    # Update object outside of s3fs.
    s3_client.put_object(Body=b"hello", Bucket="example-bucket", Key="a")

    # Using the normal s3fs exists(), the existence check would be cached and
    # would now return an incorrect result.

    # However, File.exists() avoids using the s3fs cache and gives the correct result.
    # The hash should update as well.
    assert File("s3://example-bucket/a").exists()
    assert File("s3://example-bucket/a").get_hash() == "ea438dc20234f0226736d407d7caba13f7e3c49e"

    # Directory should not exist.
    assert not Dir("s3://example-bucket/dir/").exists()

    # Update object outside of s3fs.
    s3_client.put_object(Body=b"hello", Bucket="example-bucket", Key="dir/a")

    # Directory should now exist.
    assert Dir("s3://example-bucket/dir/").exists()


@mock_s3
def test_dir_hash_consistency():
    client = boto3.client("s3", region_name="us-east-1")
    client.create_bucket(Bucket="example-bucket")

    # empty dir hash
    path = "s3://example-bucket/tmp/"
    dir = Dir(path)
    expected_empty_dir_hash = hash_struct(
        [dir.type_basename, dir.path] + sorted(file.hash for file in list(dir))
    )
    assert dir.hash == expected_empty_dir_hash

    file1 = dir.file("hello1.txt")
    file2 = dir.file("hello2.txt")

    with file1.open("w") as out:
        out.write("hello1")

    with file2.open("w") as out:
        out.write("hello2")

    expected_dir_hash = hash_struct(
        [dir.type_basename, dir.path] + sorted(file.hash for file in list(dir))
    )
    dir._hash = None  # clear hash to force recompute
    assert dir.hash != expected_empty_dir_hash
    assert dir.hash == expected_dir_hash


@mock_s3
def test_dir_error_handling():
    client = boto3.client("s3", region_name="us-east-1")
    client.create_bucket(Bucket="example-bucket")

    path = "s3://example-bucket/tmp/"
    dir = Dir(path)
    dir.mkdir()

    error_response = {
        "Error": {"Code": "403", "Message": "Forbidden"},
        "ResponseMetadata": {"HTTPStatusCode": 403},
    }

    s3_client = dir.filesystem.s3_raw
    mock_paginator = MagicMock()
    mock_paginator.paginate.side_effect = ClientError(error_response, "ListObjectsV2")

    dir._hash = None  # clear hash to force recompute
    with patch.object(s3_client, "get_paginator", return_value=mock_paginator):
        with pytest.raises(ClientError) as exc_info:
            dir.hash
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 403


@mock_s3
def test_file_error_handling():
    client = boto3.client("s3", region_name="us-east-1")
    client.create_bucket(Bucket="example-bucket")

    path = "s3://example-bucket/tmp/"
    dir = Dir(path)
    dir.mkdir()
    file = dir.file("hello.txt")
    s3_client = dir.filesystem.s3_raw

    error_response = {
        "Error": {"Code": "403", "Message": "Forbidden"},
        "ResponseMetadata": {"HTTPStatusCode": 403},
    }
    file._hash = None  # clear hash to force recompute
    with patch.object(
        s3_client, "head_object", side_effect=ClientError(error_response, "HeadObject")
    ):
        with pytest.raises(ClientError) as exc_info:
            file.hash
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 403

    nonexistent_file = File("s3://example-bucket/nonexistent.txt")
    assert nonexistent_file.hash, "Hash should be computed without error for nonexistent files"
