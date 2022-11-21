import os
from typing import cast

import boto3

from redun import task
from redun.backends.db import MAX_VALUE_SIZE_PREVIEW, PreviewValue, RedunBackendDb, Value
from redun.config import Config
from redun.file import Dir
from redun.scheduler import Scheduler
from redun.tests.utils import mock_s3


def test_value_store(tmpdir) -> None:
    """
    Values are stored on filesystem when configured with value_store_path.
    """

    @task()
    def add1(x: int) -> int:
        return x + 1

    value_store_path = os.path.join(tmpdir, "values/")

    # Use the default value store min size.
    config = {"backend": {"value_store_path": value_store_path}}
    scheduler = Scheduler(config=Config(config))
    assert scheduler.run(add1(10)) == 11

    # All of these values are below the min size so we do not expect anything
    # written in the value store.
    directory = Dir(value_store_path)
    assert not directory.exists()

    # Generate new values.
    @task()
    def add100(x: int) -> int:
        return x + 100

    # Set the value store min size to zero.
    config = {"backend": {"value_store_path": value_store_path, "value_store_min_size": "0"}}
    scheduler = Scheduler(config=Config(config))
    assert scheduler.run(add100(11)) == 111

    # Expect all values written to the value store.
    directory = Dir(value_store_path)
    assert directory.exists()
    assert len(list(directory)) == 3  # Input, output and task.


@mock_s3
def test_value_store_s3() -> None:
    """
    Values are stored on s3 when configured with proper value_store_path.
    """

    client = boto3.client("s3", region_name="us-east-1")
    client.create_bucket(Bucket="example-bucket")

    @task()
    def add1(x: int) -> int:
        return x + 1

    value_store_path = "s3://example-bucket/values/"

    # Use the default value store min size.
    config = {"backend": {"value_store_path": value_store_path}}
    scheduler = Scheduler(config=Config(config))
    assert scheduler.run(add1(10)) == 11

    # All of these values are below the min size so we do not expect anything
    # written in the value store.
    directory = Dir(value_store_path)
    assert not directory.exists()

    # Generate new values.
    @task()
    def add100(x: int) -> int:
        return x + 100

    # Set the value store min size to zero.
    config = {"backend": {"value_store_path": value_store_path, "value_store_min_size": "0"}}
    scheduler = Scheduler(config=Config(config))
    assert scheduler.run(add100(11)) == 111

    # Expect all values written to the value store.
    directory = Dir(value_store_path)
    assert directory.exists()
    assert len(list(directory)) == 3  # Input, output and task.


def test_preview_value(tmpdir) -> None:
    """
    We should be able to preview large values.
    """

    @task()
    def make_str(n: int) -> str:
        return "A" * n

    # Setup a scheduler with a value store.
    value_store_path = os.path.join(tmpdir, "values/")
    config = {"backend": {"value_store_path": value_store_path, "value_store_min_size": "100"}}
    scheduler = Scheduler(config=Config(config))
    backend = cast(RedunBackendDb, scheduler.backend)
    assert backend.session

    # Let's make a small value.
    small_result = scheduler.run(make_str(3))
    assert small_result == "AAA"

    # Let's make a large value.
    value_size = MAX_VALUE_SIZE_PREVIEW + 10
    large_result = scheduler.run(make_str(value_size))
    assert len(large_result) == value_size

    # Small value should preview as itself.
    value_hash = scheduler.type_registry.get_hash(small_result)
    value_row = backend.session.query(Value).filter(Value.value_hash == value_hash).one()
    assert value_row.preview == small_result

    # Large value should preview as a PreviewValue object.
    value_hash = scheduler.type_registry.get_hash(large_result)
    value_row = backend.session.query(Value).filter(Value.value_hash == value_hash).one()
    preview = value_row.preview
    assert isinstance(preview, PreviewValue)
    assert str(preview) == "builtins.str(hash=c901f470, size=1000020)"
