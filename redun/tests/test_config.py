import os
from configparser import SectionProxy
from unittest.mock import patch

import pytest

from redun.cli import get_config_dir, postprocess_config, setup_scheduler
from redun.config import Config
from redun.tests.utils import use_tempdir


def test_config_parse_sections():
    config_string = """
[executors.batch]
type = aws_batch
image = 123.abc.ecr.us-west-2.amazonaws.com/amazonlinux-python3
queue = queue
s3_scratch = s3://example-bucket/redun/

[executors.batch2]
type = aws_batch
image = 123.abc.ecr.us-west-2.amazonaws.com/amazonlinux-python3
queue = queue2
s3_scratch = s3://example-bucket/redun/
job_def_extra = {"a_json_key": {"a_json_nested_key": 42}}
"""

    config = Config()
    config.read_string(config_string)

    assert sorted(config.keys()) == ["executors"]
    assert config["executors"]["batch"]["type"] == "aws_batch"
    assert config["executors"]["batch"]["queue"] == "queue"
    assert config["executors"]["batch2"]["type"] == "aws_batch"
    assert config["executors"]["batch2"]["queue"] == "queue2"
    # ensure json property is loaded as text
    assert config["executors"]["batch2"]["job_def_extra"] == (
        '{"a_json_key": {"a_json_nested_key": 42}}'
    )


@pytest.fixture
def sample_config():
    return Config(
        {
            "executors.batch": {
                "type": "aws_batch",
                "image": "123.abc.ecr.us-west-2.amazonaws.com/amazonlinux-python3",
                "queue": "queue",
                "s3_scratch": "s3://example-bucket/redun/",
            },
            "executors.batch2": {
                "type": "aws_batch",
                "image": "123.abc.ecr.us-west-2.amazonaws.com/amazonlinux-python3",
                "queue": "queue2",
                "s3_scratch": "s3://example-bucket/redun/",
            },
        }
    )


def test_config_dict(sample_config):
    config = sample_config

    assert sorted(config.keys()) == ["executors"]
    assert config["executors"]["batch"]["type"] == "aws_batch"
    assert config["executors"]["batch"]["queue"] == "queue"
    assert config["executors"]["batch2"]["type"] == "aws_batch"
    assert config["executors"]["batch2"]["queue"] == "queue2"


def test_get_config_dict(sample_config):
    """Assert get_config_dict() reproduces a Config correctly"""
    config = Config(config_dict=sample_config.get_config_dict())
    assert sorted(config.keys()) == ["executors"]
    assert config["executors"]["batch"]["type"] == "aws_batch"
    assert config["executors"]["batch"]["queue"] == "queue"
    assert config["executors"]["batch2"]["type"] == "aws_batch"
    assert config["executors"]["batch2"]["queue"] == "queue2"


@use_tempdir
def test_get_config_dict_replace_config_dir(sample_config):
    """Verify replace_config_dir behavior"""
    os.makedirs(".redun")
    with open(".redun/redun.ini", "w") as out:
        out.write(
            """
[backend]
db_uri = sqlite:///redun.db
"""
        )

    scheduler = setup_scheduler()

    # Ensure initial config dir and files are created.
    assert os.path.exists(".redun/redun.ini")
    assert os.path.exists(".redun/redun.db")

    conf = scheduler.config.get_config_dict()
    local_config_dir = get_config_dir()
    db_uri = conf["backend"]["db_uri"]
    assert db_uri == f"sqlite:///{local_config_dir}/redun.db"
    assert conf["backend"]["config_dir"] == local_config_dir
    assert conf["repos.default"]["config_dir"] == local_config_dir

    replacement = "/tmp/xyz"
    conf = scheduler.config.get_config_dict(replace_config_dir=replacement)
    db_uri = conf["backend"]["db_uri"]
    assert db_uri == f"sqlite:///{replacement}/redun.db"
    assert conf["backend"]["config_dir"] == replacement
    assert conf["repos.default"]["config_dir"] == replacement


def test_postprocess_config(sample_config):
    """
    Default sections must be true Sections.
    If they aren't an exception will occur when get_config_dict() is called
    """
    post_processed_config = postprocess_config(sample_config, "/tmp/config_dir")
    config = Config(config_dict=post_processed_config.get_config_dict())

    assert sorted(config.keys()) == ["backend", "executors", "repos"]
    config = postprocess_config(config, "/tmp/config_dir")
    # Default repository should be added since it was missing
    assert config["repos"]["default"]["config_dir"] == "/tmp/config_dir"
    # db_uri should be converted to absolute absolute config_dir
    assert config["backend"]["db_uri"] == "sqlite:////tmp/config_dir/redun.db"

    # Ensure that the auto-added sections are truly Section objects and not plain dicts.
    assert isinstance(config["repos"]["default"], SectionProxy)
    assert isinstance(config["backend"], SectionProxy)


def test_config_env_vars():
    """
    Environment variables should be replaced in config values.
    """
    config_string = """
[DEFAULT]
default_scratch = s3://example-bucket/redun/
queue = default-queue

[executors.batch]
type = aws_batch
image = 123.abc.ecr.us-west-2.amazonaws.com/amazonlinux-python3
queue = ${QUEUE}
s3_scratch = ${default_scratch}
role = ${ROLE}
"""

    with patch("os.environ", {"ROLE": "my-role", "QUEUE": "my-queue"}):
        config = Config()
        config.read_string(config_string)

        assert config["executors"]["batch"]["role"] == "my-role"

        # We should be able to reference default values.
        assert config["executors"]["batch"]["s3_scratch"] == "s3://example-bucket/redun/"

        # The environment variable should override the default value.
        assert config["executors"]["batch"]["queue"] == "my-queue"


def test_config_case_sensitive():
    """
    Config variables should be case sensitive.
    """
    config_string = """
[section]
key = foo
KEY = bar
"""

    config = Config()
    config.read_string(config_string)

    assert config["section"]["key"] == "foo"
    assert config["section"]["KEY"] == "bar"
