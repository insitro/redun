from configparser import SectionProxy

import pytest

from redun.cli import postprocess_config
from redun.config import Config


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
"""

    config = Config()
    config.read_string(config_string)

    assert sorted(config.keys()) == ["executors"]
    assert config["executors"]["batch"]["type"] == "aws_batch"
    assert config["executors"]["batch"]["queue"] == "queue"
    assert config["executors"]["batch2"]["type"] == "aws_batch"
    assert config["executors"]["batch2"]["queue"] == "queue2"


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
