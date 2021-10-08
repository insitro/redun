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


def test_config_dict():
    config = Config(
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

    assert sorted(config.keys()) == ["executors"]
    assert config["executors"]["batch"]["type"] == "aws_batch"
    assert config["executors"]["batch"]["queue"] == "queue"
    assert config["executors"]["batch2"]["type"] == "aws_batch"
    assert config["executors"]["batch2"]["queue"] == "queue2"
