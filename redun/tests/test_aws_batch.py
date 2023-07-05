import json
import os
import pickle
import time
import unittest.mock
import uuid
from typing import Any, Dict, cast
from unittest.mock import Mock, patch

import boto3
import pytest
from freezegun import freeze_time
from moto import mock_logs

import redun.executors.aws_batch
import redun.tests.utils
from redun import File, job_array, task
from redun.cli import RedunClient, import_script
from redun.config import Config
from redun.executors.aws_batch import (
    BATCH_JOB_STATUSES,
    BATCH_LOG_GROUP,
    FAILED,
    SUCCEEDED,
    AWSBatchExecutor,
    batch_submit,
    equiv_job_def,
    get_batch_job_name,
    get_hash_from_job_name,
    get_job_definition,
    get_job_details,
    get_job_log_stream,
    get_or_create_job_definition,
    iter_batch_job_log_lines,
    iter_batch_job_logs,
    make_job_def_name,
    parse_job_error,
    submit_task,
)
from redun.executors.code_packaging import create_tar, package_code
from redun.executors.command import REDUN_REQUIRED_VERSION
from redun.executors.scratch import (
    SCRATCH_ERROR,
    SCRATCH_HASHES,
    SCRATCH_INPUT,
    SCRATCH_OUTPUT,
    ExceptionNotFoundError,
    get_array_scratch_file,
    get_job_scratch_file,
)
from redun.file import Dir
from redun.scheduler import Execution, Job, Scheduler, Traceback
from redun.scripting import ScriptError
from redun.tests.utils import mock_s3, mock_scheduler, use_tempdir, wait_until
from redun.utils import pickle_dumps


def test_make_job_def_name() -> None:
    """
    Job definition names should autogenerate from docker image names.
    """
    assert make_job_def_name("my-image") == "my-image-jd"
    assert make_job_def_name("my-image", "-job-def") == "my-image-job-def"
    assert make_job_def_name("my.image") == "myimage-jd"
    assert make_job_def_name("a" * 200) == ("a" * 125) + "-jd"


@patch("redun.executors.aws_utils.get_aws_client")
def test_get_job_definition(get_aws_client_mock) -> None:
    """
    Most recent active revision should be returned or empty dict if no matching job defs.
    """
    # Simulate case where no matching jobs are returned from batch API.
    get_aws_client_mock.return_value.describe_job_definitions.return_value = {"jobDefinitions": []}
    assert get_job_definition("JOB_DEF_1") == {}

    # Simulate case where there are multiple revisions, confirm newest is returned.
    get_aws_client_mock.return_value.describe_job_definitions.return_value = {
        "jobDefinitions": [
            {"revision": 1, "jobDefinitionArn": "ARN1"},
            {"revision": 3, "jobDefinitionArn": "ARN3"},
            {"revision": 2, "jobDefinitionArn": "ARN2"},
        ]
    }
    # Need to change the job def name here from the first case above to avoid the lru cache on
    # get_job_definition.
    job_def = get_job_definition("JOB_DEF_2")
    assert job_def["jobDefinitionArn"] == "ARN3"


@patch("redun.executors.aws_utils.get_aws_client")
@patch("redun.executors.aws_batch.get_job_definition")
def test_required_job_def_name(get_job_definition_mock, _) -> None:
    """
    Confirm that job_def_name is required when autocreate_job_def is False.
    """
    # A job_def_name is required when autocreate is False.
    with pytest.raises(AssertionError):
        batch_submit({"command": ["ls"]}, "queue", "image", autocreate_job_def=False)

    # When the required job_def_name is supplied, an error should be raised if a matching
    # definition cannot be found.
    get_job_definition_mock.return_value = {}
    with pytest.raises(ValueError):
        batch_submit(
            {"command": ["ls"]},
            "queue",
            "image",
            job_def_name="NONEXISTENT",
            autocreate_job_def=False,
        )


@pytest.mark.parametrize("array,suffix", [(False, ""), (True, "-array")])
def test_get_hash_from_job_name(array, suffix) -> None:
    """
    Returns the Job hash from a AWS Batch job name.
    """
    prefix = "my-job-prefix"
    job_hash = "c000d7f9b6275c58aff9d5466f6a1174e99195ca"
    job_name = get_batch_job_name(prefix, job_hash, array=array)
    assert job_name.startswith(prefix)
    assert job_name.endswith(suffix)

    job_hash2 = get_hash_from_job_name(job_name)
    assert job_hash2 == job_hash


@patch("redun.executors.aws_utils.get_aws_client")
def test_get_or_create_job_definition(get_aws_client_mock) -> None:
    """
    Confirm that jobs are correctly fetched or created.
    """

    # Get mocks.
    describe_job_definitions = get_aws_client_mock.return_value.describe_job_definitions
    register_job_definition = get_aws_client_mock.return_value.register_job_definition

    # Let's skip the LRU cache for this test.
    _get_or_create_job_definition = get_or_create_job_definition.__wrapped__

    # 1. Jobs get created
    describe_job_definitions.return_value = {"jobDefinitions": []}
    register_job_definition.return_value = {"jobId": "new-job"}
    result = _get_or_create_job_definition(
        job_def_name="test-jd", image="an-image:latest", role="aRole"
    )

    assert result == {"jobId": "new-job"}
    job_def = dict(
        jobDefinitionName="test-jd",
        type="container",
        containerProperties={
            "command": ["ls"],
            "image": "an-image:latest",
            "vcpus": 1,
            "memory": 4,
            "jobRoleArn": "aRole",
            "environment": [],
            "mountPoints": [],
            "volumes": [],
            "resourceRequirements": [],
            "ulimits": [],
            "privileged": False,
        },
    )
    register_job_definition.assert_called_with(**job_def)

    # 2a. Identical jobs are not recreated
    describe_job_definitions.return_value = {"jobDefinitions": [job_def]}
    register_job_definition.reset_mock()
    _get_or_create_job_definition(job_def_name="test-jd", image="an-image:latest", role="aRole")
    register_job_definition.assert_not_called()

    # 2.b. resource differences are ignored on single node
    _get_or_create_job_definition(
        job_def_name="test-jd",
        image="an-image:latest",
        role="aRole",
        vcpus=2,
        memory=8,
    )
    register_job_definition.assert_not_called()

    # 2.c. The newer revision is preferred.
    describe_job_definitions.return_value = {
        "jobDefinitions": [
            {**job_def, "jobDefinitionName": "test-jd:3"},
            {**job_def, "jobDefinitionName": "test-jd:10"},
            {**job_def, "jobDefinitionName": "test-jd:1"},
            {**job_def, "jobDefinitionName": "test-jd:2"},
        ]
    }
    register_job_definition.reset_mock()
    result = _get_or_create_job_definition(
        job_def_name="test-jd",
        image="an-image:latest",
        role="aRole",
    )
    assert result["jobDefinitionName"] == "test-jd:10"
    register_job_definition.assert_not_called()

    # 3. Extra keys are ignored
    describe_job_definitions.return_value = {
        "jobDefinitions": [{"extraKey": "ignored", **job_def}]
    }
    register_job_definition.reset_mock()
    _get_or_create_job_definition(job_def_name="test-jd3", image="an-image:latest", role="aRole")
    register_job_definition.assert_not_called()

    # 4. Important differences trigger recreation
    # 4.a. job type
    describe_job_definitions.return_value = {
        "jobDefinitions": [
            {
                "jobDefinitionName": "test-jd4a",
                "type": "other",
                "containerProperties": {
                    "command": ["ls"],
                    "image": "an-image:latest",
                    "vcpus": 2,
                    "memory": 4,
                    "jobRoleArn": "aRole",
                    "environment": [],
                    "mountPoints": [],
                    "volumes": [],
                    "resourceRequirements": [],
                    "ulimits": [],
                    "privileged": False,
                },
            }
        ]
    }
    register_job_definition.reset_mock()
    result = _get_or_create_job_definition(
        job_def_name="test-jd4a", image="an-image:latest", role="aRole"
    )
    assert result == {"jobId": "new-job"}
    register_job_definition.assert_called_with(
        jobDefinitionName="test-jd4a",
        type="container",
        containerProperties={
            "command": ["ls"],
            "image": "an-image:latest",
            "vcpus": 1,
            "memory": 4,
            "jobRoleArn": "aRole",
            "environment": [],
            "mountPoints": [],
            "volumes": [],
            "resourceRequirements": [],
            "ulimits": [],
            "privileged": False,
        },
    )

    # 4.b. num_nodes
    describe_job_definitions.return_value = {
        "jobDefinitions": [
            {
                "jobDefinitionName": "test-jd4b",
                "type": "container",
                "containerProperties": {
                    "command": ["ls"],
                    "image": "an-image:latest",
                    "vcpus": 2,
                    "memory": 4,
                    "jobRoleArn": "aRole",
                    "environment": [],
                    "mountPoints": [],
                    "volumes": [],
                    "resourceRequirements": [],
                    "ulimits": [],
                    "privileged": False,
                },
            }
        ]
    }
    register_job_definition.reset_mock()
    _get_or_create_job_definition(
        job_def_name="test-jd4b", image="an-image:latest", role="aRole", num_nodes=3
    )
    multinode_job = {
        "jobDefinitionName": "test-jd4b",
        "type": "multinode",
        "nodeProperties": {
            "mainNode": 0,
            "numNodes": 3,
            "nodeRangeProperties": [
                {
                    "container": {
                        "command": ["ls"],
                        "image": "an-image:latest",
                        "vcpus": 1,
                        "memory": 4,
                        "jobRoleArn": "aRole",
                        "environment": [],
                        "mountPoints": [],
                        "volumes": [],
                        "resourceRequirements": [],
                        "ulimits": [
                            {
                                "name": "nofile",
                                "softLimit": 65535,
                                "hardLimit": 65535,
                            }
                        ],
                        "privileged": False,
                    },
                    "targetNodes": "0",
                },
                {
                    "container": {
                        "command": ["ls"],
                        "image": "an-image:latest",
                        "vcpus": 1,
                        "memory": 4,
                        "jobRoleArn": "aRole",
                        "environment": [],
                        "mountPoints": [],
                        "volumes": [],
                        "resourceRequirements": [],
                        "ulimits": [
                            {
                                "name": "nofile",
                                "softLimit": 65535,
                                "hardLimit": 65535,
                            }
                        ],
                        "privileged": False,
                    },
                    "targetNodes": "1:",
                },
            ],
        },
    }
    register_job_definition.assert_called_with(**multinode_job)

    # 4c. Multinode resource definitions do not trigger changes
    describe_job_definitions.return_value = {"jobDefinitions": [multinode_job]}
    register_job_definition.reset_mock()
    _get_or_create_job_definition(
        job_def_name="test-jd4b",
        image="an-image:latest",
        role="aRole",
        num_nodes=3,
        vcpus=2,
        memory=8,
    )
    register_job_definition.assert_not_called()

    # 4.e. image
    describe_job_definitions.return_value = {
        "jobDefinitions": [
            {
                "jobDefinitionName": "test-jd4a",
                "type": "other",
                "containerProperties": {
                    "command": ["ls"],
                    "image": "an-image:latest",
                    "vcpus": 2,
                    "memory": 4,
                    "jobRoleArn": "aRole",
                    "environment": [],
                    "mountPoints": [],
                    "volumes": [],
                    "resourceRequirements": [],
                    "ulimits": [],
                    "privileged": False,
                },
            }
        ]
    }
    register_job_definition.reset_mock()
    result = _get_or_create_job_definition(
        job_def_name="test-jd4a", image="an-image:custom", role="aRole"
    )
    assert result == {"jobId": "new-job"}
    register_job_definition.assert_called_with(
        jobDefinitionName="test-jd4a",
        type="container",
        containerProperties={
            "command": ["ls"],
            "image": "an-image:custom",
            "vcpus": 1,
            "memory": 4,
            "jobRoleArn": "aRole",
            "environment": [],
            "mountPoints": [],
            "volumes": [],
            "resourceRequirements": [],
            "ulimits": [],
            "privileged": False,
        },
    )

    # 5. Passing extra job configuration options
    ulimit_option = {
        "containerProperties": {
            "ulimits": [
                {
                    "name": "nofile",
                    "softLimit": 2048,
                    "hardLimit": 2048,
                }
            ]
        }
    }

    describe_job_definitions.return_value = {"jobDefinitions": [job_def]}
    register_job_definition.reset_mock()
    _get_or_create_job_definition(
        job_def_name="test-jd", image="an-image:latest", role="aRole", job_def_extra=ulimit_option
    )
    register_job_definition.assert_called_with(
        jobDefinitionName="test-jd",
        type="container",
        containerProperties={
            "command": ["ls"],
            "image": "an-image:latest",
            "vcpus": 1,
            "memory": 4,
            "jobRoleArn": "aRole",
            "environment": [],
            "mountPoints": [],
            "volumes": [],
            "resourceRequirements": [],
            "ulimits": [
                {
                    "name": "nofile",
                    "softLimit": 2048,
                    "hardLimit": 2048,
                }
            ],
            "privileged": False,
        },
    )


@patch("redun.executors.aws_utils.get_aws_client")
def test_job_definition_role(get_aws_client_mock) -> None:
    """
    Confirm that jobs roles are set correctly.
    """

    # The default role is the AWS account's ecsTaskExecutionRole.
    details = get_job_details("image")
    assert details["containerProperties"]["jobRoleArn"].endswith("role/ecsTaskExecutionRole")

    # If specified, use the user's role.
    details = get_job_details("image", role="aRole")
    assert details["containerProperties"]["jobRoleArn"] == "aRole"

    # If the "none" role is specified, do not set the role.
    details = get_job_details("image", role="none")
    assert "jobRoleArn" not in details["containerProperties"]


@patch("redun.executors.aws_utils.get_aws_client")
@patch("redun.executors.aws_batch.get_or_create_job_definition")
def test_job_submit_resources(get_or_create_job_definition_mock, get_aws_client_mock) -> None:
    """
    Confirm that resource overrides are properly applied.
    """

    get_or_create_job_definition_mock.return_value = {"jobDefinitionArn": "test-jd-arn"}

    # 1. Single node
    batch_submit(
        batch_job_args={"containerOverrides": {"command": ["test_command"]}, "user": "user-test"},
        job_name="test",
        queue="test_queue",
        image="test_image",
        vcpus=4,
        memory=8,
        gpus=2,
    )

    # Check that the resources are applied but the custom commands are specifically not touched.
    get_aws_client_mock.return_value.submit_job.assert_called_with(
        jobName="test",
        jobQueue="test_queue",
        jobDefinition="test-jd-arn",
        retryStrategy={"attempts": 1},
        containerOverrides={
            "command": ["test_command"],
            "vcpus": 4,
            "memory": 8192,
            "resourceRequirements": [{"type": "GPU", "value": "2"}],
        },
        propagateTags=True,
        user="user-test",
    )

    # 2. multi-node
    get_aws_client_mock.return_value.submit_job.reset_mock()
    batch_submit(
        batch_job_args={
            "nodeOverrides": {
                "nodePropertyOverrides": [
                    {
                        "targetNodes": "0",
                        "containerOverrides": {"command": ["command_rank_0"]},
                    },
                    {
                        "targetNodes": "1:",
                        "containerOverrides": {"command": ["command_rank_1"]},
                    },
                ],
            }
        },
        job_name="test",
        queue="test_queue",
        image="test_image",
        vcpus=4,
        memory=8,
        gpus=2,
    )

    # Check that the resources are applied but the custom commands are specifically not touched.
    get_aws_client_mock.return_value.submit_job.assert_called_with(
        jobName="test",
        jobQueue="test_queue",
        jobDefinition="test-jd-arn",
        retryStrategy={"attempts": 1},
        nodeOverrides={
            "nodePropertyOverrides": [
                {
                    "targetNodes": "0",
                    "containerOverrides": {
                        "command": ["command_rank_0"],
                        "vcpus": 4,
                        "memory": 8192,
                        "resourceRequirements": [{"type": "GPU", "value": "2"}],
                    },
                },
                {
                    "targetNodes": "1:",
                    "containerOverrides": {
                        "command": ["command_rank_1"],
                        "vcpus": 4,
                        "memory": 8192,
                        "resourceRequirements": [{"type": "GPU", "value": "2"}],
                    },
                },
            ]
        },
        propagateTags=True,
    )


def test_batch_tags(scheduler: Scheduler) -> None:
    """
    Executor should be able to determine batch tags for a batch job.
    """
    # Setup executor.
    config = Config(
        {
            "batch": {
                "image": "image",
                "queue": "queue",
                "s3_scratch": "s3_scratch_prefix",
                "batch_tags": '{"team": "team1", "foo": "bar"}',
            }
        }
    )
    executor = AWSBatchExecutor("batch", scheduler, config["batch"])
    executor._aws_user = "alice"

    @task(batch_tags={"step": "final", "project": "acme"}, namespace="test")
    def task1(x):
        return x

    exec1 = Execution("123")
    job = Job(task1, task1(10), execution=exec1)

    batch_tags = executor._get_job_options(job)["batch_tags"]
    assert batch_tags == {
        "redun_aws_user": "alice",
        "redun_execution_id": "123",
        "redun_job_id": job.id,
        "redun_project": "test",
        "redun_task_name": "test.task1",
        "step": "final",
        "team": "team1",
        "project": "acme",
        "foo": "bar",
    }


def test_batch_tags_no_default(scheduler: Scheduler) -> None:
    """
    Executor config should be able to turn off default batch tags.
    """
    # Setup executor.
    config = Config(
        {
            "batch": {
                "image": "image",
                "queue": "queue",
                "s3_scratch": "s3_scratch_prefix",
                "default_batch_tags": "false",
            }
        }
    )
    executor = AWSBatchExecutor("batch", scheduler, config["batch"])

    @task(batch_tags={"step": "final", "project": "acme"}, namespace="test")
    def task1(x):
        return x

    exec1 = Execution("123")
    job = Job(task1, task1(10), execution=exec1)

    batch_tags = executor._get_job_options(job)["batch_tags"]
    assert batch_tags == {
        "step": "final",
        "project": "acme",
    }


def test_executor_config(scheduler: Scheduler) -> None:
    """
    Executor should be able to parse its config.
    """
    # Setup executor.
    config = Config(
        {
            "batch": {
                "image": "image",
                "queue": "queue",
                "s3_scratch": "s3_scratch_prefix",
                "code_includes": "*.txt",
                "job_def_extra": '{"a_json_key": {"a_json_nested_key": 42}}',
            }
        }
    )
    executor = AWSBatchExecutor("batch", scheduler, config["batch"])

    assert executor.image == "image"
    assert executor.queue == "queue"
    assert executor.s3_scratch_prefix == "s3_scratch_prefix"
    assert isinstance(executor.code_package, dict)
    assert executor.code_package["includes"] == ["*.txt"]
    assert executor.debug is False
    assert executor.default_task_options["job_def_extra"] == {
        "a_json_key": {"a_json_nested_key": 42}
    }


@task()
def task1(x):
    return x + 10


@task(load_module="custom.module")
def task1_custom_module(x):
    return x + 10


@use_tempdir
@mock_s3
@patch("redun.executors.aws_batch.batch_submit")
@pytest.mark.parametrize(
    "custom_module, expected_load_module, a_task",
    [
        (None, "redun.tests.test_aws_batch", task1),
        ("custom.module", "custom.module", task1_custom_module),
    ],
)
def test_submit_task(batch_submit_mock, custom_module, expected_load_module, a_task):
    job_id = "123"
    image = "my-image"
    queue = "queue"
    s3_scratch_prefix = "s3://example-bucket/redun/"

    client = boto3.client("s3", region_name="us-east-1")
    client.create_bucket(Bucket="example-bucket")

    redun.executors.aws_batch.batch_submit.return_value = {"jobId": "batch-job-id"}

    # Create example workflow script to be packaged.
    File("workflow.py").write(
        f"""
@task(load_module={custom_module})
def task1(x):
    return x + 10
    """
    )

    job = Job(a_task, a_task())
    job.id = job_id
    job.eval_hash = "eval_hash"
    code_file = package_code(s3_scratch_prefix)
    resp = submit_task(
        image,
        queue,
        s3_scratch_prefix,
        job,
        a_task,
        args=[10],
        kwargs={},
        code_file=code_file,
    )

    # We should get a AWS Batch job id back.
    assert resp["jobId"] == "batch-job-id"

    # Input files should be made.
    assert File("s3://example-bucket/redun/jobs/eval_hash/input").exists()
    [code_file] = list(Dir("s3://example-bucket/redun/code"))

    # We should have submitted a job to AWS Batch.
    redun.executors.aws_batch.batch_submit.assert_called_with(
        {
            "containerOverrides": {
                "command": [
                    "redun",
                    "--check-version",
                    REDUN_REQUIRED_VERSION,
                    "oneshot",
                    expected_load_module,
                    "--code",
                    code_file.path,
                    "--input",
                    "s3://example-bucket/redun/jobs/eval_hash/input",
                    "--output",
                    "s3://example-bucket/redun/jobs/eval_hash/output",
                    "--error",
                    "s3://example-bucket/redun/jobs/eval_hash/error",
                    a_task.name,
                ]
            }
        },
        "queue",
        image="my-image",
        job_def_suffix="-redun-jd",
        job_name="batch-job-eval_hash",
        array_size=0,
        aws_region="us-west-2",
    )


@use_tempdir
@mock_s3
@patch("redun.executors.aws_batch.batch_submit")
@pytest.mark.parametrize(
    "custom_module, expected_load_module, a_task",
    [
        (None, "redun.tests.test_aws_batch", task1),
        ("custom.module", "custom.module", task1_custom_module),
    ],
)
def test_submit_task_array(batch_submit_mock, custom_module, expected_load_module, a_task):
    """
    oneshot should use --array-job when submitting an array job.
    """
    job_id = "123"
    image = "my-image"
    queue = "queue"
    s3_scratch_prefix = "s3://example-bucket/redun/"

    client = boto3.client("s3", region_name="us-east-1")
    client.create_bucket(Bucket="example-bucket")

    redun.executors.aws_batch.batch_submit.return_value = {"jobId": "batch-job-id"}

    # Create example workflow script to be packaged.
    File("workflow.py").write(
        f"""
@task(load_module={custom_module})
def task1(x):
    return x + 10
    """
    )

    job = Job(a_task, a_task())
    job.id = job_id
    job.eval_hash = "eval_hash"
    code_file = File("s3://example-bucket/redun/code/123.tar.gz")
    resp = submit_task(
        image,
        queue,
        s3_scratch_prefix,
        job,
        a_task,
        args=[10],
        kwargs={},
        code_file=code_file,
        array_uuid="123456",
        array_size=10,
    )

    # We should get a AWS Batch job id back.
    assert resp["jobId"] == "batch-job-id"

    # We should have submitted a job to AWS Batch.
    redun.executors.aws_batch.batch_submit.assert_called_with(
        {
            "containerOverrides": {
                "command": [
                    "redun",
                    "--check-version",
                    REDUN_REQUIRED_VERSION,
                    "oneshot",
                    expected_load_module,
                    "--code",
                    code_file.path,
                    "--array-job",
                    "--input",
                    "s3://example-bucket/redun/array_jobs/123456/input",
                    "--output",
                    "s3://example-bucket/redun/array_jobs/123456/output",
                    "--error",
                    "s3://example-bucket/redun/array_jobs/123456/error",
                    a_task.name,
                ]
            }
        },
        "queue",
        image="my-image",
        job_def_suffix="-redun-jd",
        job_name="batch-job-123456-array",
        array_size=10,
        aws_region="us-west-2",
    )


@use_tempdir
@mock_s3
@patch("redun.executors.aws_batch.batch_submit")
def test_submit_multinode_task(batch_submit_mock):
    job_id = "123"
    image = "my-image"
    queue = "queue"
    s3_scratch_prefix = "s3://example-bucket/redun/"

    client = boto3.client("s3", region_name="us-east-1")
    client.create_bucket(Bucket="example-bucket")

    redun.executors.aws_batch.batch_submit.return_value = {"jobId": "batch-job-id"}

    # Create example workflow script to be packaged.
    File("workflow.py").write(
        """
@task()
def task1(x):
    return x + 10
    """
    )

    job = Job(task1, task1())
    job.id = job_id
    job.eval_hash = "eval_hash"
    code_file = package_code(s3_scratch_prefix)
    resp = submit_task(
        image,
        queue,
        s3_scratch_prefix,
        job,
        task1,
        args=[10],
        kwargs={},
        code_file=code_file,
        job_options={"num_nodes": 4},
    )

    # We should get a AWS Batch job id back.
    assert resp["jobId"] == "batch-job-id"

    # Input files should be made.
    assert File("s3://example-bucket/redun/jobs/eval_hash/input").exists()
    [code_file] = list(Dir("s3://example-bucket/redun/code"))

    # We should have submitted a job to AWS Batch.
    redun.executors.aws_batch.batch_submit.assert_called_with(
        {
            "nodeOverrides": {
                "nodePropertyOverrides": [
                    {
                        "targetNodes": "0",
                        "containerOverrides": {
                            "command": [
                                "redun",
                                "--check-version",
                                REDUN_REQUIRED_VERSION,
                                "oneshot",
                                "redun.tests.test_aws_batch",
                                "--code",
                                code_file.path,
                                "--input",
                                "s3://example-bucket/redun/jobs/eval_hash/input",
                                "--output",
                                "s3://example-bucket/redun/jobs/eval_hash/output",
                                "--error",
                                "s3://example-bucket/redun/jobs/eval_hash/error",
                                task1.name,
                            ]
                        },
                    },
                    {
                        "targetNodes": "1:",
                        "containerOverrides": {
                            "command": [
                                "redun",
                                "--check-version",
                                REDUN_REQUIRED_VERSION,
                                "oneshot",
                                "redun.tests.test_aws_batch",
                                "--code",
                                code_file.path,
                                "--no-cache",
                                "--input",
                                "s3://example-bucket/redun/jobs/eval_hash/input",
                                "--output",
                                "/dev/null",
                                "--error",
                                "s3://example-bucket/redun/jobs/eval_hash/error",
                                task1.name,
                            ]
                        },
                    },
                ],
            }
        },
        "queue",
        image="my-image",
        job_def_suffix="-redun-jd",
        job_name="batch-job-eval_hash",
        array_size=0,
        aws_region="us-west-2",
        num_nodes=4,
    )


@use_tempdir
@mock_s3
@patch("redun.executors.aws_batch.batch_submit")
def test_submit_task_deep_file(batch_submit_mock):
    """
    Executor should be able to submit a task defined in a deeply nested file path.
    """
    job_id = "123"
    image = "my-image"
    queue = "queue"
    s3_scratch_prefix = "s3://example-bucket/redun/"

    client = boto3.client("s3", region_name="us-east-1")
    client.create_bucket(Bucket="example-bucket")

    redun.executors.aws_batch.batch_submit.return_value = {"jobId": "batch-job-id"}

    # Create example workflow script to be packaged.
    File("path/to/workflow.py").write(
        """
from redun import task

@task()
def task1(x):
    return x + 10
    """
    )

    module = import_script("path/to/workflow.py")

    job = Job(module.task1, module.task1())
    job.id = job_id
    job.eval_hash = "eval_hash"
    code_file = package_code(s3_scratch_prefix)
    resp = submit_task(
        image,
        queue,
        s3_scratch_prefix,
        job,
        module.task1,
        args=[10],
        kwargs={},
        code_file=code_file,
    )

    # We should get a AWS Batch job id back.
    assert resp["jobId"] == "batch-job-id"

    # Input files should be made.
    assert File("s3://example-bucket/redun/jobs/eval_hash/input").exists()
    [code_file] = list(Dir("s3://example-bucket/redun/code"))

    # We should have submitted a job to AWS Batch.
    redun.executors.aws_batch.batch_submit.assert_called_with(
        {
            "containerOverrides": {
                "command": [
                    "redun",
                    "--check-version",
                    REDUN_REQUIRED_VERSION,
                    "oneshot",
                    "workflow",
                    "--import-path",
                    "path/to",
                    "--code",
                    code_file.path,
                    "--input",
                    "s3://example-bucket/redun/jobs/eval_hash/input",
                    "--output",
                    "s3://example-bucket/redun/jobs/eval_hash/output",
                    "--error",
                    "s3://example-bucket/redun/jobs/eval_hash/error",
                    "task1",
                ]
            }
        },
        "queue",
        image="my-image",
        job_def_suffix="-redun-jd",
        job_name="batch-job-eval_hash",
        array_size=0,
        aws_region="us-west-2",
    )


@mock_s3
def test_parse_job_error() -> None:
    """
    We should be able to parse the error of a failed task.
    """
    s3_scratch_prefix = "s3://example-bucket/redun/"

    client = boto3.client("s3", region_name="us-east-1")
    client.create_bucket(Bucket="example-bucket")

    @task()
    def task1(x):
        return x + 1

    @task(script=True)
    def task_script1():
        return "echo hello!"

    expr = task1(10)
    job = Job(task1, expr)
    job.eval_hash = "eval_hash"

    # Normal task, no error file.
    error, error_traceback = parse_job_error(s3_scratch_prefix, job)
    assert isinstance(error, ExceptionNotFoundError)
    assert isinstance(error_traceback, Traceback)

    # Simulate AWS Batch job failing.
    error = ValueError("Boom")
    error_file = File("s3://example-bucket/redun/jobs/eval_hash/error")
    error_file.write(pickle_dumps((error, Traceback.from_error(error))), mode="wb")

    # Normal task, error file exists.
    error, error_traceback = parse_job_error(s3_scratch_prefix, job)
    assert isinstance(error, ValueError)
    assert isinstance(error_traceback, Traceback)

    # Create a script task and job.
    expr2 = task_script1()
    job2 = Job(task_script1, expr2)
    job2.eval_hash = "eval_hash2"

    # Script task without an error file should retutn a generic error.
    error, error_traceback = parse_job_error(s3_scratch_prefix, job2)
    assert isinstance(error, ExceptionNotFoundError)
    assert isinstance(error_traceback, Traceback)

    # Create error file for script task.
    error_file2 = File("s3://example-bucket/redun/jobs/eval_hash2/error")
    error_file2.write("Boom")

    # Script task with an error file should return a specific error.
    error, error_traceback = parse_job_error(s3_scratch_prefix, job2)
    assert isinstance(error, ScriptError)
    assert error.message == "Boom"
    assert isinstance(error_traceback, Traceback)


@freeze_time("2020-10-13 06:47:14", tz_offset=-7)
@mock_logs
@patch("redun.executors.aws_batch.aws_describe_jobs")
def test_iter_batch_job_logs(aws_describe_jobs_mock) -> None:
    """
    We should be able to iterate through the logs of a Batch Job.
    """
    stream_name = "redun_aws_batch_example-redun-jd/default/6c939514f4054fdfb5ee65acc8aa4b07"
    aws_describe_jobs_mock.side_effect = lambda *args, **kwargs: iter(
        [
            {
                "container": {
                    "logStreamName": stream_name,
                }
            }
        ]
    )

    timestamp = int(time.time() * 1000)

    # Setup logs mocks.
    logs_client = boto3.client("logs", region_name="us-west-2")
    logs_client.create_log_group(logGroupName=BATCH_LOG_GROUP)
    logs_client.create_log_stream(logGroupName=BATCH_LOG_GROUP, logStreamName=stream_name)
    resp = logs_client.put_log_events(
        logGroupName=BATCH_LOG_GROUP,
        logStreamName=stream_name,
        logEvents=[
            {"timestamp": timestamp, "message": "A message 1."},
            {"timestamp": timestamp + 1, "message": "A message 2."},
        ],
    )
    resp = logs_client.put_log_events(
        logGroupName=BATCH_LOG_GROUP,
        logStreamName=stream_name,
        logEvents=[
            {"timestamp": timestamp + 2, "message": "A message 3."},
            {"timestamp": timestamp + 3, "message": "A message 4."},
        ],
        sequenceToken=resp["nextSequenceToken"],
    )

    expected_events = [
        {"message": "A message 1.", "timestamp": timestamp},
        {"message": "A message 2.", "timestamp": timestamp + 1},
        {"message": "A message 3.", "timestamp": timestamp + 2},
        {"message": "A message 4.", "timestamp": timestamp + 3},
    ]

    # Fetch log events.
    job_id = "123"
    events = iter_batch_job_logs(job_id, limit=1)
    event_list = [
        {"message": event["message"], "timestamp": event["timestamp"]} for event in events
    ]
    assert event_list == expected_events

    # Fetch log events in reverse.
    events = iter_batch_job_logs(job_id, limit=1, reverse=True)
    event_list = [
        {"message": event["message"], "timestamp": event["timestamp"]} for event in events
    ]
    assert event_list == list(reversed(expected_events))

    # Fetch log events in reverse with larger page size.
    events = iter_batch_job_logs(job_id, limit=2, reverse=True)
    event_list = [
        {"message": event["message"], "timestamp": event["timestamp"]} for event in events
    ]
    assert event_list == list(reversed(expected_events))

    # Fetch log lines.
    lines = list(iter_batch_job_log_lines(job_id))
    assert lines == [
        "2020-10-12 23:47:14  A message 1.",
        "2020-10-12 23:47:14.001000  A message 2.",
        "2020-10-12 23:47:14.002000  A message 3.",
        "2020-10-12 23:47:14.003000  A message 4.",
    ]

    # Fetch logs from unknown job.
    aws_describe_jobs_mock.side_effect = lambda *args, **kwargs: iter([])
    assert list(iter_batch_job_logs("unknown_job_id")) == []

    # Fetch logs from job with missing logs.
    aws_describe_jobs_mock.side_effect = lambda *args, **kwargs: iter(
        [
            {
                "container": {
                    "logStreamName": "bad_logs",
                }
            }
        ]
    )
    assert list(iter_batch_job_logs(job_id, required=False)) == []

    with pytest.raises(Exception):
        list(iter_batch_job_logs(job_id, required=True))

    # Fetch logs from job with no logs at all.
    aws_describe_jobs_mock.side_effect = lambda *args, **kwargs: iter([{"container": {}}])
    assert list(iter_batch_job_logs(job_id, required=False)) == []


def mock_executor(
    scheduler, custom_config_args: Dict[str, Any] = {}, debug=False, code_package=False
):
    """
    Returns an AWSBatchExecutor with AWS API mocks.
    """
    image = "my-image"
    queue = "queue"
    s3_scratch_prefix = "s3://example-bucket/redun/"

    # Setup executor.
    config = Config(
        {
            "batch": {
                "image": image,
                "queue": queue,
                "s3_scratch": s3_scratch_prefix,
                "job_monitor_interval": 0.05,
                "job_stale_time": 0.01,
                "code_package": code_package,
                "debug": debug,
                **custom_config_args,
            }
        }
    )
    executor = AWSBatchExecutor("batch", scheduler, config["batch"])

    executor.get_jobs = Mock()  # type: ignore[assignment]
    executor.get_jobs.return_value = []

    executor.get_array_child_jobs = Mock()  # type: ignore[assignment]
    executor.get_array_child_jobs.return_value = []

    s3_client = boto3.client("s3", region_name="us-east-1")
    s3_client.create_bucket(Bucket="example-bucket")

    return executor


@mock_s3
@patch("redun.executors.aws_utils.get_aws_user", return_value="alice")
@patch("redun.executors.aws_batch.parse_job_logs")
@patch("redun.executors.aws_batch.iter_batch_job_status")
@patch("redun.executors.aws_batch.batch_submit")
def test_executor(
    batch_submit_mock, iter_batch_job_status_mock, parse_job_logs_mock, get_aws_user_mock
) -> None:
    """
    Ensure that we can submit job to AWSBatchExecutor.
    """
    batch_job_id = "batch-job-id"
    batch_job2_id = "batch-job2-id"

    # Setup AWS Batch mocks.
    iter_batch_job_status_mock.return_value = iter([])
    parse_job_logs_mock.return_value = []

    scheduler = mock_scheduler()
    executor = mock_executor(scheduler, {"timeout": 5, "shared_memory": 5, "privileged": True})
    executor.start()

    try:
        batch_submit_mock.return_value = {
            "jobId": batch_job_id,
        }

        # Submit redun job that will succeed.
        expr = task1(10)
        job = Job(task1, expr)
        job.eval_hash = "eval_hash"
        job.args = ((10,), {})
        executor.submit(job)

        # Let job get stale so job arrayer actually submits it.
        wait_until(lambda: executor.arrayer.num_pending == 0)

        # Ensure job options were passed correctly.
        assert batch_submit_mock.call_args
        assert batch_submit_mock.call_args[1] == {
            "image": "my-image",
            "job_name": "redun-job-eval_hash",
            "job_def_suffix": "-redun-jd",
            "array_size": 0,
            "vcpus": 1,
            "gpus": 0,
            "memory": 4,
            "shared_memory": 5,
            "timeout": 5,
            "privileged": True,
            "autocreate_job_def": True,
            "job_def_name": None,
            "role": None,
            "retries": 1,
            "aws_region": "us-west-2",
            "num_nodes": None,
            "job_def_extra": None,
            "batch_tags": {
                "redun_aws_user": "alice",
                "redun_execution_id": "",
                "redun_job_id": job.id,
                "redun_project": "",
                "redun_task_name": "task1",
            },
            "share_id": None,
            "scheduling_priority_override": None,
        }

        batch_submit_mock.return_value = {
            "jobId": batch_job2_id,
        }

        # Submit redun job that will fail.
        expr2 = task1.options(memory=8)("a")
        job2 = Job(task1, expr2)
        job2.eval_hash = "eval_hash2"
        job2.args = (("a",), {})
        executor.submit(job2)

        # Let job get stale so job arrayer actually submits it.
        wait_until(lambda: executor.arrayer.num_pending == 0)

        # Ensure job options were passed correctly.
        assert batch_submit_mock.call_args[1] == {
            "image": "my-image",
            "job_name": "redun-job-eval_hash2",
            "job_def_suffix": "-redun-jd",
            "array_size": 0,
            "vcpus": 1,
            "gpus": 0,
            "memory": 8,
            "shared_memory": 5,
            "timeout": 5,
            "privileged": True,
            "autocreate_job_def": True,
            "job_def_name": None,
            "role": None,
            "retries": 1,
            "aws_region": "us-west-2",
            "num_nodes": None,
            "job_def_extra": None,
            "batch_tags": {
                "redun_aws_user": "alice",
                "redun_execution_id": "",
                "redun_job_id": job2.id,
                "redun_project": "",
                "redun_task_name": "task1",
            },
            "share_id": None,
            "scheduling_priority_override": None,
        }

        # Simulate AWS Batch completing job.
        output_file = File("s3://example-bucket/redun/jobs/eval_hash/output")
        output_file.write(pickle_dumps(task1.func(10)), mode="wb")

        # Simulate AWS Batch failing.
        error = ValueError("Boom")
        error_file = File("s3://example-bucket/redun/jobs/eval_hash2/error")
        error_file.write(pickle_dumps((error, Traceback.from_error(error))), mode="wb")

        iter_batch_job_status_mock.return_value = iter(
            [
                {
                    "jobId": batch_job_id,
                    "status": SUCCEEDED,
                    "container": {"logStreamName": "log1"},
                },
                {"jobId": batch_job2_id, "status": FAILED, "container": {"logStreamName": "log2"}},
            ]
        )

        scheduler.batch_wait([job.id, job2.id])
        executor.stop()

        # Job results and errors should be sent back to scheduler.
        assert scheduler.job_results[job.id] == 20
        assert isinstance(scheduler.job_errors[job2.id], ValueError)

        # Assert job tags.
        job.job_tags == [("aws_batch_job", "batch-job-id"), ("aws_log_stream", "log1")]
        job.job_tags == [("aws_batch_job", "batch-job2-id"), ("aws_log_stream", "log2")]
    finally:
        # Ensure executor threads stop before continuing.
        executor.stop()


@mock_s3
@patch("redun.executors.aws_utils.get_aws_user", return_value="alice")
@patch("redun.executors.aws_batch.parse_job_logs")
@patch("redun.executors.aws_batch.iter_batch_job_status")
@patch("redun.executors.aws_batch.batch_submit")
def test_executor_multinode(
    batch_submit_mock, iter_batch_job_status_mock, parse_job_logs_mock, get_aws_user_mock
) -> None:
    """
    Ensure that we can submit job to AWSBatchExecutor.
    """
    batch_job_id = "batch-job-id"

    # Setup AWS Batch mocks.
    iter_batch_job_status_mock.return_value = iter([])
    parse_job_logs_mock.return_value = []

    scheduler = mock_scheduler()
    executor = mock_executor(scheduler)
    executor.start()

    try:
        batch_submit_mock.return_value = {
            "jobId": batch_job_id,
        }

        # Submit redun job that will succeed.
        expr = task1.options(num_nodes=4)(10)
        job = Job(task1, expr)
        job.eval_hash = "eval_hash"
        job.args = ((10,), {})
        executor.submit(job)

        # Let job get stale so job arrayer actually submits it.
        wait_until(lambda: executor.arrayer.num_pending == 0)

        # Ensure job options were passed correctly.
        assert batch_submit_mock.call_args
        assert batch_submit_mock.call_args[1] == {
            "image": "my-image",
            "job_name": "redun-job-eval_hash",
            "job_def_suffix": "-redun-jd",
            "array_size": 0,
            "vcpus": 1,
            "gpus": 0,
            "memory": 4,
            "shared_memory": None,
            "timeout": None,
            "privileged": False,
            "autocreate_job_def": True,
            "job_def_name": None,
            "role": None,
            "retries": 1,
            "aws_region": "us-west-2",
            "num_nodes": 4,
            "job_def_extra": None,
            "batch_tags": {
                "redun_aws_user": "alice",
                "redun_execution_id": "",
                "redun_job_id": job.id,
                "redun_project": "",
                "redun_task_name": "task1",
            },
            "share_id": None,
            "scheduling_priority_override": None,
        }
        job.job_tags == [("aws_batch_job", "batch-job-id#0"), ("aws_log_stream", "log1")]
    finally:
        # Ensure executor threads stop before continuing.
        executor.stop()


@use_tempdir
@mock_s3
@patch("redun.executors.aws_utils.get_aws_user", return_value="alice")
@patch("redun.executors.docker.iter_job_status")
@patch("redun.executors.docker.run_docker")
def test_executor_docker(
    run_docker_mock: Mock,
    iter_local_job_status_mock: Mock,
    get_aws_user_mock: Mock,
) -> None:
    """
    Ensure that we can submit job to AWSBatchExecutor with debug=True.
    """
    batch_job_id = "batch-job-id"
    batch_job2_id = "batch-job2-id"

    # Setup Docker mocks.
    iter_local_job_status_mock.return_value = iter([])

    # Setup redun mocks.
    scheduler = mock_scheduler()
    executor = mock_executor(scheduler, debug=True)
    executor.start()

    try:
        run_docker_mock.return_value = batch_job_id

        # Submit redun job that will succeed.
        expr = task1(10)
        job = Job(task1, expr)
        job.eval_hash = "eval_hash"
        job.args = ((10,), {})
        executor.submit(job)

        # Let job get stale so job arrayer actually submits it.
        wait_until(lambda: executor.arrayer.num_pending == 0)

        # Ensure job options were passed correctly.
        scratch_dir = executor._docker_executor._scratch_prefix
        assert run_docker_mock.call_args[1] == {
            "image": "my-image",
            "gpus": 0,
            "memory": 4,
            "shared_memory": None,
            "vcpus": 1,
            "volumes": [(scratch_dir, scratch_dir)],
            "interactive": False,
            "include_aws_env": True,
        }

        run_docker_mock.reset_mock()
        run_docker_mock.return_value = batch_job2_id

        # Hand create Job and submit.
        expr2 = task1("a")
        job2 = Job(task1, expr2)
        job2.eval_hash = "eval_hash2"
        job2.args = (("a",), {})
        executor.submit(job2)

        # Let job get stale so job arrayer actually submits it.
        wait_until(lambda: executor.arrayer.num_pending == 0)

        # Ensure job options were passed correctly.
        assert run_docker_mock.call_args[1] == {
            "image": "my-image",
            "gpus": 0,
            "memory": 4,
            "shared_memory": None,
            "vcpus": 1,
            "volumes": [(scratch_dir, scratch_dir)],
            "interactive": False,
            "include_aws_env": True,
        }

        # Simulate output file created by job.
        output_file = File(f"{scratch_dir}/jobs/eval_hash/output")
        output_file.write(pickle_dumps(task1.func(10)), mode="wb")

        # Simulate AWS Batch failing.
        error = ValueError("Boom")
        error_file = File(f"{scratch_dir}/jobs/eval_hash2/error")
        error_file.write(pickle_dumps((error, Traceback.from_error(error))), mode="wb")

        iter_local_job_status_mock.return_value = iter(
            [
                {"jobId": batch_job_id, "status": SUCCEEDED, "logs": ""},
                {"jobId": batch_job2_id, "status": FAILED, "logs": ""},
            ]
        )

        scheduler.batch_wait([job.id, job2.id])

        # Job results and errors should be sent back to scheduler.
        assert scheduler.job_results[job.id] == 20
        assert isinstance(scheduler.job_errors[job2.id], ValueError)

    finally:
        # Ensure executor threads stop before continuing.
        executor.stop()


@use_tempdir
@mock_s3
@patch("redun.executors.aws_utils.get_aws_user", return_value="alice")
@patch("redun.executors.docker.iter_job_status")
@patch("redun.executors.docker.run_docker")
def test_executor_job_debug(
    run_docker_mock: Mock,
    iter_local_job_status_mock: Mock,
    get_aws_user_mock: Mock,
) -> None:
    """
    Ensure that we can submit job to AWSBatchExecutor with debug=True in task options.
    """
    batch_job_id = "batch-job-id"

    # Setup Docker mocks.
    iter_local_job_status_mock.return_value = iter([])

    # Setup redun mocks.
    scheduler = mock_scheduler()
    executor = mock_executor(scheduler)
    executor.start()

    try:
        run_docker_mock.return_value = batch_job_id

        # Submit redun job that will succeed.
        expr = task1.options(debug=True)(10)
        job = Job(task1, expr)
        job.eval_hash = "eval_hash"
        job.args = ((10,), {})
        executor.submit(job)

        # Let job get stale so job arrayer actually submits it.
        wait_until(lambda: executor.arrayer.num_pending == 0)

        # Ensure job options were passed correctly.
        scratch_dir = executor._docker_executor._scratch_prefix
        assert run_docker_mock.call_args[1] == {
            "image": "my-image",
            "gpus": 0,
            "memory": 4,
            "shared_memory": None,
            "vcpus": 1,
            "volumes": [(scratch_dir, scratch_dir)],
            "interactive": False,
            "include_aws_env": True,
        }

        # Simulate output file created by job.
        output_file = File(f"{scratch_dir}/jobs/eval_hash/output")
        output_file.write(pickle_dumps(task1.func(10)), mode="wb")

        iter_local_job_status_mock.return_value = iter(
            [
                {"jobId": batch_job_id, "status": SUCCEEDED, "logs": ""},
            ]
        )

        scheduler.batch_wait([job.id])

        # Job results and errors should be sent back to scheduler.
        assert scheduler.job_results[job.id] == 20

    finally:
        # Ensure executor threads stop before continuing.
        executor.stop()


@mock_s3
@patch("redun.executors.aws_utils.get_aws_user", return_value="alice")
@patch("redun.executors.aws_batch.parse_job_logs")
@patch("redun.executors.aws_batch.iter_batch_job_status")
@patch("redun.executors.aws_batch.batch_submit")
def test_executor_error_override(
    batch_submit_mock: Mock,
    iter_batch_job_status_mock: Mock,
    parse_job_logs_mock: Mock,
    get_aws_user_mock: Mock,
) -> None:
    """
    Some AWS Batch errors should be overridden.
    """

    @task()
    def task1(x):
        return x + 10

    @task(script=True)
    def task_script1(x):
        return "ls"

    batch_job_id = "batch-job-id"
    batch_job_script_id = "batch-job-script-id"

    # Setup AWS Batch mocks.
    iter_batch_job_status_mock.return_value = iter([])
    parse_job_logs_mock.return_value = []

    scheduler = mock_scheduler()
    executor = mock_executor(scheduler)
    executor.start()

    batch_submit_mock.return_value = {
        "jobId": batch_job_id,
    }

    # Submit redun job that will succeed at the redun-level.
    expr = task1.options(memory=8)("a")
    job = Job(task1, expr)
    job.eval_hash = "eval_hash"
    job.args = (("a",), {})
    executor.submit(job)

    # Let job get stale so job arrayer actually submits it.
    wait_until(lambda: executor.arrayer.num_pending == 0)

    batch_submit_mock.return_value = {
        "jobId": batch_job_script_id,
    }

    # Submit redun job that will succeed at the redun-level.
    expr_script = task_script1.options(memory=8)("a")
    job_script = Job(task_script1, expr_script)
    job_script.eval_hash = "eval_script_hash"
    job_script.args = (("a",), {})
    executor.submit(job_script)

    # Let job get stale so job arrayer actually submits it.
    wait_until(lambda: executor.arrayer.num_pending == 0)

    # Simulate output file created by job.
    output_file = File("s3://example-bucket/redun/jobs/eval_hash/output")
    output_file.write(pickle_dumps(task1.func(10)), mode="wb")

    # Simulate output file created by job.
    output_file = File("s3://example-bucket/redun/jobs/eval_script_hash/output")
    output_file.write(pickle_dumps("done"), mode="wb")
    File("s3://example-bucket/redun/jobs/eval_script_hash/status").write("ok")

    # But simulate AWS Batch failing.
    reason = "CannotInspectContainerError: Could not transition to inspecting."
    iter_batch_job_status_mock.return_value = iter(
        [
            {
                "jobId": batch_job_id,
                "status": FAILED,
                "attempts": [
                    {
                        "container": {
                            "reason": reason,
                        },
                    },
                ],
            },
            {
                "jobId": batch_job_script_id,
                "status": FAILED,
                "attempts": [
                    {
                        "container": {
                            "reason": reason,
                        },
                    },
                ],
            },
        ]
    )

    scheduler.batch_wait([job.id, job_script.id])
    executor.stop()

    # Despite AWS Batch error, redun job should succeed and
    # results should be sent back to scheduler.
    assert scheduler.job_results[job.id] == 20


@mock_s3
@patch("redun.executors.aws_utils.get_aws_user", return_value="alice")
@patch("redun.executors.docker.iter_job_status")
@patch("redun.executors.docker.run_docker")
def test_executor_multiple_start(
    run_docker_mock, iter_local_job_status_mock, get_aws_user_mock
) -> None:
    """
    Ensure that we can start executor multiple times.
    """
    # Setup Docker mocks.
    iter_local_job_status_mock.return_value = iter([])

    # Setup redun mocks.
    scheduler = mock_scheduler()
    executor = mock_executor(scheduler, debug=True)
    executor.start()
    executor._start()
    executor._start()
    executor.stop()


@mock_s3
@patch("redun.executors.aws_utils.get_aws_user", return_value="alice")
@patch("redun.executors.docker.iter_job_status")
@patch("redun.executors.docker.run_docker")
def test_interactive(run_docker_mock, iter_local_job_status_mock, get_aws_user_mock) -> None:
    """
    The interactive task option should be passed to run_docker.
    """
    # Setup Docker mocks.
    iter_local_job_status_mock.return_value = iter([])
    run_docker_mock.return_value = "batch-job-id"

    # Setup redun mocks.
    scheduler = mock_scheduler()
    executor = mock_executor(scheduler, debug=True)
    executor.start()

    # Hand create Job and submit.
    expr = task1.options(interactive=True)(10)
    job = Job(task1, expr)
    job.eval_hash = "eval_hash"
    job.args = ((10,), {})
    executor.submit(job)

    # Let job get stale so job arrayer actually submits it
    wait_until(lambda: executor.arrayer.num_pending == 0)

    # Ensure job options were passed correctly.
    scratch_dir = executor._docker_executor._scratch_prefix
    assert run_docker_mock.call_args[1] == {
        "image": "my-image",
        "interactive": True,
        "gpus": 0,
        "memory": 4,
        "shared_memory": None,
        "vcpus": 1,
        "volumes": [(scratch_dir, scratch_dir)],
        "include_aws_env": True,
    }

    # Cleanly stop executor.
    executor.stop()


@mock_s3
def test_executor_handles_unrelated_jobs() -> None:
    """
    Regression test for https://insitro.atlassian.net/browse/DE-2632

    There is an expanding pattern of using a "headnode" running in batch to trigger redun
    pipelines. If the headnode and redun jobs that it spawns have a shared job_name_prefix then
    the headnode job can get gathered in the `get_jobs` call and we will try to extract the hash.
    However, since the headnode job is not a redun job, it will not have a hash and previously
    caused execution failures.

    This test confirms that jobs without hashes in their names are ignored which allows headnode
    jobs(triggered via lambda or otherwise) to share job name prefixes with the redun jobs that
    they spawn.
    """
    scheduler = mock_scheduler()
    executor = mock_executor(scheduler)

    prefix = "liveratlas_spearmancor"
    hash1 = "123456789"
    hash2 = "987654321"

    # Set up mocks to include a headnode job(no hash) and some redun jobs that it "spawned".
    executor.get_jobs.return_value = [
        # The headnode job. Note the job name has not hash in it as the hash appears after the "-"
        # in a redun job name.
        {"jobId": "headnode", "jobName": f"{prefix}_automation_headnode"},
        # Redun jobs that were triggered by the "redun run" in the headnode.
        {"jobId": "preprocess", "jobName": f"{prefix}_preprocess-{hash1}"},
        {"jobId": "decode", "jobName": f"{prefix}_decode-{hash2}"},
    ]

    executor.gather_inflight_jobs()

    assert executor.preexisting_batch_jobs == {
        hash1: "preprocess",
        hash2: "decode",
    }


@mock_s3
def test_executor_inflight_array_job() -> None:
    """
    Ensure we reunite with an inflight array job
    """
    scheduler = mock_scheduler()
    executor = mock_executor(scheduler)

    # Set up mocks to indicate an array job is inflight.
    array_uuid = str(uuid.uuid4()).replace("-", "")
    executor.get_jobs.return_value = [
        {"jobId": "carrots", "jobName": f"redun-job-{array_uuid}-array"}
    ]
    executor.get_array_child_jobs.return_value = [
        {"jobId": "carrots:1", "arrayProperties": {"index": 1}},
        {"jobId": "carrots:0", "arrayProperties": {"index": 0}},
        {"jobId": "carrots:2", "arrayProperties": {"index": 2}},
    ]

    # Set up hash scratch file
    eval_file = File(f"s3://example-bucket/redun/array_jobs/{array_uuid}/eval_hashes")
    with eval_file.open("w") as eval_f:
        eval_f.write("zero\none\ntwo")

    # Force the scheduler to gather the inflight jobs. This normally happens on
    # first job submission but we want to just check that we can join here.
    executor.gather_inflight_jobs()

    # Check query for child job happened.
    assert executor.get_array_child_jobs.call_args
    assert executor.get_array_child_jobs.call_args[0] == ("carrots", BATCH_JOB_STATUSES.inflight)

    # Make sure child jobs (and not parent) ended up in pending batch jobs.
    assert executor.preexisting_batch_jobs == {
        "zero": "carrots:0",
        "one": "carrots:1",
        "two": "carrots:2",
    }


@mock_s3
@patch("redun.executors.aws_utils.get_aws_user", return_value="alice")
@patch("redun.executors.aws_batch.package_code")
@patch("redun.executors.aws_batch.iter_batch_job_status")
@patch("redun.executors.aws_batch.submit_task")
def test_code_packaging(
    submit_task_mock, iter_batch_job_status_mock, package_code_mock, get_aws_user_mock
) -> None:
    """
    Ensure that code packaging only happens on first submission.
    """
    package_code_mock.return_value = File("s3://fake-bucket/code.tar.gz")

    scheduler = mock_scheduler()
    executor = mock_executor(scheduler, code_package=True)
    executor.start()

    # Starting the executor should not have triggered code packaging.
    assert executor.code_file is None
    assert package_code_mock.call_count == 0

    # Hand create jobs.
    job1 = Job(task1, task1(10))
    job1.id = "1"
    job1.task = task1
    job1.eval_hash = "eval_hash"
    job1.args = ((10,), {})

    job2 = Job(task1, task1(20))
    job2.id = "2"
    job2.eval_hash = "eval_hash"
    job2.args = ((20,), {})

    # Submit a job and ensure that the code was packaged.
    executor.submit(job1)
    assert executor.code_file
    assert executor.code_file.path == "s3://fake-bucket/code.tar.gz"
    assert package_code_mock.call_count == 1

    # Submit another job and ensure that code was not packaged again.
    executor.submit(job2)
    assert package_code_mock.call_count == 1

    executor.stop()


@mock_s3
@patch("redun.executors.aws_utils.get_aws_user", return_value="alice")
@patch("redun.executors.aws_batch.aws_describe_jobs")
def test_inflight_join_only_on_first_submission(aws_describe_jobs_mock, get_aws_user_mock) -> None:
    """
    Ensure that inflight jobs are only gathered once and not on every job submission.
    """
    scheduler = mock_scheduler()
    executor = mock_executor(scheduler)

    executor.start()

    # Hand create jobs.
    job1 = Job(task1, task1(10))
    job1.id = "1"
    job1.eval_hash = "eval_hash"
    job1.args = ((10,), {})

    job2 = Job(task1, task1(20))
    job2.id = "2"
    job2.eval_hash = "eval_hash"
    job2.args = ((20,), {})

    # Submit redun job.
    executor.submit(job1)

    # Ensure that inflight jobs were gathered.
    assert executor.get_jobs.call_count == 1

    # Submit the second job and confirm that job reuniting was not done again.
    executor.submit(job2)
    assert executor.get_jobs.call_count == 1

    executor.stop()


@mock_s3
@patch("redun.executors.aws_utils.get_aws_user", return_value="alice")
@patch("redun.executors.aws_batch.aws_describe_jobs")
@patch("redun.executors.aws_batch.iter_batch_job_status")
@patch("redun.executors.aws_batch.batch_submit")
def test_executor_inflight_job(
    batch_submit_mock,
    iter_batch_job_status_mock,
    aws_describe_jobs_mock,
    get_aws_user_mock,
) -> None:
    """
    Ensure we reunite with an inflight job.
    """
    batch_job_id = "333"

    # Setup AWS Batch mocks.
    iter_batch_job_status_mock.return_value = iter([])
    aws_describe_jobs_mock.return_value = iter(
        [
            {
                "jobId": batch_job_id,
            }
        ]
    )

    scheduler = mock_scheduler()
    executor = mock_executor(scheduler)
    executor.get_jobs.return_value = [{"jobId": batch_job_id, "jobName": "redun-job-eval_hash"}]
    executor.start()

    # Hand create job.
    job = Job(task1, task1(10))
    job.id = "123"
    job.eval_hash = "eval_hash"
    job.args = ((10,), {})

    # Submit redun job.
    executor.submit(job)

    # Ensure no batch jobs were submitted.
    assert batch_submit_mock.call_count == 0

    # Simulate AWS Batch completing with valid value.
    output_file = File("s3://example-bucket/redun/jobs/eval_hash/output")
    output_file.write(pickle_dumps(task1.func(10)), mode="wb")

    iter_batch_job_status_mock.return_value = iter([{"jobId": batch_job_id, "status": SUCCEEDED}])

    scheduler.batch_wait([job.id])

    # Simulate pre-existing job output.
    output_file = File("s3://example-bucket/redun/jobs/eval_hash/output")
    output_file.write(pickle_dumps(task1.func(10)), mode="wb")

    # Ensure redun job is completed.
    assert scheduler.job_results[job.id] == 20

    executor.stop()


@task(limits={"cpu": 1}, random_option=5)
def array_task(x):
    return x + 10


@task()
def other_task(x, y):
    return x - y


# Tests begin here
def test_job_descrs() -> None:
    """Tests the JobDescription class used to determine if Jobs are equivalent"""
    j1 = Job(array_task, array_task(1))
    j2 = Job(array_task, array_task(2))

    a = job_array.JobDescription(j1)
    b = job_array.JobDescription(j2)

    assert hash(a) == hash(b)
    assert a == b

    j3 = Job(other_task, other_task(1, y=2))
    c = job_array.JobDescription(j3)
    assert a != c


@mock_s3
def test_job_staleness() -> None:
    """Tests staleness criteria for array'ing jobs"""
    j1 = Job(array_task, array_task(1))
    j1.args = ((1,), {})
    d = job_array.JobDescription(j1)

    sched = mock_scheduler()
    exec = mock_executor(sched)
    arr = job_array.JobArrayer(
        exec._submit_jobs,
        exec._on_error,
        submit_interval=10000.0,
        stale_time=0.05,
        min_array_size=5,
    )

    for i in range(10):
        arr.add_job(j1)

    assert arr.get_stale_descrs() == []
    wait_until(lambda: arr.get_stale_descrs() == [d])


@mock_s3
def test_arrayer_thread() -> None:
    """Tests that the arrayer monitor thread can be restarted after exit"""
    j1 = Job(array_task, array_task(1))
    j1.args = ((1,), {})

    sched = mock_scheduler()
    exec = mock_executor(sched)
    arr = job_array.JobArrayer(
        exec._submit_jobs,
        exec._on_error,
        submit_interval=10000.0,
        stale_time=0.05,
        min_array_size=5,
    )

    arr.add_job(j1)
    assert arr._monitor_thread.is_alive()

    # Stop the monitoring thread.
    arr.stop()
    assert not arr._monitor_thread.is_alive()

    # Submitting an additional job should restart the thread.
    arr.add_job(j1)
    assert arr._monitor_thread.is_alive()

    arr.stop()


@mock_s3
@patch("redun.executors.aws_utils.get_aws_user", return_value="alice")
@patch("redun.executors.aws_batch.aws_describe_jobs")
@patch("redun.executors.aws_batch.submit_task")
def test_jobs_are_arrayed(
    submit_task_mock: Mock, aws_describe_jobs_mock: Mock, get_aws_user_mock: Mock
) -> None:
    """
    Tests repeated jobs are submitted as a single array job. Checks that
    job ID for the array job and child jobs end up tracked
    """
    scheduler = mock_scheduler()
    executor = mock_executor(scheduler)
    executor.arrayer.min_array_size = 3
    executor.arrayer.max_array_size = 7

    aws_describe_jobs_mock.return_value = iter([])
    submit_task_mock.side_effect = [
        {"jobId": "first-array-job", "arrayProperties": {"size": 7}},
        {"jobId": "second-array-job", "arrayProperties": {"size": 3}},
        {"jobId": "single-job"},
    ]

    test_jobs = []
    for i in range(10):
        job = Job(array_task, array_task(i))
        job.id = f"task_{i}"
        job.eval_hash = f"eval_hash_{i}"
        job.args = ((i,), {})

        executor.submit(job)
        test_jobs.append(job)

    # Wait for jobs to get submitted from arrayer to executor.
    wait_until(lambda: len(executor.pending_batch_jobs) == 10)

    # Two array jobs, of size 7 and 3, should have been submitted.
    pending_correct = {
        f"first-array-job:{i}": test_jobs[i] for i in range(executor.arrayer.max_array_size)
    }
    pending_correct.update(
        {
            f"second-array-job:{i}": j
            for i, j in enumerate(test_jobs[executor.arrayer.max_array_size :])
        }
    )
    assert executor.pending_batch_jobs == pending_correct

    # Two array jobs should have been submitted
    assert submit_task_mock.call_count == 2

    # Submit a different kind of job now.
    j = Job(other_task, other_task(3, 5))
    j.id = "other_task"
    j.eval_hash = "hashbrowns"
    j.args = ((3, 5), {})
    executor.submit(j)

    assert len(executor.arrayer.pending) == 1
    pending_correct["single-job"] = j
    wait_until(lambda: executor.pending_batch_jobs == pending_correct)

    # Make monitor thread exit correctly
    executor.stop()


@use_tempdir
@mock_s3
@patch("redun.executors.aws_utils.get_aws_user", return_value="alice")
@patch("redun.executors.aws_batch.AWSBatchExecutor._submit_single_job")
def test_array_disabling(submit_single_mock, get_aws_user_mock):
    """
    Tests setting `min_array_size=0` disables job arraying.
    """
    # Setup executor.
    config = Config(
        {
            "batch": {
                "image": "image",
                "queue": "queue",
                "s3_scratch": "s3_scratch_prefix",
                "code_includes": "*.txt",
                "min_array_size": 0,
            }
        }
    )
    scheduler = mock_scheduler()

    executor = AWSBatchExecutor("batch", scheduler, config["batch"])
    executor.get_jobs = Mock()
    executor.get_jobs.return_value = []

    # Submit one test job.
    job = Job(other_task, other_task(5, 3))
    job.id = "carrots"
    job.eval_hash = "why do i always say carrots in test cases idk"
    job.args = ((5, 3), {})
    executor.submit(job)

    # Job should be submitted immediately.
    assert submit_single_mock.call_args
    assert submit_single_mock.call_args[0] == (job,)

    # Monitor thread should not run.
    assert not executor.arrayer._monitor_thread.is_alive()
    executor.stop()


@mock_s3
@use_tempdir
@patch("redun.executors.aws_batch.batch_submit")
def test_array_job_s3_setup(batch_submit_mock: Mock) -> None:
    """
    Tests that args, kwargs, and output file paths end up
    in the correct locations in S3 as the right data structure
    """
    scheduler = mock_scheduler()
    executor = mock_executor(scheduler)
    executor.s3_scratch_prefix = "./evil\ndirectory"

    batch_submit_mock.return_value = {
        "jobId": "array-job-id",
        "arrayProperties": {"size": "10"},
    }

    test_jobs = []
    for i in range(10):
        job = Job(other_task, other_task(i, y=2 * i))
        job.id = f"task_{i}"
        job.eval_hash = f"hash_{i}"
        job.args = ((i,), {"y": 2 * i})
        test_jobs.append(job)

    array_uuid = executor._submit_array_job(test_jobs)

    # Check input file is on S3 and contains list of (args, kwargs) tuples
    input_file = File(
        get_array_scratch_file(executor.s3_scratch_prefix, array_uuid, SCRATCH_INPUT)
    )
    assert input_file.exists()

    with input_file.open("rb") as infile:
        arglist, kwarglist = pickle.load(infile)
    assert arglist == [(i,) for i in range(10)]
    assert kwarglist == [{"y": 2 * i} for i in range(10)]

    # Check output paths file is on S3 and contains correct output paths
    output_file = File(
        get_array_scratch_file(executor.s3_scratch_prefix, array_uuid, SCRATCH_OUTPUT)
    )
    assert output_file.exists()
    ofiles = json.loads(output_file.read())

    assert ofiles == [
        get_job_scratch_file(executor.s3_scratch_prefix, j, SCRATCH_OUTPUT) for j in test_jobs
    ]

    # Error paths are the same as output, basically
    error_file = File(
        get_array_scratch_file(executor.s3_scratch_prefix, array_uuid, SCRATCH_ERROR)
    )
    assert error_file.exists()
    efiles = json.loads(error_file.read())

    assert efiles == [
        get_job_scratch_file(executor.s3_scratch_prefix, j, SCRATCH_ERROR) for j in test_jobs
    ]

    # Child job eval hashes should be present as well.
    eval_file = File(
        get_array_scratch_file(executor.s3_scratch_prefix, array_uuid, SCRATCH_HASHES)
    )
    with eval_file.open("r") as evfile:
        hashes = evfile.read().splitlines()

    assert hashes == [job.eval_hash for job in test_jobs]

    # Make monitor thread exit correctly
    executor.stop()


@mock_s3
@use_tempdir
@patch("redun.executors.aws_batch.batch_submit")
def test_array_oneshot(batch_submit_mock: Mock) -> None:
    """
    Checks array child jobs can fetch their args and kwargs, and
    put their (correct) output in the right place.
    """
    # Create a code file
    file = File("workflow.py")
    file.write(
        """
from redun import task

@task()
def other_task(x, y):
   return x / y
        """
    )
    create_tar("code.tar.gz", ["workflow.py"])
    file.remove()

    # Submit 10 jobs that will be arrayed
    scheduler = mock_scheduler()
    executor = mock_executor(scheduler)
    executor.s3_scratch_prefix = "."

    batch_submit_mock.return_value = {
        "jobId": "array-job-id",
        "arrayProperties": {"size": "10"},
    }

    test_jobs = []
    for i in range(3):
        job = Job(other_task, other_task(i, y=2 * i))
        job.id = f"task_{i}"
        job.eval_hash = f"hash_{i}"
        job.args = ((i,), {"y": 2 * i})
        test_jobs.append(job)

    array_uuid = executor._submit_array_job(test_jobs)

    # Now run 2 of those jobs and make sure they work ok
    client = RedunClient()
    array_dir = os.path.join(executor.s3_scratch_prefix, "array_jobs", array_uuid)
    input_path = os.path.join(array_dir, SCRATCH_INPUT)
    output_path = os.path.join(array_dir, SCRATCH_OUTPUT)
    error_path = os.path.join(array_dir, SCRATCH_ERROR)
    executor.stop()

    for i in range(3):
        os.environ[job_array.AWS_ARRAY_VAR] = str(i)
        try:
            client.execute(
                [
                    "redun",
                    "oneshot",
                    "workflow.py",
                    "--code",
                    "code.tar.gz",
                    "--array-job",
                    "--input",
                    input_path,
                    "--output",
                    output_path,
                    "--error",
                    error_path,
                    "other_task",
                ]
            )

            # Check expected output.
            output_file = File(
                get_job_scratch_file(executor.s3_scratch_prefix, test_jobs[i], SCRATCH_OUTPUT)
            )
            assert pickle.loads(cast(bytes, output_file.read("rb"))) == i / (2 * i)
        except Exception:
            # Check expected error.
            error_file = File(
                get_job_scratch_file(executor.s3_scratch_prefix, test_jobs[i], SCRATCH_ERROR)
            )
            error, _ = pickle.loads(cast(bytes, error_file.read("rb")))
            assert isinstance(error, ZeroDivisionError)


@patch("redun.executors.aws_batch.aws_describe_jobs")
def test_get_log_stream(aws_describe_jobs_mock) -> None:
    """Test the log stream fetching utility, which handles the difference in behavior for
    multi-node."""

    aws_region = "fake_region"

    # Heavily pruned `JobDetail` messages
    single_node_status = {
        "jobArn": "arn:aws:batch:us-west-2:298579124006:job/89ee416c",
        "jobName": "redun-testing-c813121061b56b5934d5db78730ada2e2ae11e44",
        "jobId": "single_node_id",
        "status": "FAILED",
        "container": {
            "logStreamName": "log_stream_arn",
        },
        "nodeDetails": {"nodeIndex": 0, "isMainNode": True},
    }

    # This is the overall message for a multi-node job. There is no log stream, but there is
    # a `nodeProperties` field.
    multi_node_status = {
        "jobArn": "arn:aws:batch:us-west-2:298579124006:job/89ee416c-1ab1-457c-b728-1f10318f61bb",
        "jobName": "redun-testing-c813121061b56b5934d5db78730ada2e2ae11e44",
        "jobId": "multi_node_id",
        "attempts": [
            {
                "container": {
                    "exitCode": 1,
                    "logStreamName": "cannot_be_returned",
                    "networkInterfaces": [],
                },
                "startedAt": 1663274825002,
                "stoppedAt": 1663274840060,
                "statusReason": "Essential container in task exited",
            }
        ],
        "createdAt": 1663274377312,
        "retryStrategy": {"attempts": 2, "evaluateOnExit": []},
        "startedAt": 1663274957408,
        "parameters": {},
        "nodeProperties": {
            "numNodes": 3,
            "mainNode": 0,
            "nodeRangeProperties": [
                {
                    "targetNodes": "0",
                    "container": {
                        "image": "image_arn",
                    },
                },
                {
                    "targetNodes": "1:",
                    "container": {
                        "image": "image_arn",
                    },
                },
            ],
        },
    }

    # No query is required for single node
    aws_describe_jobs_mock.return_value = None
    assert get_job_log_stream(single_node_status, aws_region) == "log_stream_arn"

    # Multi node will query again and fetch from there.
    aws_describe_jobs_mock.return_value = iter([single_node_status])
    assert get_job_log_stream(multi_node_status, aws_region) == "log_stream_arn"
    assert aws_describe_jobs_mock.call_args == unittest.mock.call(
        ["multi_node_id#0"], aws_region=aws_region
    )


def test_equiv_job_def() -> None:
    """Check our equality testing for job definitions."""

    basic_job = get_job_details(image="some_image", role="some_role")
    # This is derived from a real query for a job description from batch, lightly redacted.
    # It has lots of keys we don't set, but shouldn't cause errors.
    existing_job = {
        "jobDefinitionName": "jobDefinitionName",
        "jobDefinitionArn": "jobDefinitionArn",
        "revision": 12345,
        "status": "ACTIVE",
        "type": "container",
        "parameters": {},
        "containerProperties": {
            "image": "some_image",
            "vcpus": 1,
            "memory": 4,
            "command": ["ls"],
            "jobRoleArn": "some_role",
            "volumes": [],
            "environment": [],
            "mountPoints": [],
            "privileged": False,
            "ulimits": [],
            "resourceRequirements": [],
            "secrets": [],
        },
        "tags": {},
    }

    assert equiv_job_def(basic_job, existing_job)


################################################################################################
# Fair Share Scheduling related tests
################################################################################################


def test_fss_executor_config(scheduler: Scheduler) -> None:
    """
    FSS config variables flow are recognized by Executor.
    """
    # Setup executor.
    config = Config(
        {
            "batch": {
                "image": "image",
                "queue": "queue",
                "s3_scratch": "s3_scratch_prefix",
                "share_id": "team1",
                "scheduling_priority_override": 20,
            }
        }
    )
    executor = AWSBatchExecutor("batch", scheduler, config["batch"])

    assert executor.image == "image"
    assert executor.queue == "queue"
    assert executor.s3_scratch_prefix == "s3_scratch_prefix"
    assert isinstance(executor.code_package, dict)
    assert executor.debug is False

    @task(namespace="test")
    def task1(x):
        return x

    exec1 = Execution("123")
    job = Job(task1, task1(10), execution=exec1)

    options = executor._get_job_options(job)
    assert options["share_id"] == "team1"
    assert options["scheduling_priority_override"] == 20


def test_fss_task_override(scheduler: Scheduler) -> None:
    """
    FSS config variables can be overridden on Task
    """
    # Setup executor.
    config = Config(
        {
            "batch": {
                "image": "image",
                "queue": "queue",
                "s3_scratch": "s3_scratch_prefix",
                "share_id": "team1",
                "scheduling_priority_override": 20,
            }
        }
    )
    executor = AWSBatchExecutor("batch", scheduler, config["batch"])
    executor._aws_user = "alice"

    @task(share_id="subteam2", scheduling_priority_override=99, namespace="test")
    def task1(x):
        return x

    exec1 = Execution("123")
    job = Job(task1, task1(10), execution=exec1)

    options = executor._get_job_options(job)
    assert options["share_id"] == "subteam2"
    assert options["scheduling_priority_override"] == 99
