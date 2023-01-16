from unittest.mock import patch

import boto3
import pytest

import redun
from redun import File, task
from redun.executors.code_packaging import package_code
from redun.executors.command import REDUN_REQUIRED_VERSION
from redun.executors.gcp_batch import submit_task
from redun.file import Dir
from redun.scheduler import Job
from redun.tests.utils import mock_s3, use_tempdir


@task()
def task1(x):
    return x + 10


@task(load_module="custom.module")
def task1_custom_module(x):
    return x + 10


# @use_tempdir
# @mock_s3
# @patch("redun.executors.gcp_batch.batch_submit")
# @pytest.mark.parametrize(
#     "custom_module, expected_load_module, a_task",
#     [
#         (None, "redun.tests.test_gcp_batch", task1),
#         ("custom.module", "custom.module", task1_custom_module),
#     ],
# )
# def test_submit(batch_submit_mock, custom_module, expected_load_module, a_task):
def test_submit():
    job_id = "batch-job"
    image = "us-west2-docker.pkg.dev/retro-cloud/redun-repo/scrna-seq-wf:0.1.15"
    # image = "my-image"

    # scratch_prefix = "s3://example-bucket/redun/"
    scratch_prefix = "gs://retro_test_data/redun/"

    custom_module = None
    a_task = task1

    # client = boto3.client("s3", region_name="us-east-1")
    google_access_key_id = ""
    google_access_key_secret = ""
    _ = boto3.client(
        "s3",
        region_name="auto",
        endpoint_url="https://storage.googleapis.com",
        aws_access_key_id=google_access_key_id,
        aws_secret_access_key=google_access_key_secret,
    )
    # client.create_bucket(Bucket="example-bucket")

    # redun.executors.gcp_batch.batch_submit.return_value = {"name": "batch-job-id"}

    # Create example workflow script to be packaged.
    File("workflow.py").write(
        f"""
from redun import task
@task(load_module={custom_module})
def task1(x):
    return x + 10
    """
    )

    job = Job(a_task())
    job.id = job_id
    job.eval_hash = "eval-hash-9"
    code_file = package_code(scratch_prefix)
    resp = submit_task(
        image,
        scratch_prefix,
        job,
        a_task,
        args=[10],
        kwargs={},
        code_file=code_file,
        gcp_region="us-west1",
    )
    import pdb

    pdb.set_trace()
    print(resp)

    # We should get a AWS Batch job id back.
    # assert resp.name == "batch-job-id"

    # Input files should be made.
    assert File("s3://example-bucket/redun/jobs/eval_hash/input").exists()
    [code_file] = list(Dir("s3://example-bucket/redun/code"))

    # We should have submitted a job to AWS Batch.
    # redun.executors.gcp_batch.batch_submit.assert_called_with(
    #     [
    #         "redun",
    #         "--check-version",
    #         REDUN_REQUIRED_VERSION,
    #         "oneshot",
    #         expected_load_module,
    #         "--code",
    #         code_file.path,
    #         "--input",
    #         "s3://example-bucket/redun/jobs/eval_hash/input",
    #         "--output",
    #         "s3://example-bucket/redun/jobs/eval_hash/output",
    #         "--error",
    #         "s3://example-bucket/redun/jobs/eval_hash/error",
    #         a_task.name,
    #     ],
    #     image="my-image",
    #     job_name="batch-job-eval_hash",
    #     job_def_suffix="-redun-jd",
    #     gcp_region="us-east-1",
    #     array_size=0,
    # )


@use_tempdir
@mock_s3
@patch("redun.executors.gcp_batch.batch_submit")
@pytest.mark.parametrize(
    "custom_module, expected_load_module, a_task",
    [
        (None, "redun.tests.test_gcp_batch", task1),
        ("custom.module", "custom.module", task1_custom_module),
    ],
)
def test_submit_task(batch_submit_mock, custom_module, expected_load_module, a_task):
    job_id = "batch-job"
    image = "my-image"

    scratch_prefix = "s3://example-bucket/redun/"

    client = boto3.client("s3", region_name="us-east-1")
    client.create_bucket(Bucket="example-bucket")

    # redun.executors.gcp_batch.batch_submit.return_value = {"jobId": "batch-job-id"}

    # Create example workflow script to be packaged.
    File("workflow.py").write(
        f"""
@task(load_module={custom_module})
def task1(x):
    return x + 10
    """
    )

    job = Job(a_task())
    job.id = job_id
    job.eval_hash = "eval_hash"
    code_file = package_code(scratch_prefix)
    _ = submit_task(
        image,
        scratch_prefix,
        job,
        a_task,
        args=[10],
        kwargs={},
        code_file=code_file,
        gcp_region="us-east-1",
    )

    # We should get a AWS Batch job id back.
    # assert resp["jobId"] == "batch-job-id"

    # Input files should be made.
    assert File("s3://example-bucket/redun/jobs/eval_hash/input").exists()
    [code_file] = list(Dir("s3://example-bucket/redun/code"))

    # We should have submitted a job to AWS Batch.
    redun.executors.gcp_batch.batch_submit.assert_called_with(
        [
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
        ],
        image="my-image",
        job_name="batch-job-eval_hash",
        job_def_suffix="-redun-jd",
        gcp_region="us-east-1",
        array_size=0,
    )
