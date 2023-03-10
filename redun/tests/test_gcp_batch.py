from unittest.mock import patch

from redun.config import Config
from redun.executors.gcp_batch import GCPBatchExecutor
from redun.scheduler import Scheduler
from redun.tests.utils import mock_scheduler

PROJECT = "project"
IMAGE = "gcr.io/gcp-project/redun"
REGION = "region"
GCS_SCRATCH_PREFIX = "gs://example-bucket/redun/"


def mock_executor(scheduler, debug=False, code_package=False):
    """
    Returns an GCPBatchExecutor with GCP API mocks.
    """

    config = Config(
        {
            "gcp_batch": {
                "gcs_scratch": GCS_SCRATCH_PREFIX,
                "project": PROJECT,
                "region": PROJECT,
                "image": IMAGE,
                "code_package": code_package,
                "debug": debug,
            }
        }
    )

    return GCPBatchExecutor("batch", scheduler, config["gcp_batch"])


def test_executor_config(scheduler: Scheduler) -> None:
    """
    Executor should be able to parse its config.
    """
    # Setup executor.
    config = Config(
        {
            "gcp_batch": {
                "image": IMAGE,
                "project": PROJECT,
                "region": REGION,
                "gcs_scratch": GCS_SCRATCH_PREFIX,
            }
        }
    )
    executor = GCPBatchExecutor("batch", scheduler, config["gcp_batch"])
    assert executor.image == IMAGE
    assert executor.project == PROJECT
    assert executor.region == REGION
    assert executor.gcs_scratch_prefix == GCS_SCRATCH_PREFIX
    assert isinstance(executor.code_package, dict)
    assert executor.debug is False


@patch("redun.executors.gcp_utils.get_gcp_client")
def test_executor_creation(get_gcp_client) -> None:
    """
    Ensure we reunite with inflight jobs
    """
    assert get_gcp_client.call_count == 0

    scheduler = mock_scheduler()
    mock_executor(scheduler)

    assert get_gcp_client.call_count == 1
