import pytest

from redun.scheduler import Scheduler


@pytest.fixture
def scheduler() -> Scheduler:
    """
    Returns an in memory Scheduler for testing.
    """
    return Scheduler()
