from random import random
from time import sleep
from typing import Optional

import pytest

from redun.backends.db import Job as JobDb
from redun.backends.db import RedunBackendDb
from redun.config import Config
from redun.scheduler import Scheduler
from redun.task import task
from redun.value import Value

API_LIMIT = 10
MEMORY_LIMIT = 3
CONFIG_DICT = {"limits": {"api": str(API_LIMIT), "memory": str(MEMORY_LIMIT)}}


def test_scheduler_limits_config() -> None:
    """
    Scheduled should correctly interpret limits from config and should convert limits to ints.
    """
    scheduler = Scheduler(config=Config(CONFIG_DICT))
    assert scheduler.limits == {key: int(value) for key, value in CONFIG_DICT["limits"].items()}


def test_scheduler_limits_utils() -> None:
    """
    Test the limits utility methods on scheduler for consuming, releasing, and checking limits.
    """
    scheduler = Scheduler(config=Config(CONFIG_DICT))

    job_limits = {"api": 1, "memory": MEMORY_LIMIT}

    # Check that a job that should have enough resources to run is correctly deemed within limits.
    assert scheduler._is_job_within_limits(job_limits)

    # Consume all resources for one limit and make sure scheduler's internal accounting is correct.
    scheduler._consume_resources(job_limits)
    assert scheduler.limits_used == {"api": job_limits["api"], "memory": MEMORY_LIMIT}

    # The scheduler should now recognize that it cannot fit another job given consumed resources.
    assert not scheduler._is_job_within_limits(job_limits)

    # Now, let's release the limits from the job and confirm scheduler's accounting.
    scheduler._release_resources(job_limits)
    assert scheduler.limits_used == {limit_name: 0 for limit_name in CONFIG_DICT["limits"]}


class API(Value):
    """
    Trivial mock of a throttled API.

    This class will raise an exception if connect() would cause current connection count to be
    higher than the maximum connection count allowed.
    """

    def __init__(self, max_connections):
        self.current_connections = 0
        self.max_connections = max_connections
        self.max_connections_exceeded = False

    def connect(self):
        if self.current_connections == self.max_connections:
            self.max_connections_exceeded = True

        self.current_connections += 1

    def close(self):
        self.current_connections -= 1
        # Just a sanity check to make sure we are not somehow getting to negative connection count.
        assert self.current_connections >= 0

    def get_hash(self, data: Optional[bytes] = None) -> str:
        """We don't want the hash to change, because that will prevent cache hits, depending upon
        the order in which the jobs get resolved."""
        return "constant_hash"

    def __getstate__(self):
        return {
            "current_connections": self.current_connections,
            "max_connections": self.max_connections,
            "max_connections_exceeded": self.max_connections_exceeded,
        }

    def __setstate__(self, state):
        self.current_connections = state["current_connections"]
        self.max_connections = state["max_connections"]
        self.max_connections_exceeded = state["max_connections_exceeded"]


@pytest.mark.parametrize("use_cache", [False, True])
def test_scheduler_respects_limits(use_cache: bool) -> None:
    """
    Used to check that the scheduler is deferring jobs and scheduling them as resources are freed.
    This should work with or without the cache.
    """

    @task(limits=["api"], cache=use_cache)
    def limit_task(api, i):
        """
        Task to connect to the "API", sleep a bit, then disconnect from the "API".

        We include the `i` argument so that each task is different in the eyes of the scheduler. If
        we just passed api, the scheduler would see N of this task as only needing one execution.
        """
        api.connect()
        sleep(random())
        api.close()

    @task()
    def workflow(api):
        return [limit_task(api, i // 2) for i in range(10)]

    # If we run a workflow with more tasks than the API allows, we should exceed the maximum
    # connections because the scheduler has the resource limit set so high.
    # This ensures that our test actually would fail if limiting doesn't work.
    max_connections = 2
    api = API(max_connections)
    scheduler = Scheduler(config=Config({"limits": {"api": max_connections * 2}}))
    scheduler.run(workflow(api))
    assert api.max_connections_exceeded

    # Now, we can do the same experiment as above but we can use a scheduler without resource
    # limits defined. In this case, the limit should default to 1 and we should not exceed the
    # maximum connections of the API instance.
    api = API(max_connections)
    scheduler = Scheduler()
    scheduler.run(workflow(api))
    assert not api.max_connections_exceeded

    # If we used the cache, half the jobs should be cached.
    if use_cache:
        assert isinstance(scheduler.backend, RedunBackendDb)
        assert scheduler.backend.session
        assert (
            scheduler.backend.session.query(JobDb)
            .filter(JobDb.task_hash == limit_task.hash, JobDb.cached == False)  # noqa: E712
            .count()
            == 5
        )
        # Expression cycles don't show up here.
        assert (
            scheduler.backend.session.query(JobDb)
            .filter(JobDb.task_hash == limit_task.hash, JobDb.cached == True)  # noqa: E712
            .count()
            == 0
        )
    else:
        if use_cache:
            assert isinstance(scheduler.backend, RedunBackendDb)
            assert scheduler.backend.session
            assert (
                scheduler.backend.session.query(JobDb)
                .filter(JobDb.task_hash == limit_task.hash, JobDb.cached == False)  # noqa: E712
                .count()
                == 10
            )


def test_scheduler_cache_limits() -> None:
    """
    Used to check that the scheduler is deferring jobs and scheduling them as resources are freed.
    """

    @task(limits=["api"], check_valid="full")
    def inner():
        return None

    @task(limits=["api"], check_valid="shallow")
    def workflow():
        return inner()

    # If we run a workflow with more tasks than the API allows, we should exceed the maximum
    # connections because the scheduler has the resource limit set so high.
    scheduler = Scheduler(config=Config({"limits": {"api": 1}}))

    # Check that limits are recovered after the execution, with a cache miss
    scheduler.run(workflow())
    assert scheduler.limits_used == {"api": 0}

    # Or a cache hit.
    scheduler.run(workflow())
    assert scheduler.limits_used == {"api": 0}
