from random import random
from time import sleep

from redun.config import Config
from redun.scheduler import Scheduler
from redun.task import task

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


class API:
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


def test_scheduler_respects_limits() -> None:
    """
    Used to check the the scheduler is deferring jobs and scheduling them as resources are freed.
    """

    @task(limits=["api"], cache=False)
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
        return [limit_task(api, i) for i in range(5)]

    # If we run a workflow with more tasks than the API allows, we should exceed the maximum
    # connections because the scheduler has the resource limit set so high..
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
