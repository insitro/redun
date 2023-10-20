import os
import subprocess
import time

from redun import File, Scheduler
from redun.cli import RedunClient
from redun.config import Config
from redun.executors.postgres import PgExecutor
from redun.tests.utils import use_tempdir


def test_pg_executor_config(scheduler: Scheduler) -> None:
    """
    Check correct parsing of config object
    """

    config_string = """
[executors.default]
type = pg
dsn = postgresql://postgres:postgres@localhost:5432/postgres
scratch_root = /tmp/xyz

[executors.alt_connection]
type = pg
dbname = postgres
user = postgres
password = postgres
host = postgres
port = 5432

[executors.alt_queues]
type = pg
dsn = postgresql://postgres:postgres@localhost:5432/postgres
queue_args = custom_queue_args
queue_results = custom_queue_results
"""

    config = Config()
    config.read_string(config_string)
    scheduler = Scheduler(config=config)

    assert isinstance(scheduler.executors["default"], PgExecutor)
    assert isinstance(scheduler.executors["alt_connection"], PgExecutor)
    assert isinstance(scheduler.executors["alt_queues"], PgExecutor)

    assert scheduler.executors["default"].scratch_root() == "/tmp/xyz"
    assert scheduler.executors["alt_connection"].scratch_root() == "/tmp"

    assert scheduler.executors["default"]._conn_opt == {
        "dsn": "postgresql://postgres:postgres@localhost:5432/postgres",
    }
    assert scheduler.executors["alt_connection"]._conn_opt["port"] == "5432"

    assert scheduler.executors["default"]._queue_args == PgExecutor.DEFAULT_QUEUE_ARGS
    assert scheduler.executors["default"]._queue_results == PgExecutor.DEFAULT_QUEUE_RESULTS

    assert scheduler.executors["alt_queues"]._queue_args == "custom_queue_args"
    assert scheduler.executors["alt_queues"]._queue_results == "custom_queue_results"


@use_tempdir
def test_pg_executor_simple_workflow(scheduler, testdb) -> None:
    """
    Run PgExecutor on simple workflow
    """

    File("workflow.py").write(
        """
from redun import task

@task
def main():
    return 10
"""
    )

    os.makedirs(".redun")
    File(".redun/redun.ini").write(
        f"""
[backend]
db_uri = {testdb}
automigrate = True

[executors.default]
type = pg
dsn = {testdb}
"""
    )

    File("worker.py").write(
        """
import workflow

from redun.cli import RedunClient
client = RedunClient()
client.execute(["redun", "pg-executor-worker", "default"])
"""
    )

    worker = subprocess.Popen(["python", "worker.py"])
    # make sure worker is still running after 1s, in case we have syntax errors in it,
    # otherwise `run` command sleeps forever
    time.sleep(1.0)
    assert worker.poll() is None, "worker died"
    try:
        client = RedunClient()
        assert client.execute(["redun", "run", "workflow.py", "main"]) == 10
    finally:
        worker.kill()

    # try again, but this time, start the worker after the scheduler
    worker = subprocess.Popen("sleep 1.5 && python worker.py", shell=True)
    try:
        client = RedunClient()
        assert client.execute(["redun", "run", "workflow.py", "main"]) == 10
    finally:
        worker.kill()
