from typing import cast

import pytest
from sqlalchemy.orm import Session

from redun import File, Scheduler, task
from redun.backends.db.query import CallGraphQuery, Job, infer_id
from redun.scheduler import scheduler_task
from redun.tests.utils import use_tempdir


def test_infer_id(scheduler: Scheduler, session: Session) -> None:
    """
    infer_id should be able to detect specialty ids (-, ~2, etc).
    """

    @task
    def add(x: int, y: int) -> int:
        return x + y

    # Perform some Executions.
    assert scheduler.run(add(1, 2)) == 3
    assert scheduler.run(add(1, 3)) == 4
    assert scheduler.run(add(1, 4)) == 5

    # Get most recent job.
    exec1 = infer_id(session, "-")
    assert exec1.job.call_node.value_parsed == 5

    exec1 = infer_id(session, "~1")
    assert exec1.job.call_node.value_parsed == 5

    # Get second most recent job.
    exec2 = infer_id(session, "~2")
    assert exec2.job.call_node.value_parsed == 4

    # Get third most recent job.
    exec3 = infer_id(session, "~3")
    assert exec3.job.call_node.value_parsed == 3

    # Infer id by prefix.
    assert infer_id(session, exec1.id[:8]) == exec1


@use_tempdir
def test_infer_id_file(scheduler: Scheduler, session: Session) -> None:
    """
    infer_id should be able to detect filenames.
    """

    @task
    def write_file(name: str) -> File:
        file = File(name)
        file.write("hello")
        return file

    @task
    def read_file(file: File) -> str:
        return cast(str, file.read())

    # Perform some Executions.
    scheduler.run(write_file("file1"))

    file = File("file2")
    file.write("bye")
    scheduler.run(read_file(file))

    # Get file info.
    file_info = infer_id(session, "file1")
    assert file_info[0].path == "file1"
    assert isinstance(file_info[1], Job)
    assert file_info[1].call_node.value_parsed.path == "file1"
    assert file_info[2] == "result"

    file_info = infer_id(session, "file2")
    assert file_info[0].path == "file2"
    assert isinstance(file_info[1], Job)
    assert file_info[1].call_node.arguments[0].value_parsed.path == "file2"
    assert file_info[2] == "arg"


def test_status(scheduler: Scheduler, session: Session) -> None:
    """
    We should be able to filter on Execution and Job status correctly.
    """

    @task
    def div(x: float, y: float) -> float:
        return x / y

    @scheduler_task()
    def kill(scheduler, parent_job, expr):
        # Immediately kill the workflow. This skips recording end_time of the active jobs, so they
        # will be considered RUNNING.
        scheduler.reject_job(None, Exception())

    @task
    def do_kill():
        return kill()

    # Perform some Executions.
    assert scheduler.run(div(4, 2)) == 2
    assert scheduler.run(div(4, 2)) == 2
    assert scheduler.run(div(8, 2)) == 4

    with pytest.raises(ZeroDivisionError):
        assert scheduler.run(div(1, 0))

    with pytest.raises(Exception):
        assert scheduler.run(do_kill())

    query = CallGraphQuery(session)

    # Assert job statuses are correct.
    assert len(list(query.filter_job_statuses(["DONE"]).all())) == 2
    assert len(list(query.filter_job_statuses(["CACHED"]).all())) == 1
    assert len(list(query.filter_job_statuses(["FAILED"]).all())) == 1
    assert len(list(query.filter_job_statuses(["RUNNING"]).all())) == 1

    # Assert execution statuses are correct.
    assert len(list(query.filter_execution_statuses(["DONE"]).all())) == 3
    assert len(list(query.filter_execution_statuses(["FAILED"]).all())) == 1
    assert len(list(query.filter_execution_statuses(["RUNNING"]).all())) == 1
