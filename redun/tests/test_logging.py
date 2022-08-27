import os
import tempfile

from redun import File, Scheduler, task
from redun.backends.db import Execution
from redun.backends.db import File as RowFile
from redun.backends.db import Job, RedunBackendDb, Value


def test_record_job(scheduler: Scheduler) -> None:
    @task()
    def task1():
        return 10

    @task()
    def workflow(x):
        return task1()

    scheduler.run(workflow(1), exec_argv=["redun", "run", "workflow.py", "workflow", "--x", "1"])
    scheduler.run(workflow(2), exec_argv=["redun", "run", "workflow.py", "workflow", "--x", "2"])

    assert isinstance(scheduler.backend, RedunBackendDb)
    assert scheduler.backend.session
    jobs = scheduler.backend.session.query(Job).order_by(Job.start_time).all()
    assert len(jobs) == 4

    # Ensure the jobs were recorded.
    assert jobs[0].task.name == "workflow"
    assert jobs[1].task.name == "task1"
    assert jobs[0].task.source == "    def workflow(x):\n        return task1()\n"

    # Tasks should also point back to their jobs.
    assert len(jobs[0].task.jobs) == 2

    # The second pair of jobs are partially cached.
    assert not jobs[2].cached
    assert jobs[3].cached

    # Jobs should be connected into a tree.
    assert jobs[0].child_jobs[0].task.name == "task1"
    assert jobs[0].child_jobs[0].parent_job.task.name == "workflow"

    # CallNodes should be connected into a graph.
    assert jobs[0].call_node.child_edges[0].child_node.task.name == "task1"
    assert jobs[0].call_node.parents == []
    assert jobs[0].call_node.children[0].task.name == "task1"

    executions = scheduler.backend.session.query(Execution).all()
    assert executions[0].job.task.name == "workflow"

    assert executions[0].job.call_node.value_parsed == 10


def test_record_multicall(scheduler: Scheduler) -> None:
    """
    CallNodes can have repeat children and parents. The database should
    not apply a distinct to call_node.children or call_node.parents.
    """

    @task
    def id(x):
        return x

    @task
    def task1(x):
        return x

    @task
    def workflow():
        return [
            task1(10),
            task1(id(10)),
        ]

    scheduler.run(workflow())

    assert isinstance(scheduler.backend, RedunBackendDb)
    assert scheduler.backend.session
    jobs = scheduler.backend.session.query(Job).order_by(Job.start_time).all()

    # Let's get the top CallNode.
    call_node = jobs[0].call_node
    assert call_node.task.name == "workflow"

    # We should have three child CallNodes, even though two are the same CallNode.
    assert len(call_node.children) == 3
    assert len(set(call_node.children)) == 2

    child_node = call_node.children[0]
    assert len(child_node.parents) == 2
    assert len(set(child_node.parents)) == 1


def test_record_file(scheduler: Scheduler) -> None:
    """
    We should record files that are pass through tasks.
    """

    @task()
    def task1(path):
        file = File(os.path.join(path, "myfile"))
        file.write("hello")
        return file

    @task()
    def task2(file):
        return file.read()

    @task()
    def main(path):
        file = task1(path)
        return [task2(file), file]

    with tempfile.TemporaryDirectory() as tmpdir:
        scheduler.run(main(tmpdir))

    # Our file should have been recorded by its type.
    assert isinstance(scheduler.backend, RedunBackendDb)
    assert scheduler.backend.session
    my_file = scheduler.backend.session.query(Value).filter_by(type="redun.File").one()

    # We should have a record of which tasks returned this File.
    # TODO: Could I have a record that the file came from main() as well?
    assert {result.task.name for result in my_file.results} == {"task1"}

    # We also have a record of who accepted this File as input.
    assert my_file.arguments[0].arg_position == 0
    assert my_file.arguments[0].call_node.task.name == "task2"

    # Ensure subvalues were detected.
    # my_file was also part of a list that was returned from main.
    assert my_file.parents[0].type == "builtins.list"
    assert my_file.parents[0].results[0].task.name == "main"

    # Ensure ee can get File information like path.
    assert my_file.file.path
    path = my_file.file.path

    # We can also search the logs starting with file path.
    files = scheduler.backend.session.query(RowFile).filter_by(path=path).all()
    my_file2 = files[0].value
    assert {result.task.name for result in my_file2.results} == {"task1"}
