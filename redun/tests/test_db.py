from collections import Counter
from typing import Callable, Dict, List, Set, cast
from unittest import mock
from unittest.mock import patch

import pytest
from sqlalchemy.orm import Session
from sqlalchemy.sql.expression import cast as sa_cast

from redun import Handle, Scheduler, catch, cond, task
from redun.backends.db import JSON, REDUN_DB_VERSIONS, CallNode, DBVersionInfo, Execution
from redun.backends.db import Handle as DbHandle
from redun.backends.db import (
    HandleEdge,
    Job,
    PreviewValue,
    RedunBackendDb,
    RedunDatabaseError,
    RedunMigration,
    RedunVersion,
    RedunVersionError,
    Tag,
    TagEntity,
    Value,
    parse_db_version,
)
from redun.config import Config, create_config_section
from redun.file import Dir, File
from redun.functools import seq
from redun.tests.utils import use_tempdir
from redun.utils import PreviewClass


def test_cache_db() -> None:
    backend = RedunBackendDb(db_uri="sqlite:///:memory:")
    backend.load()

    # Set values into cache and retrieve them.
    value_hash1 = backend.record_value("myvalue")
    value_hash2 = backend.record_value(10)
    assert backend.get_value(value_hash1) == ("myvalue", True)
    assert backend.get_value(value_hash2) == (10, True)

    # Unknown value should return None.
    assert backend.get_value("unknown_call_hash") == (None, False)

    # Reinserting should be a no-op.
    backend.record_value("myvalue") == value_hash1


def test_max_value_size() -> None:
    """
    Test max_value_size config variable changes the max size limit for caching.
    """
    config_dict = {"max_value_size": 100}
    config = create_config_section(config_dict)

    backend = RedunBackendDb(db_uri="sqlite:///:memory:", config=config)
    backend.load()
    small_object = "J" * 10000

    with pytest.raises(RedunDatabaseError):
        backend.record_value(small_object)


def test_call_graph_db() -> None:
    backend = RedunBackendDb(db_uri="sqlite:///:memory:")
    backend.load()
    task_hashes = {
        "task_hash1",
        "task_hash2",
        "task_hash3",
    }

    # Build graph.
    call_hash1 = backend.record_call_node(
        task_name="mytask1",
        task_hash="task_hash1",
        args_hash="args_hash1",
        expr_args=((), {}),
        eval_args=((), {}),
        result_hash="result_hash1",
        child_call_hashes=[],
    )
    call_hash2 = backend.record_call_node(
        task_name="mytask2",
        task_hash="task_hash2",
        args_hash="args_hash2",
        expr_args=((), {}),
        eval_args=((), {}),
        result_hash="result_hash2",
        child_call_hashes=[],
    )
    backend.record_call_node(
        task_name="mytask3",
        task_hash="task_hash3",
        args_hash="args_hash3",
        expr_args=((), {}),
        eval_args=((), {}),
        result_hash="result_hash3",
        child_call_hashes=[call_hash1, call_hash2],
    )

    # Get a node.
    call_node = backend._get_call_node("task_hash3", "args_hash3", task_hashes)
    assert call_node
    assert call_node.task_name == "mytask3"

    # Ensure we can fetch the children.
    assert call_node.children[0].task_name == "mytask1"
    assert call_node.children[1].task_name == "mytask2"

    # Get another node.
    call_node = backend._get_call_node("task_hash1", "args_hash1", task_hashes)
    assert call_node
    assert call_node.task_name == "mytask1"

    # Ensure we can get the parents.
    call_node.parents[0].task_name == "mytask3"


def test_handle_db() -> None:
    backend = RedunBackendDb(db_uri="sqlite:///:memory:")
    backend.load()

    # Create a Handle and pass it through three calls.
    handle = Handle("conn")
    handle2 = handle.apply_call("call_hash2")
    handle3 = handle.apply_call("call_hash3")
    handle4 = handle3.apply_call("call_hash4")
    backend.advance_handle([handle], handle2)
    backend.advance_handle([handle], handle3)
    backend.advance_handle([handle3], handle4)

    # We should have 4 handles and 3 edges.
    assert backend.session
    assert backend.session.query(DbHandle).count() == 4
    assert backend.session.query(HandleEdge).count() == 3

    # They should all be valid.
    assert backend.is_valid_handle(handle)
    assert backend.is_valid_handle(handle2)
    assert backend.is_valid_handle(handle3)
    assert backend.is_valid_handle(handle4)

    # Let's rollback to first handle.
    backend.rollback_handle(handle)

    # Descendants should be invalid.
    assert backend.is_valid_handle(handle)
    assert not backend.is_valid_handle(handle2)
    assert not backend.is_valid_handle(handle3)
    assert not backend.is_valid_handle(handle4)


def test_scheduler_db():
    """
    Scheduler should be able to use the database Backend.
    """
    backend = RedunBackendDb(db_uri="sqlite:///:memory:")
    backend.load()
    scheduler = Scheduler(backend=backend)

    @task()
    def my_task1():
        return 10

    @task()
    def my_task2():
        return my_task1()

    result = scheduler.run(my_task2())
    assert result == 10

    result = scheduler.run(my_task2())
    assert result == 10


def test_db_migrate() -> None:
    """
    Ensure we can safely detect database versions.
    """
    backend = RedunBackendDb(db_uri="sqlite:///:memory:")
    backend.load()
    assert backend.session

    # Assert fresh database is current version and is compatible.
    version = backend.get_db_version()
    latest_version = backend.get_all_db_versions()[-1]
    assert version == latest_version
    assert backend.is_db_compatible()

    # Make an older database (one with a version incompatible with the latest one)
    min_compat_for_latest_version, _ = backend.get_db_version_required()
    prev_incompat_version = [
        v for v in backend.get_all_db_versions() if v < min_compat_for_latest_version
    ][-1]
    backend.migrate(prev_incompat_version)

    # Database should no longer be compatible with library.
    assert not backend.is_db_compatible()

    # Migrating should update the database version.
    backend.migrate()
    assert backend.is_db_compatible()
    assert backend.get_db_version() == latest_version

    # Migrate should not downgrade when upgrade_only=True.
    backend.migrate(desired_version=parse_db_version("1.0"), upgrade_only=True)
    assert backend.get_db_version() == latest_version

    # Make a newer database.
    version_row = (
        backend.session.query(RedunVersion).order_by(RedunVersion.timestamp.desc()).first()
    )
    version_row.version = latest_version.major + 1
    backend.session.add(version_row)
    backend.session.commit()

    with pytest.raises(RedunVersionError, match="too new"):
        backend.migrate()


def test_db_migrate_minor() -> None:
    """
    Ensure we can support minor version numbers.
    """
    backend = RedunBackendDb(db_uri="sqlite:///:memory:")
    backend.load()
    assert backend.session

    latest_version = REDUN_DB_VERSIONS[-1]
    new_migration_id = "abcd1234abcd"
    new_version = DBVersionInfo(
        new_migration_id,
        latest_version.major,
        latest_version.minor + 1,
        "Example minor version.",
    )
    versions_mock = REDUN_DB_VERSIONS + [new_version]

    def upgrade(config, migration_id):
        # Simulate alembic upgrade.
        backend.session.add(RedunMigration(version_num=migration_id))
        backend.session.commit()

    with patch("redun.backends.db.REDUN_DB_VERSIONS", versions_mock), patch(
        "redun.backends.db.upgrade", side_effect=upgrade
    ) as upgrade_mock:
        backend.migrate(new_version)

        # Alembic upgrade should be called.
        assert upgrade_mock.call_args[0][1] == new_migration_id

        # Version should now be updated.
        assert backend.get_db_version() == new_version


def test_task_args(scheduler: Scheduler, session: Session) -> None:
    """
    Arguments to a task should be recorded.
    """

    @task()
    def task1(a, b, c=10, d=20):
        return a

    assert scheduler.run(task1(1, 2, c=3)) == 1

    last_exec = session.query(Execution).one()
    [arg1, arg2, arg3, arg4] = sorted(
        last_exec.call_node.arguments, key=lambda arg: arg.arg_key or str(arg.arg_position)
    )

    # Positional arguments.
    assert arg1.call_hash == last_exec.call_node.call_hash
    assert arg1.arg_position == 0
    assert arg1.arg_key is None
    assert arg1.value_parsed == 1

    assert arg2.call_hash == last_exec.call_node.call_hash
    assert arg2.arg_position == 1
    assert arg2.arg_key is None
    assert arg2.value_parsed == 2

    # Keyword argument.
    assert arg3.call_hash == last_exec.call_node.call_hash
    assert arg3.arg_position is None
    assert arg3.arg_key == "c"
    assert arg3.value_parsed == 3

    # Default argument.
    assert arg4.call_hash == last_exec.call_node.call_hash
    assert arg4.arg_position is None
    assert arg4.arg_key == "d"
    assert arg4.value_parsed == 20


def test_multiple_expr(scheduler: Scheduler) -> None:
    """
    Multiple references of the same expression should only run the task once.
    """
    task_calls = []

    @task()
    def task1():
        task_calls.append("task1")
        return 1

    @task()
    def task2(y):
        return [y, task1()]

    @task()
    def main(x):
        return [task2(1), task2(2)]

    assert scheduler.run(main(1)) == [[1, 1], [2, 1]]
    assert task_calls == ["task1"]

    # Now let's test the cache.
    with mock.patch.object(scheduler, "_get_cache", wraps=scheduler._get_cache) as get_cache:
        assert scheduler.run(main(1)) == [[1, 1], [2, 1]]
        # Depending on timing, we'll get a CSE from pending or the backend.
        assert 4 <= get_cache.call_count <= 5

    # Get root call node.
    backend = scheduler.backend
    assert isinstance(backend, RedunBackendDb) and backend.session

    call_node = backend.session.query(CallNode).filter_by(task_name="main").one()

    # Assert diamond call graph.
    assert len(call_node.children) == 2
    assert (
        call_node.children[0].children[0].call_hash == call_node.children[1].children[0].call_hash
    )


def test_parent_job(scheduler: Scheduler, session: Session) -> None:
    """
    Parent job should be set for subtasks and arguments.

    By setting the parent job we can trace where an argument came from, i.e.
    the upstream CallNode.
    """

    @task()
    def task1():
        return 10

    @task()
    def task2(x):
        return x

    @task()
    def task3():
        return task2(task1())

    assert scheduler.run(task3()) == 10

    last_exec = session.query(Execution).one()
    assert last_exec.job.task.name == "task3"
    assert last_exec.job.child_jobs[0].task.name == "task1"
    assert last_exec.job.child_jobs[1].task.name == "task2"
    assert last_exec.job.child_jobs[1].call_node.arguments[0].upstream[0].task_name == "task1"


def test_parent_job_simple(scheduler: Scheduler, session: Session) -> None:
    """
    Parent job should be set for subtasks even through SimpleExpression.
    """

    @task()
    def task1():
        return {"a": 10}

    @task()
    def task2():
        return task1()["a"]

    assert scheduler.run(task2()) == 10

    last_exec = session.query(Execution).one()
    assert last_exec.job.task.name == "task2"
    assert last_exec.job.child_jobs[0].task.name == "task1"


def test_parent_job_simple_arg(scheduler: Scheduler, session: Session) -> None:
    """
    Parent job should be set for subtasks even through SimpleExpression as arguments.

    The upstream CallNode should also be recorded for such arguments.
    """

    @task()
    def task1():
        return {"a": 10}

    @task()
    def task2(x):
        return x

    @task()
    def task3():
        return task2(task1()["a"])

    assert scheduler.run(task3()) == 10

    last_exec = session.query(Execution).one()
    assert last_exec.job.task.name == "task3"
    assert last_exec.job.child_jobs[0].task.name == "task1"
    assert last_exec.job.child_jobs[1].task.name == "task2"
    assert last_exec.job.child_jobs[1].call_node.arguments[0].upstream[0].task_name == "task1"


def test_parent_job_multiple_simple_arg(scheduler: Scheduler, session: Session) -> None:
    """
    Parent job should be set for subtasks even through SimpleExpression as arguments.
    """

    @task()
    def task1():
        return {"a": 10}

    @task()
    def task2(x, y):
        return x

    @task()
    def task3():
        return task2(task1()["a"], task1()["a"])

    assert scheduler.run(task3()) == 10

    last_exec = session.query(Execution).one()
    assert last_exec.job.task.name == "task3"

    # Due to collapsing multiple equivalent expressions, there should only be
    # two sub jobs.
    assert len(last_exec.job.child_jobs) == 2
    assert last_exec.job.child_jobs[0].task.name == "task1"
    assert last_exec.job.child_jobs[1].task.name == "task2"

    # Both arguments should be recorded and both of their upstreams should be set.
    assert len(last_exec.job.child_jobs[1].call_node.arguments) == 2
    assert last_exec.job.child_jobs[1].call_node.arguments[0].upstream[0].task_name == "task1"
    assert last_exec.job.child_jobs[1].call_node.arguments[1].upstream[0].task_name == "task1"


def test_parent_job_simple_call(scheduler: Scheduler, session: Session) -> None:
    """
    Parent job should be set for subtask that were returned as result values.
    """

    @task()
    def task1():
        return 10

    @task()
    def task2():
        return task1

    @task()
    def task3():
        return task2()()

    assert scheduler.run(task3()) == 10

    last_exec = session.query(Execution).one()
    assert last_exec.job.task.name == "task3"
    assert last_exec.job.child_jobs[0].task.name == "task2"
    assert last_exec.job.child_jobs[1].task.name == "task1"


def test_task_value_recording(scheduler: Scheduler, session: Session) -> None:
    """
    For every Task recorded in the database, a corresponding Value record should exist. This
    should be true even for Tasks that aren't passed as arguments to other Tasks.
    """
    from redun.backends.db import Task as DBTask
    from redun.backends.db import Value as DBValue

    @task()
    def task1(a: int, b: int) -> int:
        return a + b

    scheduler.run(task1(1, 2))
    assert (
        session.query(DBValue).join(DBTask, DBTask.hash == DBValue.value_hash).one().value_hash
        == task1.hash
    )

    @task()
    def main(tsk: Callable[[int, int], int]):
        return [tsk(1, 10), tsk(1, 12)]

    scheduler.run(main(task1))

    assert session.query(DBValue).join(DBTask, DBTask.hash == DBValue.value_hash).count() == 2


@use_tempdir
def test_subvalues(scheduler: Scheduler, session: Session) -> None:
    """
    Subvalues should be recorded.
    """
    file1 = File("file1")
    file1.write("hello")

    file2 = File("file2")
    file2.write("goodbye")

    @task()
    def main(files: List[File]) -> Dict[File, str]:
        return {file: cast(str, file.read()) for file in files}

    assert scheduler.run(main([file1, file2])) == {file1: "hello", file2: "goodbye"}

    last_exec = session.query(Execution).one()

    # Subvalues (files) of the List[File] argument should be recorded.
    subvalues = last_exec.call_node.arguments[0].value.children
    paths = {subvalue.value_parsed.path for subvalue in subvalues}
    assert paths == {"file1", "file2"}

    # Subvalues (files) of the return value Dict[File, str] should be recorded.
    subvalues2 = last_exec.call_node.value.children
    paths2 = {subvalue.value_parsed.path for subvalue in subvalues2}
    assert paths2 == {"file1", "file2"}


@use_tempdir
def test_dir_subvalues(scheduler: Scheduler, session: Session) -> None:
    """
    Subvalues of a Dir should be recorded.
    """
    file1 = File("dir/file1")
    file1.write("hello")

    file2 = File("dir/file2")
    file2.write("goodbye")

    dir = Dir("dir")

    @task()
    def main(dir: Dir) -> Set[str]:
        return {cast(str, file.read()) for file in dir}

    assert scheduler.run(main(dir)) == {"hello", "goodbye"}

    last_exec = session.query(Execution).one()

    # Dir should be recorded as argument.
    arg_value = last_exec.call_node.arguments[0].value
    dir2 = arg_value.value_parsed
    assert isinstance(dir2, Dir)
    assert dir2.path == dir.path

    # Subvalues (files) of the Dir argument should be recorded.
    subvalues = arg_value.children
    paths = {subvalue.value_parsed.path for subvalue in subvalues}
    assert paths == {"dir/file1", "dir/file2"}


@use_tempdir
def test_nested_dir_subvalues(scheduler: Scheduler, session: Session) -> None:
    """
    Subvalues of a Dir nested inside another Value should be recorded.
    """
    file1 = File("dir1/file1")
    file1.write("hello")

    file2 = File("dir2/file2")
    file2.write("goodbye")

    file3 = File("dir2/file3")
    file3.write("world")

    dir1 = Dir("dir1")
    dir2 = Dir("dir2")

    @task()
    def main(dirs: List[Dir]) -> Set[str]:
        return {cast(str, file.read()) for dir in dirs for file in dir}

    assert scheduler.run(main([dir1, dir2])) == {"hello", "goodbye", "world"}

    last_exec = session.query(Execution).one()

    # List of Dirs should be recorded.
    arg_value = last_exec.call_node.arguments[0].value
    dir_list = arg_value.value_parsed
    assert isinstance(dir_list, list)
    assert [dir.path for dir in dir_list] == ["dir1", "dir2"]

    # All subvalues, Dirs and Files, should be recorded.
    paths = {child.value_parsed.path for child in arg_value.children}
    assert paths == {"dir1", "dir1/file1", "dir2", "dir2/file2", "dir2/file3"}


def test_execution_job(scheduler: Scheduler, session: Session) -> None:
    """
    We should be able to query an Execution's job tree.
    """

    @task()
    def task1(x):
        return x + 1

    @task()
    def main():
        return [task1(1), task1(2)]

    assert scheduler.run(main()) == [2, 3]

    # We should be able to query the job tree.
    last_exec = session.query(Execution).one()
    assert len(last_exec.jobs) == 3
    assert {job.task.name for job in last_exec.jobs} == {"task1", "main"}


def test_job_status(scheduler: Scheduler, session: Session) -> None:
    """
    We should be able to classify a Job's status.
    """

    @task()
    def task1(x):
        assert x != 0
        return x + 1

    @task()
    def main():
        return [task1(1), task1(0)]

    with pytest.raises(AssertionError):
        scheduler.run(main())

    with pytest.raises(AssertionError):
        scheduler.run(main())

    [exec1, exec2] = (
        session.query(Execution).join(Job, Job.id == Execution.job_id).order_by(Job.start_time)
    )

    assert Counter(job.status for job in exec1.jobs) == Counter({"FAILED": 2, "DONE": 1})
    assert Counter(job.status for job in exec2.jobs) == Counter({"FAILED": 2, "CACHED": 1})


def test_catch_downstream(scheduler: Scheduler) -> None:
    """
    Result from catch should properly propagate to downstream tasks.
    """

    @task()
    def task1(x):
        return x + 1

    @task()
    def recover(error):
        return error

    @task()
    def main():
        x = catch(task1(1), ValueError, recover)
        return task1(x)

    assert scheduler.run(main()) == 3


def test_cond_downstream(scheduler: Scheduler, session: Session) -> None:
    @task()
    def id(x):
        return x

    @task()
    def main(pred=True):
        x = cond(id(pred), id(2), id(3))
        return id(x)

    assert scheduler.run(main()) == 2


def test_catch_dataflow(scheduler: Scheduler, session: Session) -> None:
    """
    SchedulerTasks with partially evaluated args should have data provenance.
    """

    @task()
    def task1(x):
        if x:
            raise ValueError("BOOM")
        return x

    @task()
    def task2(x):
        return x

    @task()
    def recover(error):
        return "recover"

    @task()
    def main():
        x = catch(task1(True), ValueError, recover)
        return task2(x)

    @task()
    def main2():
        x = catch(task1(False), ValueError, recover)
        return task2(x)

    assert scheduler.run(main()) == "recover"
    assert scheduler.run(main2()) is False

    [exec1, exec2] = (
        session.query(Execution).join(Job, Job.id == Execution.job_id).order_by(Job.start_time)
    )

    # Assert dataflow for exec1.
    [task2_node, task1_node, recover_node] = exec1.call_node.children
    assert task2_node.task.name == "task2"
    assert task1_node.task.name == "task1"
    assert recover_node.task.name == "recover"
    assert task2_node.arguments[0].upstream == [recover_node]
    assert recover_node.arguments[0].upstream == [task1_node]

    # Assert dataflow for exec2.
    [task2_node, task1_node] = exec2.call_node.children
    assert task2_node.task.name == "task2"
    assert task1_node.task.name == "task1"
    assert task2_node.arguments[0].upstream == [task1_node]


def test_catch_reraise_dataflow(scheduler: Scheduler, session: Session) -> None:
    """
    catch with a reraise should have proper dataflow.
    """

    @task()
    def task1():
        raise ValueError("BOOM")

    @task()
    def recover(error):
        raise error

    @task()
    def main():
        return catch(task1(), ValueError, recover)

    with pytest.raises(ValueError):
        scheduler.run(main())

    exec1 = session.query(Execution).one()
    assert exec1.job.child_jobs[1].call_node.task.name == "recover"
    assert isinstance(exec1.job.child_jobs[1].call_node.value.value_parsed.error, ValueError)


def test_seq_dataflow(scheduler: Scheduler, session: Session) -> None:
    """
    seq() should record a proper dataflow.
    """

    @task()
    def inc(x: int) -> int:
        return x + 1

    @task()
    def main():
        return seq([inc(1), inc(2), inc(3)])

    assert scheduler.run(main()) == [2, 3, 4]

    exec1 = session.query(Execution).one()
    assert [job.call_node.value.value_parsed for job in exec1.job.child_jobs] == [2, 3, 4]


def test_cond_dataflow(scheduler: Scheduler, session: Session) -> None:
    @task()
    def pred(x):
        return x

    @task()
    def then(x):
        return x

    @task()
    def else_(x):
        return x

    @task()
    def follow_up(x):
        return x

    @task()
    def main(arg=True):
        x = cond(pred(arg), then(2), else_(3))
        return follow_up(x)

    assert scheduler.run(main(True)) == 2
    assert scheduler.run(main(False)) == 3

    [exec1, exec2] = (
        session.query(Execution).join(Job, Job.id == Execution.job_id).order_by(Job.start_time)
    )

    # Assert dataflow for exec1.
    [follow_up_node, pred_node, then_node] = exec1.call_node.children
    [follow_up_node, pred_node, then_node] = exec1.call_node.children
    assert follow_up_node.task.name == "follow_up"
    assert pred_node.task.name == "pred"
    assert then_node.task.name == "then"
    assert set(follow_up_node.arguments[0].upstream) == {pred_node, then_node}

    # Assert dataflow for exec2.
    [follow_up_node, pred_node, else_node] = exec2.call_node.children
    assert follow_up_node.task.name == "follow_up"
    assert pred_node.task.name == "pred"
    assert else_node.task.name == "else_"
    assert set(follow_up_node.arguments[0].upstream) == {pred_node, else_node}


def test_simple_expression_upstreams(scheduler: Scheduler, session: Session) -> None:
    """
    We should be able to record the upstream of a cached SimpleExpression.
    """

    @task()
    def get_data() -> dict:
        return {"key": 10}

    @task()
    def add(x: int, y: int) -> int:
        return x + y

    @task()
    def main(a: int) -> int:
        data = get_data()
        x = data["key"]
        z = add(x, a)
        return z

    assert scheduler.run(main(1)) == 11

    # Redefine the task.
    @task()  # type: ignore[no-redef]
    def get_data() -> dict:
        return {"key": -10}

    # Running the scheduler a second time will use the cached result for main(), which is
    #   TaskExpression('add',
    #     (SimpleExpression('getitem', (
    #         TaskExpression('get_data', (), {}),
    #         'key',
    #       ), {})),
    #     {}
    #   )
    #
    # The SimpleExpression will deserialized from cache and will need its upstreams
    # set to TaskExpression('get_data', (), {})
    assert scheduler.run(main(1)) == -9

    def get_args(call_node):
        return sorted(call_node.arguments, key=lambda arg: arg.arg_position)

    execs = session.query(Execution).all()
    assert get_args(execs[0].call_node.children[0])[0].upstream[0].task_name == "get_data"
    assert get_args(execs[1].call_node.children[0])[0].upstream[0].task_name == "get_data"


@use_tempdir
def test_db_automigrate() -> None:
    """
    Backend should respect automigration config option.
    """
    backend = RedunBackendDb(db_uri="sqlite:///redun.db")
    backend.load()
    assert backend.session

    min_compat_for_latest_version, _ = backend.get_db_version_required()

    # Ensure we are at current database version.
    version = backend.get_db_version()
    latest_version = backend.get_all_db_versions()[-1]
    assert version == latest_version

    prev_incompat_version = [
        v for v in backend.get_all_db_versions() if v < min_compat_for_latest_version
    ][-1]

    # Downgrade database to a version incompatible with the latest one.
    backend.migrate(prev_incompat_version)
    version = backend.get_db_version()
    assert version == prev_incompat_version

    # Load without migrating should leave version unchanged.
    with pytest.raises(RedunVersionError):
        backend.load(migrate=False)
    version = backend.get_db_version()
    assert version == prev_incompat_version

    # Load by default automigrates.
    backend.load()
    version = backend.get_db_version()
    assert version == latest_version


@use_tempdir
def test_db_automigrate_scheduler() -> None:
    """
    Scheduler should respect automigrate config variable.
    """
    scheduler = Scheduler(
        config=Config(
            {
                "backend": {
                    "db_uri": "sqlite:///redun.db",
                }
            }
        )
    )
    backend = cast(RedunBackendDb, scheduler.backend)
    assert backend.automigrate

    # Loading the database should automigrate by default.
    scheduler.load()

    # Ensure we are at current database version.
    version = backend.get_db_version()
    latest_version = backend.get_all_db_versions()[-1]
    assert version == latest_version

    # Downgrade database.
    prev_major_version = [v for v in backend.get_all_db_versions() if v < latest_version][-1]
    backend.migrate(prev_major_version)
    version = backend.get_db_version()
    assert version == prev_major_version

    # Get a new scheduler that does not do automigration.
    scheduler = Scheduler(
        config=Config({"backend": {"db_uri": "sqlite:///redun.db", "automigrate": "False"}})
    )
    backend = cast(RedunBackendDb, scheduler.backend)
    backend.create_engine()
    version = backend.get_db_version()
    assert version == prev_major_version


@use_tempdir
def test_automigrate_postgres() -> None:
    """
    Postgres databases should not automigrate.
    """
    File(".redun2/redun.ini").write(
        """
[backend]
db_uri = postgresql://host/db
"""
    )

    latest_version = REDUN_DB_VERSIONS[-1]

    scheduler = Scheduler(config=Config({"backend": {"db_uri": "postgresql://host/db"}}))
    with patch.object(RedunBackendDb, "migrate") as migrate_mock, patch.object(
        RedunBackendDb, "create_engine"
    ), patch.object(RedunBackendDb, "is_db_compatible", returns=True), patch.object(
        RedunBackendDb, "get_db_version", returns=latest_version
    ):
        scheduler.load()

    migrate_mock.assert_not_called()


def test_db_backend_clone() -> None:
    """
    Cloning should result in a new db backend that shares the same engine and task/execution
    records as the original, without sharing a database session
    """
    backend = RedunBackendDb(db_uri="sqlite:///:memory:")

    with pytest.raises(RedunDatabaseError, match="after.*load"):
        backend.clone()
    backend.load()
    clone = backend.clone()
    assert clone.engine is backend.engine
    assert clone.current_execution is backend.current_execution

    assert clone.session is not backend.session


def test_json_field(scheduler: Scheduler, session: Session) -> None:
    """
    Ensure we can properly store and filter a JSON db field.
    """

    # Add and filter a string value.
    session.add(
        Tag(
            tag_hash="111",
            entity_type=TagEntity.Value,
            entity_id="1",
            key="params",
            value="hello",
        )
    )
    session.commit()

    assert session.query(Tag).filter(Tag.value == sa_cast("hello", JSON)).one().value == "hello"
    assert not session.query(Tag).filter(Tag.value == sa_cast("bye", JSON)).all()

    # Add and filter an int value.
    session.add(
        Tag(tag_hash="222", entity_type=TagEntity.Value, entity_id="2", key="params", value=10)
    )
    session.commit()

    assert session.query(Tag).filter(Tag.value == sa_cast(10, JSON)).one().value == 10
    assert not session.query(Tag).filter(Tag.value == sa_cast(11, JSON)).all()

    # Add and filter a complex value.
    session.add(
        Tag(
            tag_hash="333",
            entity_type=TagEntity.Value,
            entity_id="2",
            key="params",
            value=[1, 2, 3],
        )
    )
    session.commit()

    assert session.query(Tag).filter(Tag.value == sa_cast([1, 2, 3], JSON)).one().value == [
        1,
        2,
        3,
    ]
    assert not session.query(Tag).filter(Tag.value == sa_cast([1, 2, 3, 4], JSON)).all()

    # Ensure dict normalization is used.
    session.add(
        Tag(
            tag_hash="444",
            entity_type=TagEntity.Value,
            entity_id="2",
            key="params",
            value={"a": 1, "b": 2},
        )
    )
    session.commit()

    assert session.query(Tag).filter(Tag.value == sa_cast({"b": 2, "a": 1}, JSON)).one().value == {
        "a": 1,
        "b": 2,
    }


class Data:
    def __init__(self, x: int):
        self.x = x


def test_value_no_class(scheduler: Scheduler, backend: RedunBackendDb, session: Session) -> None:
    """
    When loading a value without a importable class, use a preview.
    """

    # Store a value using a custom class, Data.
    value = Data(10)
    value_hash = backend.record_value(value)

    value_row = session.query(Value).filter(Value.value_hash == value_hash).one()

    # If data is not available, a PreviewValue should be returned.
    with patch.object(backend, "_get_value_data", lambda _: (None, False)):
        with pytest.warns(UserWarning, match="value_store_path"):
            value2 = value_row.preview
        assert isinstance(value2, PreviewValue)
        assert str(value2) == "redun.tests.test_db.Data(hash=2ad3f921, size=50)"

    # Remove custom class.
    original_Data = Data
    del globals()["Data"]

    # If class is not available, a PreviewClass should be returned.
    value3 = value_row.preview
    assert isinstance(value3, PreviewClass)
    assert str(value3) == "Data(x=10)"

    # Restore custom class.
    globals()["Data"] = original_Data
