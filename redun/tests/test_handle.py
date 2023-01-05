import pickle
from typing import List, Optional
from unittest import mock

import pytest

from redun import Handle, Scheduler, merge_handles, namespace, task
from redun.backends.db import Handle as HandleRow
from redun.backends.db import RedunBackendDb
from redun.utils import pickle_dumps
from redun.value import get_type_registry

redun_namespace = "redun.tests.test_handle"


class DBConn:
    """
    Mock database connection class.
    """

    def __init__(self, uri):
        self.uri = uri

    def execute(self, sql):
        return [1]


class DbHandle(Handle):
    def __init__(self, name: str, db_file: str, namespace: Optional[str] = None):
        self.db_file = db_file
        self.conn = DBConn(db_file)

    def execute(self, sql: str):
        return self.conn.execute(sql)


def get_handle(scheduler: Scheduler, conn: Handle) -> HandleRow:
    assert isinstance(scheduler.backend, RedunBackendDb)
    assert scheduler.backend.session
    return scheduler.backend.session.query(HandleRow).filter_by(hash=conn.__handle__.hash).one()


def test_handle_class() -> None:
    # Subclasses of Handle automatically register into registry.
    registry = get_type_registry()
    registry.parse_type_name("redun.tests.test_handle.DbHandle") == DbHandle

    # Handle subclass should have __handle_class__ info.
    assert DbHandle.type_name == "redun.tests.test_handle.DbHandle"


def test_handle_init() -> None:
    # Handle instances should have __handle__ info.
    conn = DbHandle("mydb", "data.db", namespace="")
    assert conn.__handle__.name == "mydb"
    assert conn.__handle__.namespace == ""
    assert conn.__handle__.args == ("data.db",)
    assert conn.__handle__.kwargs == {}

    # Handle instance should also have their own custom data.
    assert conn.db_file == "data.db"
    assert isinstance(conn.conn, DBConn)

    # Pretend to use connection.
    assert conn.execute("SELECT 1") == [1]

    # Handles should support serialization.
    state: dict = {
        "name": "mydb",
        "namespace": "",
        "args": ("data.db",),
        "kwargs": {},
        "class_name": "redun.tests.test_handle.DbHandle",
        "call_hash": "",
        "key": "",
        "hash": "07fc03e706cf914c352e2cf22d4947447e91d797",
    }
    assert conn.__getstate__() == state

    # Recreate the handle from serialized data.
    conn2 = DbHandle.__new__(DbHandle, state["name"], *state["args"], **state["kwargs"])
    conn2.__init__(state["name"], *state["args"], **state["kwargs"])  # type: ignore
    assert conn2.db_file == "data.db"
    assert isinstance(conn2.conn, DBConn)

    # Recreate the handle from serialized data like pickle does.
    conn3 = DbHandle.__new__(DbHandle)
    conn3.__setstate__(state)
    assert conn3.db_file == "data.db"
    assert isinstance(conn3.conn, DBConn)


def test_handle_info_get_hash() -> None:
    # Regression test to ensure that non-string handle args and kwargs are properly serialized when
    # getting hash
    handle_info = Handle.HandleInfo(
        name="foo", class_name="bar", args=(22, 23), kwargs={"not_a_string": True}
    )
    handle_info.get_hash()


def test_handle_pickle() -> None:
    """
    Handles should pickle correctly and recall the __init__ method.
    """
    conn = DbHandle("mydb", "data.db")
    data = pickle_dumps(conn)

    with mock.patch.object(DbHandle, "__init__") as init:
        init.return_value = None
        pickle.loads(data)
        init.assert_called_with("mydb", "data.db")


def test_handle_no_namespace(scheduler: Scheduler) -> None:
    @task()
    def task1():
        conn = DbHandle("conn", "data.db", namespace="")
        return (conn.__handle__.namespace, conn.__handle__.name, conn.__handle__.fullname)

    assert scheduler.run(task1()) == ("", "conn", "conn")


# TODO: bring back.
def _test_handle_task_namespace():
    @task(namespace="myspace")
    def task1():
        conn = DbHandle("conn", "data.db")
        return (conn.__handle__.namespace, conn.__handle__.name, conn.__handle__.fullname)

    scheduler = Scheduler()
    assert scheduler.run(task1()) == ("myspace", "conn", "myspace.conn")


# TODO: bring back.
def _test_handle_scope_namespace():
    namespace("myspace")

    @task()
    def task1():
        conn = DbHandle("conn", "data.db")
        return (conn.__handle__.namespace, conn.__handle__.name, conn.__handle__.fullname)

    namespace()

    scheduler = Scheduler()
    assert scheduler.run(task1()) == ("myspace", "conn", "myspace.conn")


def test_handle_apply() -> None:
    conn = DbHandle("mydb", "data.db")
    conn2 = conn.apply_call("call_hash123")
    assert conn2.__handle__.hash != conn.__handle__.hash

    assert conn2.db_file == conn.db_file
    assert conn2.conn == conn.conn


def test_handle_fork() -> None:
    conn = DbHandle("mydb", "data.db")
    conn2 = conn.fork("1")
    assert conn2.__handle__.fullname == conn.__handle__.fullname
    assert conn2.__handle__.class_name == conn.__handle__.class_name
    assert conn2.__handle__.hash != conn.__handle__.hash

    assert conn2.db_file == conn.db_file
    assert conn2.conn == conn.conn


def test_handle_fork_explicit_namespace() -> None:
    conn = DbHandle("mydb", "data.db", namespace="explicit_namespace")
    conn2 = conn.fork("1")
    assert conn2.__handle__.fullname == conn.__handle__.fullname
    assert conn2.__handle__.class_name == conn.__handle__.class_name
    assert conn2.__handle__.hash != conn.__handle__.hash

    assert conn2.db_file == conn.db_file
    assert conn2.conn == conn.conn


def test_handle_edges(scheduler: Scheduler) -> None:
    @task()
    def do_write(conn, data):
        conn.execute(data)
        return conn

    @task()
    def pipeline(conn):
        conn = do_write(conn, "a")
        conn = do_write(conn, "b")
        conn = do_write(conn, "c")
        return conn

    @task()
    def main():
        conn = DbHandle("conn", "postgresql://host/db")
        return pipeline(conn)

    scheduler.run(main())

    # We should be able to see both the pre and post processing handles.
    # 1 original + 3 do_writes * 2 + 1 pipeline
    assert isinstance(scheduler.backend, RedunBackendDb)
    assert scheduler.backend.session
    handles = scheduler.backend.session.query(HandleRow).all()
    assert len(handles) == 8


def test_handle_tasks_simple(scheduler: Scheduler) -> None:
    """
    Ensure a handle changes hash as it passes through tasks.
    """

    @task()
    def task1(conn):
        return conn

    # Pass handle through one task.
    conn1 = DbHandle("conn", "data.db")
    conn2 = scheduler.run(task1(conn1))

    # Handle hash should update.
    assert conn2.__handle__.hash != conn1.__handle__.hash

    # 3 handles should exist in HandleGraph (conn1, pre_conn1, conn2).
    assert isinstance(scheduler.backend, RedunBackendDb)
    assert scheduler.backend.session
    handles = scheduler.backend.session.query(HandleRow).all()
    assert len(handles) == 3

    # The should all be valid.
    assert all(handle.is_valid for handle in handles)

    # Let's update the task.
    @task()
    def task2(conn):
        return conn

    # Running the handle through the updated task should cause a branch in the HandleGraph.
    conn3 = scheduler.run(task2(conn1))
    handle3 = get_handle(scheduler, conn3)
    assert handle3.is_valid

    # conn2 should have rolled back.
    handle2 = get_handle(scheduler, conn2)
    assert not handle2.is_valid

    # conn3 should be a descendant of conn1.
    assert handle3.parents[0].parents[0].hash == conn1.__handle__.hash

    # Let's inspect the branch created in the HandleGraph.
    child_hashes = {h.hash: h for h in handle3.parents[0].children}
    assert conn3.__handle__.hash in child_hashes
    assert conn2.__handle__.hash in child_hashes

    # Rerunning task1 should revert the handle to its old state.
    conn2b = scheduler.run(task1(conn1))
    assert conn2b.__handle__.hash == conn2.__handle__.hash
    handle2 = get_handle(scheduler, conn2)
    handle2b = get_handle(scheduler, conn2b)
    assert handle2b.is_valid

    # conn3 should have rolled back.
    handle3 = get_handle(scheduler, conn3)
    assert not handle3.is_valid


def test_handle_tasks(scheduler: Scheduler) -> None:
    """
    Ensure a handle changes hash as it passes through tasks.
    """
    task_calls = []
    handles = {}

    @task()
    def init_db(conn: DbHandle):
        task_calls.append("init_db")
        handles["init_db"] = conn
        conn.execute("CREATE TABLE ...")
        return conn

    @task(version="v1")
    def update_db(conn: DbHandle):
        task_calls.append("update_db")
        handles["update_db"] = conn
        conn.execute("INSERT INTO ...")
        return conn

    @task()
    def workflow():
        task_calls.append("workflow")
        conn = DbHandle("conn", "data.db")
        handles["workflow"] = conn

        conn = init_db(conn)
        conn = update_db(conn)
        return conn

    final_conn = scheduler.run(workflow())

    # All the tasks should run in order.
    assert task_calls == ["workflow", "init_db", "update_db"]

    # We should get a changing handle hash.
    assert handles["workflow"].__handle__.hash != handles["init_db"].__handle__.hash
    assert handles["init_db"].__handle__.hash != handles["update_db"].__handle__.hash

    # Change the update_db task.
    @task(version="v2")  # type: ignore[no-redef]
    def update_db(conn: DbHandle):
        task_calls.append("update_db2")
        handles["update_db2"] = conn
        conn.execute("INSERT INTO ... something different;")
        return conn

    task_calls = []
    final_conn2 = scheduler.run(workflow())

    # Just the update_db task should run again.
    assert task_calls == ["update_db2"]

    # The previous final conn shouldn't be valid anymore, because
    # update_db2 branches off before it.
    final_handle = get_handle(scheduler, final_conn)
    final_handle2 = get_handle(scheduler, final_conn2)
    assert not final_handle.is_valid
    assert final_handle2.is_valid

    # The early handles used for init_db, should still be valid, since they
    # are ancestral to to final_handle2.
    assert scheduler.backend.is_valid_handle(handles["workflow"])
    assert scheduler.backend.is_valid_handle(handles["init_db"])
    assert scheduler.backend.is_valid_handle(handles["update_db"])
    assert scheduler.backend.is_valid_handle(handles["update_db2"])

    # The handle entering update_db should be the same hash.
    assert handles["update_db"].__handle__.hash == handles["update_db2"].__handle__.hash

    # Revert the update_db task.
    @task(version="v1")  # type: ignore[no-redef]
    def update_db(conn: DbHandle):
        task_calls.append("update_db")
        handles["update_db"] = conn
        conn.execute("INSERT INTO ...")
        return conn

    task_calls = []
    final_conn3 = scheduler.run(workflow())
    get_handle(scheduler, final_conn3)

    # Because we are using a handle, we should NOT have a fast revert (task_calls == []).
    # Instead update_db needs to run again.
    assert task_calls == ["update_db"]


def test_handle_rollback_forks(scheduler: Scheduler) -> None:
    """
    Ensure previous handles rollback.
    """
    task_calls = []
    handles = {}

    @task()
    def update_db(conn: DbHandle, data):
        task_calls.append(("update_db", data))
        handles["update_db", data] = conn
        conn.execute("INSERT INTO ...")
        return conn

    @task()
    def post_update_db(conn: DbHandle, data):
        handles["post_update_db", data] = conn
        return conn

    @task()
    def workflow():
        conn = DbHandle("conn", "data.db")
        handles["workflow"] = conn
        conn2 = post_update_db(update_db(conn, 10), 10)
        conn3 = post_update_db(update_db(conn, 20), 20)
        return merge_handles([conn2, conn3])

    scheduler.run(workflow())

    # Change the workflow.
    @task()  # type: ignore
    def workflow():
        conn = DbHandle("conn", "data.db")
        handles["workflow"] = conn
        conn2 = post_update_db(update_db(conn, 12), 12)
        conn3 = post_update_db(update_db(conn, 20), 20)
        return merge_handles([conn2, conn3])

    prev_handles = dict(handles)
    task_calls = []
    scheduler.run(workflow())

    # The first call to update_db(conn, 10) should have caused a rollback.
    assert not scheduler.backend.is_valid_handle(prev_handles["post_update_db", 10])
    assert scheduler.backend.is_valid_handle(handles["post_update_db", 12])

    # The second call, update_db(conn, 20), should be the same handle and valid.
    assert (
        prev_handles["post_update_db", 20].__handle__.hash
        == handles["post_update_db", 20].__handle__.hash
    )
    assert scheduler.backend.is_valid_handle(prev_handles["post_update_db", 20])


def test_handle_nested(scheduler: Scheduler) -> None:
    @task()
    def task1(conn1, conn2):
        return [conn1, conn2]

    conn1 = DbHandle("conn1", "data1.db")
    conn2 = DbHandle("conn2", "data2.db")

    conn1b, conn2b = scheduler.run(task1(conn1, conn2))

    # Handles in a nested structure should also advance their hash after task call.
    assert conn1.__handle__.hash != conn1b.__handle__.hash
    assert conn2.__handle__.hash != conn2b.__handle__.hash


def test_handle_merge(scheduler: Scheduler) -> None:
    """
    Test that we can merge two handles into one.
    """

    @task()
    def task1(conn):
        return conn

    @task()
    def task2(conn):
        return conn

    @task()
    def workflow():
        conn = DbHandle("conn", "data.db")
        conns = [
            task1(conn),
            task2(conn),
        ]
        return merge_handles(conns)

    conn = scheduler.run(workflow())

    # Assert that we have a handle with two parents in the handle graph.
    assert isinstance(scheduler.backend, RedunBackendDb)
    assert scheduler.backend.session
    handle = (
        scheduler.backend.session.query(HandleRow)
        .filter(HandleRow.hash == conn.__handle__.hash)
        .first()
    )
    assert len(handle.parents) == 2


def test_handle_merge3(scheduler: Scheduler) -> None:
    """
    Test that we can merge three handles into one.
    """

    @task()
    def task1(conn, arg):
        return conn

    @task()
    def workflow():
        conn = DbHandle("conn", "data.db")
        conns = [
            task1(conn, "a"),
            task1(conn, "b"),
            task1(conn, "c"),
        ]
        return merge_handles(conns)

    conn = scheduler.run(workflow())

    # Assert that we have a handle with two parents in the handle graph.
    assert isinstance(scheduler.backend, RedunBackendDb)
    assert scheduler.backend.session
    handle = (
        scheduler.backend.session.query(HandleRow)
        .filter(HandleRow.hash == conn.__handle__.hash)
        .first()
    )
    assert len(handle.parents) == 3


def test_handle_fork_many(scheduler: Scheduler) -> None:
    """
    Test that we can use named forking to allow narrow re-execution.
    """

    @task()
    def task1(conn, arg):
        task_calls.append(arg)
        return conn

    @task()
    def workflow():
        conn = DbHandle("conn", "data.db")
        conns = [
            task1(conn.fork("a"), "a"),
            task1(conn.fork("b"), "b"),
            task1(conn.fork("c"), "c"),
        ]
        return merge_handles(conns)

    task_calls: List[str] = []
    scheduler.run(workflow())
    assert set(task_calls) == {"a", "b", "c"}

    # Redefine workflow to change only order of parallel tasks.
    # We should not need to re-execute any of them.
    @task()  # type: ignore
    def workflow():
        conn = DbHandle("conn", "data.db")
        conns = [
            task1(conn.fork("b"), "b"),
            task1(conn.fork("c"), "c"),
            task1(conn.fork("a"), "a"),
        ]
        return merge_handles(conns)

    task_calls = []
    scheduler.run(workflow())
    assert task_calls == []


def test_handle_implicit_fork(scheduler: Scheduler) -> None:
    """
    Test that our heuristic for reconciling handles with implicit forks works.
    """

    @task()
    def task1(conn, arg):
        task_calls.append(arg)
        return conn

    @task(version="1")
    def task2(conn, arg):
        task_calls.append(arg)
        return conn

    @task()
    def workflow():
        conn = DbHandle("conn", "data.db")
        conns = [
            task1(conn, "a"),
            task2(conn, "b"),
            task1(conn, "c"),
        ]
        return merge_handles(conns)

    task_calls: List[str] = []
    scheduler.run(workflow())
    assert set(task_calls) == {"a", "b", "c"}

    # Executing again should be fully cached.
    task_calls = []
    scheduler.run(workflow())
    assert task_calls == []

    # Redfine workflow to just have more tasks at the end.
    # Just one more task should be executed.
    @task()  # type: ignore
    def workflow():
        conn = DbHandle("conn", "data.db")
        conns = [
            task1(conn, "a"),
            task2(conn, "b"),
            task1(conn, "c"),
            task1(conn, "d"),
        ]
        return merge_handles(conns)

    task_calls = []
    scheduler.run(workflow())
    assert set(task_calls) == {"d"}

    # Without explicit forks our heuristic (call order) for Handle
    # reconciliation isn't good enough. Let's just confirm that.
    @task(version="2")  # type: ignore[no-redef]
    def task2(conn, arg):
        task_calls.append(arg)
        return conn

    @task()  # type: ignore
    def workflow():
        conn = DbHandle("conn", "data.db")
        conns = [
            task2(conn, "b"),  # Updated task2.
            task1(conn, "c"),
            task1(conn, "a"),
        ]
        return merge_handles(conns)

    task_calls = []
    scheduler.run(workflow())
    assert set(task_calls) == {"a", "b", "c"}

    # Executing again should be fully cached.
    task_calls = []
    scheduler.run(workflow())
    assert task_calls == []


def test_handle_serial(scheduler: Scheduler) -> None:
    """
    Test that we can extend a serial pipeline with proper caching.
    """

    @task()
    def task1(conn, arg):
        task_calls.append(arg)
        return conn

    @task(version="1")
    def task2(conn, arg):
        task_calls.append(arg)
        return conn

    @task()
    def workflow():
        conn = DbHandle("conn", "data.db")
        conn = task1(conn, "a")
        conn = task2(conn, "b")
        conn = task1(conn, "c")
        return conn

    task_calls: List[str] = []
    scheduler.run(workflow())
    assert set(task_calls) == {"a", "b", "c"}

    # Executing again should be fully cached.
    task_calls = []
    scheduler.run(workflow())
    assert task_calls == []

    # Redfine workflow to just have more tasks at the end.
    # Just one more task should be executed.
    @task()  # type: ignore
    def workflow():
        conn = DbHandle("conn", "data.db")
        conn = task1(conn, "a")
        conn = task2(conn, "b")
        conn = task1(conn, "c")
        conn = task1(conn, "d")
        return conn

    task_calls = []
    scheduler.run(workflow())
    assert set(task_calls) == {"d"}


def test_handle_proxy() -> None:
    """
    Ensure that attributes access proxies to Handle.instance.
    """

    class DbHandle2(Handle):
        def __init__(self, name, db_file):
            self.instance = DBConn(db_file)

    conn = DbHandle2("mydb", "data.db")

    assert conn.uri == "data.db"
    assert conn.execute("select * from my_table;") == [1]
    assert conn.__handle__.name == "mydb"
    assert dict(conn.__getstate__(), hash="") == {
        "name": "mydb",
        "namespace": "redun.tests.test_handle",
        "args": ("data.db",),
        "kwargs": {},
        "class_name": "redun.tests.test_handle.DbHandle2",
        "call_hash": "",
        "key": "",
        "hash": "",
    }


def test_handle_proxy_attr_error() -> None:
    """
    Ensure that attributes don't proxy when Handle.instance doesn't exist.
    """

    class DbHandle2(Handle):
        def __init__(self, name, db_file):
            self.conn = DBConn(db_file)

    conn = DbHandle2("mydb", "data.db")

    assert hasattr(conn, "conn")
    assert not hasattr(conn, "uri")
    with pytest.raises(AttributeError):
        conn.uri


def test_multiple_forks(scheduler: Scheduler) -> None:
    """
    Ensure multiple handle forks are recorded.
    """

    @task()
    def task1(conn):
        return conn

    @task()
    def main():
        conn = DbHandle("conn", "data.db")
        conn = task1(conn)
        conn = task1(conn.fork("a").fork("b").fork("c"))
        return conn

    scheduler.run(main())

    # We should record both forks.
    assert isinstance(scheduler.backend, RedunBackendDb)
    assert scheduler.backend.session
    handles = scheduler.backend.session.query(HandleRow).all()
    assert len(handles) == 8
