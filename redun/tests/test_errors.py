import pickle

import pytest
import sqlalchemy as sa
from sqlalchemy.orm import Session

from redun import Scheduler, task
from redun.backends.db import Execution, Job
from redun.scheduler import ErrorValue
from redun.utils import pickle_dumps
from redun.value import get_type_registry


def test_serializing_lambdas(scheduler):
    """
    redun does not support serializing lambdas.
    """
    # Should raise error.
    # with pytest.raises(Exception):
    #    result = scheduler.run(lambda x: 10)

    # Try to pass lambda as an argument.
    @task()
    def task1(x):
        return x

    with pytest.raises(Exception):
        scheduler.run(task1(lambda: 10))

    # Try to return lambda as a result.
    @task()
    def task2(x):
        return lambda: 10

    with pytest.raises(Exception):
        scheduler.run(task2(10))


def func1(x):
    return x


def is_naked_function(value):
    """
    Return true if value is a function but not a Task.
    """
    return type(value).__name__ == "function"


def test_detecting_func():
    """
    Ensure we can distinguish naked functions from tasks.
    """

    @task()
    def task1(x):
        return x

    assert callable(func1)
    assert is_naked_function(func1)

    assert callable(task1)
    assert not is_naked_function(task1)


def test_serializing_functions(scheduler):
    """
    redun does not support serializing functions.

    Instead users should pass and return Tasks.
    """
    # Functions defined globally can be serialized by pickle.
    # The function bytecode not serialized, just a name to the func.
    data = pickle_dumps(func1)
    assert data == b"\x80\x03credun.tests.test_errors\nfunc1\nq\x00."

    # If we change the function name to 'bad_func', pickle will raise an
    # error when deserializing.
    data2 = b"\x80\x03credun.tests.test_errors\nbad_func\nq\x00."
    with pytest.raises(AttributeError):
        pickle.loads(data2)

    # Try to pass a function as an argument.
    @task()
    def task1(x):
        return x

    # with pytest.raises(Exception):
    #    scheduler.run(task1(func1))


def test_exceptions(scheduler: Scheduler) -> None:
    """
    Ensure exceptions in tasks are handled correctly.
    """

    @task()
    def faulty():
        raise ValueError("BOOM")

    @task()
    def test1(x):
        return x

    @task()
    def test2():
        return faulty()

    # Task that raises an exception should be raised from run().
    with pytest.raises(ValueError):
        scheduler.run(faulty())

    # Error raised in Task as argument should be raised from run().
    with pytest.raises(ValueError):
        scheduler.run(test1(faulty()))

    # Error raised in Task as result should be raised from run().
    with pytest.raises(ValueError):
        scheduler.run(test2())


def test_exception_call_graph(scheduler: Scheduler, session: Session) -> None:
    """
    Exceptions should be recorded in call graph.
    """

    @task(version="1")
    def faulty():
        raise ValueError("BOOM")

    @task()
    def inc(x):
        return x + 1

    @task()
    def task1():
        return [inc(10), faulty()]

    # Workflow should raise exception.
    with pytest.raises(ValueError):
        scheduler.run(task1())

    # CallGraph should have recorded exception as ValueError.
    execution = (
        session.query(Execution)
        .join(Job, Job.id == Execution.job_id)
        .order_by(sa.desc(Job.start_time))
    ).first()
    assert execution.job.call_node
    assert isinstance(execution.job.call_node.value.value_parsed.error, ValueError)

    # Both successful and failed jobs should be recorded.
    assert execution.job.child_jobs[0].call_node.value.value_parsed == 11
    assert execution.job.child_jobs[1].call_node.value.value_parsed.error.args == ("BOOM",)

    # Exceptions should not be cached. Let's fix the bug without bump version.

    @task(version="1")  # type: ignore[no-redef]
    def faulty():
        return 20

    assert scheduler.run(task1.options(check_valid="shallow")()) == [11, 20]


def test_error_value() -> None:
    """
    ErrorValue should implement the Value interface.
    """

    @task(version="1")
    def faulty():
        raise ValueError("BOOM")

    @task()
    def test1(x):
        return x

    @task()
    def test2():
        return faulty()

    error = ErrorValue(ValueError("BOOM"))
    assert error.type_name == "redun.ErrorValue"
    assert error.get_hash() == "4e7d45fe277f8638271781336996591c1cf6b7bb"

    registry = get_type_registry()
    bytes = registry.serialize(error)
    error2 = registry.deserialize("redun.ErrorValue", bytes)

    assert isinstance(error2.error, ValueError)
    assert error2.error.args == ("BOOM",)


def test_error_value_fallback(scheduler: Scheduler, session: Session) -> None:
    """
    redun should fallback to Exception for errors that cannot be serialized.
    """

    @task()
    def main():
        # Lambdas are not pickleable.
        raise ValueError(lambda: "unpicklable")

    # Workflow should raise exception.
    with pytest.raises(ValueError):
        scheduler.run(main())

    # CallGraph should have recorded exception as ValueError.
    execution = session.query(Execution).one()
    error = execution.job.call_node.value.value_parsed.error
    assert type(error) == Exception
    assert "ValueError" in str(error)
