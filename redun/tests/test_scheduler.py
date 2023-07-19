import copy
import dataclasses
import os
import time
from traceback import FrameSummary
from typing import Any, Dict, List, Optional, Sequence, Tuple
from unittest.mock import Mock, patch

import pytest
from sqlalchemy.orm import Session

from redun import Scheduler, task
from redun.backends.db import Execution
from redun.backends.db import Job as JobDb
from redun.backends.db import RedunBackendDb
from redun.backends.db import Task as TaskDb
from redun.config import Config
from redun.expression import SchedulerExpression
from redun.promise import Promise
from redun.scheduler import (
    DryRunResult,
    Frame,
    Job,
    SchedulerError,
    Task,
    Traceback,
    catch,
    catch_all,
    cond,
    federated_task,
    scheduler_task,
)
from redun.scheduler_config import postprocess_config
from redun.task import CacheScope, PartialTask, SchedulerTask, hash_args_eval
from redun.tests.utils import assert_match_lines, use_tempdir
from redun.utils import map_nested_value
from redun.value import Value, get_type_registry


@dataclasses.dataclass
class Node:
    value: str
    next_: Optional["Node"]


@task(namespace="redun_test")
def module_task(x, y=3):
    return x + y


def test_simple(scheduler: Scheduler) -> None:
    """
    A single task should execute.
    """

    @task()
    def workflow():
        return "ok"

    assert scheduler.run(workflow()) == "ok"


def test_call(scheduler: Scheduler) -> None:
    """
    A subtask call should evaluate.
    """

    @task()
    def task1(x):
        return "hello {}".format(x)

    @task()
    def workflow():
        result = task1("world")
        return result

    assert scheduler.run(workflow()) == "hello world"


def test_task_args(scheduler: Scheduler) -> None:
    """
    A task result can be used as an argument to another task.
    """

    @task()
    def task1():
        return "world"

    @task()
    def task2(x):
        return "hello {}".format(x)

    @task()
    def workflow():
        result = task1()
        result2 = task2(result)
        return result2

    assert scheduler.run(workflow()) == "hello world"


def test_call_deep(scheduler: Scheduler) -> None:
    @task()
    def task1(a):
        return "task1({})".format(a)

    @task()
    def combine(*values):
        return ",".join(values)

    @task()
    def task2(a_list):
        results = [task1(a) for a in a_list]
        return combine(*results)

    @task()
    def workflow():
        values = ["alice", "bob", "claire"]
        result = task2(values)
        return result

    assert scheduler.run(workflow()) == "task1(alice),task1(bob),task1(claire)"


@use_tempdir
def test_scheduler_backend() -> None:
    """
    Backend should set its db_uri correctly.
    """
    # Default scheduler should us in memory backend db.
    scheduler = Scheduler()
    assert isinstance(scheduler.backend, RedunBackendDb)
    assert scheduler.backend.db_uri == "sqlite:///:memory:"

    # If config specifies a db_uri, we should use it.
    os.makedirs(".redun")
    scheduler = Scheduler(config=Config({"backend": {"db_uri": "sqlite:///.redun/redun.db"}}))
    scheduler.load()
    assert isinstance(scheduler.backend, RedunBackendDb)
    assert scheduler.backend.db_uri == "sqlite:///.redun/redun.db"
    assert os.path.exists(".redun/redun.db")


def test_scheduler_incremental(scheduler: Scheduler) -> None:
    """
    Scheduler should incrementally execute tasks that change their hash.
    """
    task_calls = []

    @task()
    def run_bwa(reads):
        task_calls.append("run_bwa")
        align = "align({})".format(reads)
        return align

    @task()
    def run_gatk(align):
        task_calls.append("run_gatk")
        calls = "calls({})".format(align)
        return calls

    @task()
    def workflow():
        task_calls.append("workflow")
        reads = "my reads"
        align = run_bwa(reads)
        calls = run_gatk(align)
        return calls

    # Scheduler should call all the tasks and return the correct result.
    result = scheduler.run(workflow())

    assert result == "calls(align(my reads))"
    assert task_calls == ["workflow", "run_bwa", "run_gatk"]

    # Running the workflow again should not trigger any more task calls.
    task_calls = []
    result = scheduler.run(workflow())
    assert result == "calls(align(my reads))"
    assert task_calls == []

    # Simulate a code change to run_gatk task.

    @task()  # type: ignore[no-redef]
    def run_gatk(align):
        task_calls.append("run_gatk")
        calls = "calls2({})".format(align)
        return calls

    task_calls = []
    result = scheduler.run(workflow())

    assert result == "calls2(align(my reads))"
    assert task_calls == ["run_gatk"]

    # A reverted code change should be fully memoized too.

    @task()  # type: ignore[no-redef]
    def run_gatk(align):
        task_calls.append("run_gatk")
        calls = "calls({})".format(align)
        return calls

    task_calls = []
    result = scheduler.run(workflow())

    assert result == "calls(align(my reads))"
    assert task_calls == []


def test_scheduler_multiple_calls(scheduler: Scheduler) -> None:
    """
    We need to memoize the same function with different arguments.
    """

    @task()
    def task1(arg):
        return "hello {}".format(arg)

    @task()
    def task2():
        return [task1("alice"), task1("bob")]

    # Scheduler should call all the tasks and return the correct result.
    assert scheduler.run(task2()) == ["hello alice", "hello bob"]


def test_scheduler_recursive(scheduler: Scheduler) -> None:
    """
    Scheduler should handle recursive task calls.
    """

    task_calls = []

    @task()
    def add(a, b):
        return a + b

    @task()
    def fib(n):
        task_calls.append(n)
        if n <= 0:
            return 0
        elif n <= 2:
            return 1
        else:
            return add(fib(n - 1), fib(n - 2))

    # Scheduler should call all the tasks and return the correct result.
    assert scheduler.run(fib(1)) == 1
    assert scheduler.run(fib(2)) == 1
    assert scheduler.run(fib(3)) == 2
    assert scheduler.run(fib(5)) == 5
    assert len(task_calls) == 5


def test_destructure(scheduler: Scheduler):
    """
    Workflows should be able to use Expression destructuring.
    """

    @task()
    def task1():
        return {
            "output1": 10,
            "output2": 20,
        }

    @task()
    def task2(x):
        return x + 1

    @task()
    def workflow():
        outputs = task1()
        a = task2(outputs["output1"])
        b = task2(outputs["output2"])
        return [a, b]

    assert scheduler.run(workflow()) == [11, 21]


def test_return_tuple_type(scheduler: Scheduler) -> None:
    """
    Tasks with return type Tuple should support destructuring.
    """

    @task()
    def make_pair() -> Tuple[str, int]:
        return ("Bob", 20)

    @task()
    def make_message(name: str, age: int) -> str:
        return "{} is {}".format(name, age)

    @task()
    def main() -> str:
        name, age = make_pair()
        return make_message(name, age)

    @task()
    def main2() -> str:
        pair = make_pair()
        return make_message(pair[0], pair[1])

    assert scheduler.run(main()) == "Bob is 20"
    assert scheduler.run(main2()) == "Bob is 20"


def test_nout(scheduler: Scheduler) -> None:
    """
    Tasks with nout should support iteration.
    """

    @task(nout=3)
    def make_list() -> List[int]:
        return [1, 2, 3]

    @task()
    def process(x):
        return x + 1

    @task()
    def main() -> List[int]:
        # make_list() supports iteration because of nout.
        return [process(x) for x in make_list()]

    assert scheduler.run(main()) == [2, 3, 4]


def test_nout_undefined(scheduler: Scheduler) -> None:
    """
    Tasks without nout should not support iteration.
    """

    @task()
    def make_list() -> List[int]:
        return [1, 2, 3]

    with pytest.raises(TypeError):
        for x in make_list():
            pass


def test_nout_bad(scheduler: Scheduler) -> None:
    """
    Task nout must be non-negative int.
    """
    with pytest.raises(TypeError):

        @task(nout="3")
        def make_list() -> List[int]:
            return [1, 2, 3]

    with pytest.raises(TypeError):

        @task(nout=-1)
        def make_list2() -> List[int]:
            return [1, 2, 3]


def test_nout_options(scheduler: Scheduler) -> None:
    """
    Tasks should be able to specify nout using options().
    """

    @task()
    def make_list() -> List[int]:
        return [1, 2, 3]

    @task()
    def process(x):
        return x + 1

    @task()
    def main() -> List[int]:
        # make_list() supports iteration because of nout.
        return [process(x) for x in make_list.options(nout=3)()]

    assert scheduler.run(main()) == [2, 3, 4]


def test_nest_args(scheduler: Scheduler) -> None:
    """
    Tasks should allow nested arguments.
    """

    @task()
    def task1():
        return {
            "output1": 10,
            "output2": 20,
        }

    @task()
    def task2(x):
        return x + 1

    @task()
    def sum_task(xs):
        return sum(xs)

    @task()
    def workflow():
        outputs = task1()
        a = task2(outputs["output1"])
        b = task2(outputs["output2"])
        return sum_task([a, b])

    assert scheduler.run(workflow()) == 32


def test_nested_results(scheduler: Scheduler) -> None:
    """
    Tasks should allow nested results.
    """

    @task()
    def task1():
        return 10

    @task()
    def task2():
        return [task1()]

    result = scheduler.run(task2())
    assert result == [10]


def test_nested_results2(scheduler: Scheduler) -> None:
    @task()
    def to_str(x: int) -> str:
        return str(x)

    def create_linked_list(y: List[int]) -> Optional[Node]:
        if len(y) > 1:
            return Node(value=to_str(y[0]), next_=create_linked_list(y[1:]))
        else:
            return Node(to_str(y[0]), next_=None) if y else None

    assert scheduler.run(create_linked_list([1])) == Node("1", None)
    assert scheduler.run(create_linked_list([1, 2])) == Node("1", Node("2", None))


def test_higher_order(scheduler: Scheduler) -> None:
    """
    Using task as an argument to another task.
    """
    task_calls = []

    @task()
    def get_offset():
        task_calls.append("get_offset")
        return 10

    @task()
    def adder_helper(values):
        return sum(values)

    @task()
    def adder(a, b):
        task_calls.append("adder")
        return adder_helper([a, b, get_offset()])

    @task()
    def executor(my_task, a, b):
        task_calls.append("executor")
        return my_task(a, b)

    @task()
    def workflow():
        task_calls.append("workflow")
        return executor(adder, 2, 3)

    # Ensure that workflow runs correctly.
    assert scheduler.run(workflow()) == 15
    assert task_calls == ["workflow", "executor", "adder", "get_offset"]

    # Ensure cache is reused even when argument is a task.
    task_calls = []
    assert scheduler.run(workflow()) == 15
    assert task_calls == []

    # Changing a child of adder(), changes adder(). Caching should detect this.
    @task()  # type: ignore[no-redef]
    def get_offset():
        task_calls.append("get_offset")
        return 20

    task_calls = []
    assert scheduler.run(workflow()) == 25
    assert task_calls == ["get_offset"]


def test_higher_order2(scheduler: Scheduler) -> None:
    """
    Using task as an output of another task.
    """
    task_calls = []

    @task()
    def helper(values):
        return sum(values)

    @task()
    def helper2(values):
        result = 1
        for value in values:
            result *= value
        return result

    @task()
    def get_offset():
        task_calls.append("get_offset")
        return 10

    @task()
    def adder(a, b):
        task_calls.append("adder")
        return helper([a, b, get_offset()])

    @task()
    def multiplier(a, b):
        task_calls.append("multiplier")
        return helper2([a, b, get_offset()])

    @task()
    def chooser(key):
        task_calls.append("chooser")
        if key == "adder":
            return adder
        elif key == "multiplier":
            return multiplier
        else:
            raise NotImplementedError(key)

    @task()
    def executor(my_task_key, a, b):
        task_calls.append("executor")
        my_task = chooser(my_task_key)
        return my_task(a, b)

    @task()
    def workflow():
        task_calls.append("workflow")
        return helper([executor("adder", 2, 3), executor("multiplier", 2, 3)])

    # Ensure that workflow runs correctly.
    task_calls = []
    assert scheduler.run(workflow()) == 75
    assert sorted(task_calls) == sorted(
        [
            "workflow",
            "executor",
            "chooser",
            "adder",
            "get_offset",
            "executor",
            "chooser",
            "multiplier",
        ]
    )

    # Ensure cache is reused even when argument is a task.
    task_calls = []
    assert scheduler.run(workflow()) == 75
    assert task_calls == []

    # Change definition of task that was cached.
    @task()  # type: ignore[no-redef]
    def adder(a, b):
        task_calls.append("adder")
        return a + b

    # We detect that the cached task is not valid to use, so adder is called again.
    # The rest is still cached.
    task_calls = []
    assert scheduler.run(workflow()) == 65
    assert task_calls == ["chooser", "adder"]

    # Changing a child of multiplier, changes multiplier. Caching should detect this.
    @task()  # type: ignore[no-redef]
    def get_offset():
        task_calls.append("get_offset")
        return 20

    task_calls = []
    assert scheduler.run(workflow()) == 125
    assert task_calls == ["get_offset"]


def test_default_args(scheduler: Scheduler) -> None:
    """
    Changes in default arguments should be considered by caching.
    """
    default = 10

    @task()
    def add(a, b=default):
        return a + b

    assert scheduler.run(add(1, 2)) == 3
    assert scheduler.run(add(1)) == 11

    # Now let's change the default args, without changing the task.
    default = 20

    @task()  # type: ignore
    def add(a, b=default):
        return a + b

    # We should use the new default args.
    assert scheduler.run(add(1)) == 21


def test_novel_kwargs(scheduler: Scheduler) -> None:
    """
    Tasks should be able to accept novel kwargs.
    """

    @task()
    def task1(**kwargs: Any) -> Dict[str, Any]:
        return kwargs

    assert scheduler.run(task1(a=1, b=2)) == {"a": 1, "b": 2}


def test_no_cache_task(scheduler: Scheduler) -> None:
    """
    Tasks should be able to disable caching.
    """
    task_calls = []

    # Try out the legacy option and all three
    @task(cache_scope=CacheScope.NONE)
    def task1():
        task_calls.append("task1")
        return 10

    task_calls = []
    scheduler.run(task1())
    assert task_calls == ["task1"]

    # Running the task again will execute again because we have disabled caching.
    scheduler.run(task1())
    assert task_calls == ["task1", "task1"]


def test_no_cache(scheduler: Scheduler) -> None:
    """
    Scheduler should be able to disable caching.
    """
    task_calls = []

    @task()
    def task1():
        task_calls.append("task1")
        return 10

    scheduler.run(task1())
    assert task_calls == ["task1"]

    # Running the task again will execute again because we have disabled caching.
    scheduler.run(task1(), cache=False)
    assert task_calls == ["task1", "task1"]


def test_cse_scopes(scheduler: Scheduler) -> None:
    """
    Scheduler should trigger CSE against both pending and completed jobs, depending upon the
    requested scope.
    """
    task_calls = []

    @task
    def noop(any: Any) -> None:
        # Use this no-op to ensure all the expressions are different.
        return

    @task(cache_scope=CacheScope.CSE)
    def task1(any):
        task_calls.append("task1")
        return 10

    @task(cache_scope=CacheScope.NONE)
    def task2():
        return task1(noop(3))

    @task(cache_scope=CacheScope.NONE)
    def task3():
        return task1.options(cache_scope=CacheScope.NONE)(noop(4))

    @task(cache_scope=CacheScope.CSE)
    def task_fail(any):
        task_calls.append("task_fail")
        raise RuntimeError("Fail")

    @task(cache_scope=CacheScope.NONE)
    def task_wrapped_fail():
        return task_fail(noop(2))

    # Spy on the call to `collapse`, as a way to distinguish between a cache hit from the
    # backend and from a pending job.
    with patch.object(Job, "collapse", side_effect=Job.collapse, autospec=True) as collapse_mock:
        # The second call will trigger CSE against the first job while it is still pending
        task_calls = []
        scheduler.run([task1(noop(1)), task1(noop(2))])
        assert collapse_mock.call_count == 1
        assert task_calls == ["task1"]

        # Verify there is an entry in the backend, so that our later tests demonstrate that
        # it's not being retrieved.
        task_calls = []
        scheduler.run([task1.options(cache_scope=CacheScope.BACKEND)])
        assert task_calls == []

        # Running the task again will execute once more, because we can't use the one from the
        # backend.
        collapse_mock.reset_mock()
        task_calls = []
        scheduler.run([task1(noop(1)), task1(noop(2))])
        assert collapse_mock.call_count == 1
        assert task_calls == ["task1"]

        # Ensure that CSE from pending jobs honors the cache setting
        collapse_mock.reset_mock()
        task_calls = []
        scheduler.run([task1(noop(1)), task1.options(cache_scope=CacheScope.NONE)(noop(2))])
        assert collapse_mock.call_count == 0
        assert task_calls == ["task1", "task1"]

        # The second call is delayed, so it will come from the backend, not the pending cache.
        collapse_mock.reset_mock()
        task_calls = []
        scheduler.run([task1(noop(1)), task2()])
        assert collapse_mock.call_count == 0
        assert task_calls == ["task1"]

        # Ensure that CSE from the backend also honors the cache setting
        collapse_mock.reset_mock()
        task_calls = []
        scheduler.run([task1(noop(1)), task3()])
        assert collapse_mock.call_count == 0
        assert task_calls == ["task1", "task1"]

        # Combine the two styles
        collapse_mock.reset_mock()
        task_calls = []
        scheduler.run([task1(noop(1)), task1(noop(2)), task2()])
        assert collapse_mock.call_count == 1
        assert task_calls == ["task1"]

        # Check that errors are replayed from pending CSE
        collapse_mock.reset_mock()
        task_calls = []
        with pytest.raises(RuntimeError, match="Fail"):
            scheduler.run([task_fail(noop(1)), task_fail(noop(2))])
        assert collapse_mock.call_count == 1
        assert task_calls == ["task_fail"]

        # Or from remote CSE.
        collapse_mock.reset_mock()
        task_calls = []
        with pytest.raises(RuntimeError, match="Fail"):
            scheduler.run([task_fail(noop(1)), task_wrapped_fail()])
        assert collapse_mock.call_count == 0
        assert task_calls == ["task_fail"]


class InvalidValue(Value):
    """A value that is never valid, hence can't be cached."""

    def is_valid(self) -> bool:
        return False

    def __setstate__(self, state):
        pass

    def __getstate__(self):
        return {}


def test_cse_no_validity(scheduler: Scheduler) -> None:
    """
    Validity of values is not checked upon CSE.
    """
    task_calls = []

    @task
    def noop():
        # Ensure we don't get any expression cycles.
        return None

    @task()
    def task1(any=None):
        task_calls.append("task1")
        return InvalidValue()

    @task(cache_scope=CacheScope.NONE)
    def task2():
        return task1()

    # Seed the cache
    scheduler.run([task1()])
    assert task_calls == ["task1"]

    # We can't get a hit, because it's not valid.
    task_calls = []
    scheduler.run([task1()])
    assert task_calls == ["task1"]

    # But if we get a CSE hit, we don't check validity, either from a pending job
    task_calls = []
    scheduler.run([task1(), task1(any=noop())])
    assert task_calls == ["task1"]

    # or from the backend.
    task_calls = []
    scheduler.run([task1(), task2()])
    assert task_calls == ["task1"]


def test_bad_executor(scheduler: Scheduler, session: Session) -> None:
    """
    Scheduler should fail gracefully with an undefined executor.
    """

    @task(executor="unknown")
    def task1():
        return 10

    with pytest.raises(SchedulerError):
        scheduler.run(task1())

    # Job and task should still be recorded.
    assert session.query(JobDb).one()
    assert session.query(TaskDb).filter(TaskDb.hash == task1.hash).one()


def test_job_options() -> None:
    """
    The task options for a job should follow the precedence of:
    1. Expression level.
    2. Task level.
    """

    @task(option1="aaa", option2="bbb")
    def task1():
        return 10

    expr = task1.options(option2="ccc")()
    job = Job(task1, expr)
    job.task = task1
    assert job.get_options() == {
        "option1": "aaa",
        "option2": "ccc",
    }


def test_job_status() -> None:
    """
    Job status should update as job progresses.
    """

    @task()
    def task1():
        return 10

    job = Job(task1, task1())
    assert job.status == "PENDING"

    job.eval_args = ((), {})
    assert job.status == "RUNNING"

    job.resolve(10)
    assert job.status == "DONE"

    job = Job(task1, task1())
    job.eval_args = ((), {})
    job.reject(ValueError())
    assert job.status == "FAILED"


def test_log_job_status(scheduler: Scheduler) -> None:
    """
    Scheduler should display a Job status table.
    """

    @task()
    def task1(x):
        return x + 1

    @task()
    def main():
        return [task1(1), task1(2)]

    assert scheduler.run(main()) == [2, 3]

    logs = []

    def log(*messages, **args):
        logs.extend(messages)

    with patch.object(scheduler, "log", wraps=log):
        report = scheduler.get_job_status_report()
        scheduler.log_job_statuses()

    assert report == logs
    assert logs[1:] == [
        "| TASK    PENDING RUNNING  FAILED  CACHED    DONE   TOTAL",
        "| ",
        "| ALL           0       0       0       0       3       3",
        "| main          0       0       0       0       1       1",
        "| task1         0       0       0       0       2       2",
        "",
    ]


def test_dryrun(scheduler: Scheduler) -> None:
    """
    No jobs should be submitted to executors during dryrun.
    """

    @task()
    def task1():
        return 10

    def boom(*args):
        raise AssertionError()

    # No jobs should be submitted to executor.
    with patch("redun.executors.local.LocalExecutor.submit") as submit_mock:
        submit_mock.side_effect = boom

        with pytest.raises(DryRunResult):
            scheduler.run(task1(), dryrun=True)
            assert not submit_mock.called

    # Run workflow.
    assert scheduler.run(task1()) == 10

    with patch("redun.executors.local.LocalExecutor.submit") as submit_mock:
        submit_mock.side_effect = boom

        # Workflow will now be fully cached.
        assert scheduler.run(task1()) == 10
        assert not submit_mock.called


def test_dryrun_explain(scheduler: Scheduler) -> None:
    """
    Dryrun should log explanation for cache miss.
    """

    @task(version="1")
    def main(x):
        return x

    # Run pipeline.
    assert scheduler.run(main(10)) == 10

    # Update arguments.
    with patch.object(scheduler.logger, "log") as log:
        with pytest.raises(DryRunResult):
            scheduler.run(main(11), dryrun=True)

        assert any(
            "Existing task 'main()' is called with new arguments" in call[0][1]
            for call in log.call_args_list
        )

    # Update task.
    @task(version="2")  # type: ignore
    def main(x):
        return x

    with patch.object(scheduler.logger, "log") as log:
        with pytest.raises(DryRunResult):
            scheduler.run(main(10), dryrun=True)

        assert any(
            "New task 'main()' with previous arguments" in call[0][1]
            for call in log.call_args_list
        )

    # Update arguments and task.
    with patch.object(scheduler.logger, "log") as log:
        with pytest.raises(DryRunResult):
            scheduler.run(main(11), dryrun=True)

        assert any(
            "New task 'main()' is called with new arguments" in call[0][1]
            for call in log.call_args_list
        )


def test_log_error_stack(scheduler: Scheduler) -> None:
    """
    Scheduler should log the task stack when raising an exception.
    """

    def func():
        raise ValueError("Boom")

    @task()
    def task1(a, b, c=3):
        return func()

    @task()
    def main(x):
        return task1(x, x + 1)

    scheduler.logger = Mock()

    with pytest.raises(ValueError):
        scheduler.run(main(10))

    logs = "\n".join(call[1][1] for call in scheduler.logger.method_calls)
    traceback_logs = logs.split("\n")[-14:]

    expected_logs = [
        r"\*\*\* Execution failed. Traceback \(most recent task last\):",
        r'  Job .*: File ".*", line \d+, in main',
        r"    def main\(x\):",
        r"    x = 10",
        r'  Job .*: File ".*", line \d+, in task1',
        r"    def task1\(a, b, c=3\):",
        r"    a = 10",
        r"    b = 11",
        r"    c = 3",
        r'  File ".*", line \d+, in task1',
        r"    return func\(\)",
        r'  File ".*", line \d+, in func',
        r'    raise ValueError\("Boom"\)',
        r"ValueError: Boom",
    ]
    assert_match_lines(expected_logs, traceback_logs)


def test_traceback_trim_frames() -> None:
    """
    Traceback should be able to trim frames related to redun scheduler.
    """
    basedir = os.path.dirname(os.path.dirname(__file__))

    @task()
    def task1():
        return 10

    frames = Traceback.trim_frames(
        [
            FrameSummary(basedir + "/__init__.py", 5, "func0"),
            FrameSummary(basedir + "/executors/local.py", 5, "func0"),
            Frame("myfile.py", 10, "func", {}, job=Job(task1, task1())),
            Frame("myfile2.py", 20, "func2", {}, job=Job(task1, task1())),
        ]
    )
    assert [frame.filename for frame in frames] == ["myfile.py", "myfile2.py"]

    frames = Traceback.trim_frames(
        [
            FrameSummary(basedir + "/cli.py", 5, "func0"),
            Frame("myfile.py", 10, "func", {}, job=Job(task1, task1())),
            Frame("myfile2.py", 20, "func2", {}, job=Job(task1, task1())),
        ]
    )
    assert [frame.filename for frame in frames] == ["myfile.py", "myfile2.py"]


def test_traceback_serialize() -> None:
    """
    Traceback should be able to trim frames related to redun scheduler.
    """
    basedir = os.path.dirname(os.path.dirname(__file__))

    @task()
    def task1():
        return 10

    traceback = Traceback(
        error=ValueError("boom"),
        frames=[
            FrameSummary(basedir + "/__init__.py", 5, "func0"),
            FrameSummary(basedir + "/executors/local.py", 5, "func0"),
            Frame("myfile.py", 10, "func", {}, job=Job(task1, task1())),
            Frame("myfile2.py", 20, "func2", {}, job=Job(task1, task1())),
        ],
        logs=[
            "line1",
            "line2",
            "line3",
        ],
    )

    # Traceback and Frame should have a type_name.
    assert Frame.type_name == "redun.Frame"
    assert Traceback.type_name == "redun.Traceback"

    # Traceback and Frame should serialize and deserialize.
    registry = get_type_registry()
    data = registry.serialize(traceback)
    traceback2 = registry.deserialize("redun.Traceback", data)
    data2 = registry.serialize(traceback2)

    assert data == data2


def test_check_valid(scheduler: Scheduler, session: Session) -> None:
    """
    check_valid=shallow should skip evaluating subtrees of the call graph.
    """
    calls = []

    @task()
    def task1():
        calls.append("task1")
        return 10

    @task()
    def main():
        return task1()

    with patch.object(Scheduler, "_exec_job", wraps=scheduler._exec_job) as exec_job_mock:
        assert scheduler.run(main.options(check_valid="full")()) == 10
        assert calls == ["task1"]

        task_execs = [call[0][0].task.name for call in exec_job_mock.call_args_list]
        assert task_execs == ["main", "task1"]

    with patch.object(Scheduler, "_exec_job", wraps=scheduler._exec_job) as exec_job_mock:
        assert scheduler.run(main.options(check_valid="full")()) == 10
        # task1 is not executed again, due to caching.
        assert calls == ["task1"]

        # But we do recurse to task1 to assess its cache value.
        task_execs = [call[0][0].task.name for call in exec_job_mock.call_args_list]
        assert task_execs == ["main", "task1"]

    with patch.object(Scheduler, "_exec_job", wraps=scheduler._exec_job) as exec_job_mock:
        # Now use check_valid="shallow".
        assert scheduler.run(main.options(check_valid="shallow")()) == 10
        assert calls == ["task1"]

        # We short circuit the cache checking at the top-level main task.
        task_execs = [call[0][0].task.name for call in exec_job_mock.call_args_list]
        assert task_execs == ["main"]

    # We should have recorded the same CallNode for each execution.
    from redun.backends.db import Execution

    executions = session.query(Execution).all()
    assert len({execution.job.call_hash for execution in executions}) == 1


def test_scheduler_task(scheduler: Scheduler) -> None:
    """
    scheduler_task decorator should allow custom evaluation.
    """

    @scheduler_task("task1", "redun")
    def task1(
        scheduler: Scheduler, parent_job: Job, sexpr: SchedulerExpression, x: int
    ) -> Promise:
        return scheduler.evaluate(x, parent_job=parent_job).then(lambda x2: x2 + 1)

    expr = task1(1)
    assert isinstance(task1, SchedulerTask)
    assert isinstance(expr, SchedulerExpression)
    assert expr.task_name == "redun.task1"
    assert expr.args == (1,)

    assert scheduler.run(task1(1)) == 2
    assert scheduler.run(task1(task1(1))) == 3

    # SchedulerTasks should be first-class values.
    assert isinstance(task1, Value)

    # SchedulerTasks should support partial application.
    expr2 = task1.partial()
    assert isinstance(expr2, PartialTask)
    assert isinstance(expr2(1), SchedulerExpression)
    assert scheduler.run(task1.partial()(1)) == 2


def test_cond(scheduler: Scheduler) -> None:
    """
    cond() should act like lasy if-statement.
    """

    @task()
    def id(x):
        return x

    @task()
    def boom():
        raise ValueError()

    assert scheduler.run(cond(id(True), id(1), id(2))) == 1
    assert scheduler.run(cond(id(False), id(1), id(2))) == 2

    # boom should not execute at all.
    assert scheduler.run(cond(id(False), boom(), id(2))) == 2

    # if, elif, else
    assert scheduler.run(cond(id(False), id(1), id(True), id(2), id(True), id(3))) == 2


def test_reduce(scheduler: Scheduler) -> None:
    @scheduler_task(namespace="redun.tests.test_scheduler")
    def reduce_(
        scheduler: Scheduler,
        parent_job: Job,
        sexpr: SchedulerExpression,
        a_task: Task,
        init: Any,
        values: Sequence[Any],
    ) -> Promise:
        def _reduce(args: List):
            a_task, init, values = args
            reduce_promise: Promise = Promise()
            queue = values if values else [init]
            remaining = len(values) - 1

            def fail(error):
                reduce_promise.do_reject(error)

            def then(value):
                nonlocal remaining
                remaining -= 1

                if remaining == 0:
                    reduce_promise.do_resolve(value)
                    return value

                queue.append(value)

                while len(queue) >= 2:
                    value1 = queue.pop()
                    value2 = queue.pop()
                    scheduler.evaluate(a_task(value1, value2), parent_job=parent_job).then(
                        then, fail
                    )

            if len(queue) == 1:
                # Base case.
                reduce_promise.do_resolve(queue[0])
            else:
                value1 = queue.pop()
                value2 = queue.pop()
                scheduler.evaluate(a_task(value1, value2), parent_job=parent_job).then(then, fail)

            return reduce_promise

        return scheduler.evaluate([a_task, init, values]).then(_reduce)

    @task()
    def add(a, b):
        return a + b

    assert scheduler.run(reduce_(add, 0, [])) == 0
    assert scheduler.run(reduce_(add, 0, [1])) == 1
    assert scheduler.run(reduce_(add, 0, [1, 2])) == 3
    assert scheduler.run(reduce_(add, 0, [1, 2, 3])) == 6
    assert scheduler.run(reduce_(add, 0, list(range(20)))) == 190


def test_catch(scheduler: Scheduler) -> None:
    """
    Catch expression should handle exceptions.
    """

    @task()
    def faulty():
        return 1 / 0

    @task()
    def recover(error):
        return 1.0

    @task()
    def task1():
        return catch(faulty(), ZeroDivisionError, recover)

    @task()
    def task2():
        return faulty()

    @task()
    def task3():
        # Catch should also allow multiple error classes.
        return catch(faulty(), (ZeroDivisionError, ValueError), recover)

    # Single task should be able to raise an exception.
    with pytest.raises(ZeroDivisionError):
        scheduler.run(faulty())

    # A nested task should be able to raise an exception.
    with pytest.raises(ZeroDivisionError):
        scheduler.run(task2())

    # A catch expression should catch the exception.
    assert scheduler.run(task1()) == 1.0
    assert scheduler.run(task3()) == 1.0


def test_multi_catch(scheduler: Scheduler) -> None:
    """
    Catch expression should handle multiple exception classes.
    """

    @task()
    def faulty():
        return 1 / 0

    @task()
    def faulty2():
        return {"a": 1}["b"]

    @task()
    def recover1(error):
        return "hello"

    @task()
    def recover2(error):
        return 1.0

    @task()
    def task1():
        return catch(
            faulty(),
            KeyError,
            recover1,
            ZeroDivisionError,
            recover2,
        )

    assert scheduler.run(task1()) == 1.0

    @task()
    def task2():
        return catch(
            faulty2(),
            KeyError,
            recover1,
            ZeroDivisionError,
            recover2,
        )

    assert scheduler.run(task2()) == "hello"


def test_catch_reraise(scheduler: Scheduler) -> None:
    """
    Catch expression can reraise a different exception.
    """

    @task()
    def faulty():
        return 1 / 0

    @task()
    def recover(error):
        raise ValueError("Reraised exception {}.".format(error))

    @task()
    def task1():
        return catch(faulty(), ZeroDivisionError, recover)

    with pytest.raises(ValueError):
        scheduler.run(task1())


def test_catch_cache(scheduler: Scheduler) -> None:
    """
    Catch expression should cache if successful.
    """

    @task()
    def faulty():
        calls.append("faulty")
        return 1 / 0

    @task()
    def recover(error):
        return 1.0

    @task()
    def reraise(error):
        raise error

    calls: List[str] = []
    with pytest.raises(ZeroDivisionError):
        scheduler.run(faulty())

    with pytest.raises(ZeroDivisionError):
        scheduler.run(faulty())

    # Failed tasks should not cache.
    assert calls == ["faulty", "faulty"]

    # Failed catch should not cache.
    calls = []
    assert scheduler.run(catch(faulty(), ZeroDivisionError, recover)) == 1.0
    assert calls == ["faulty"]

    # Previously recovered catch should cache.
    assert scheduler.run(catch(faulty(), ZeroDivisionError, recover)) == 1.0
    assert calls == ["faulty"]

    # Caching should not be used if turned off at the scheduler level
    assert scheduler.run(catch(faulty(), ZeroDivisionError, recover), cache=False) == 1.0
    assert calls == ["faulty", "faulty"]

    # Caching should not be used if turned off at the task level or the scheduler level.
    assert (
        scheduler.run(
            catch(faulty.options(cache_scope=CacheScope.NONE)(), ZeroDivisionError, recover),
        )
        == 1.0
    )
    assert calls == ["faulty", "faulty", "faulty"]
    assert scheduler.run(catch(faulty(), ZeroDivisionError, recover), cache=False) == 1.0
    assert calls == ["faulty", "faulty", "faulty", "faulty"]

    # Reraised catch should not cache.
    calls = []
    with pytest.raises(ZeroDivisionError):
        scheduler.run(catch(faulty(), ZeroDivisionError, reraise))
    assert calls == ["faulty"]

    with pytest.raises(ZeroDivisionError):
        scheduler.run(catch(faulty(), ZeroDivisionError, reraise))
    assert calls == ["faulty", "faulty"]


def test_catch_task_react(scheduler: Scheduler) -> None:
    """
    Catch should react to new task hashes.
    """

    @task()
    def safe():
        calls.append("safe")
        return 1 / 1

    @task()
    def faulty():
        calls.append("faulty")
        return 1 / 0

    @task()
    def recover(error):
        calls.append("recover")
        return 1.0

    # Running safe twice should cache.
    calls: List[str] = []
    assert scheduler.run(catch(safe(), ZeroDivisionError, recover)) == 1
    assert calls == ["safe"]
    assert scheduler.run(catch(safe(), ZeroDivisionError, recover)) == 1
    assert calls == ["safe"]

    # Updating safe task should force re-execution.
    @task()  # type: ignore[no-redef]
    def safe():
        calls.append("safe2")
        return 2 / 1

    assert scheduler.run(catch(safe(), ZeroDivisionError, recover)) == 2
    assert calls == ["safe", "safe2"]

    # Running recover twice should cache.
    calls = []
    assert scheduler.run(catch(faulty(), ZeroDivisionError, recover)) == 1.0
    assert calls == ["faulty", "recover"]
    assert scheduler.run(catch(faulty(), ZeroDivisionError, recover)) == 1.0
    assert calls == ["faulty", "recover"]

    # Updating recover task should force re-execution.
    @task()  # type: ignore[no-redef]
    def recover(error):
        calls.append("recover2")
        return 2.0

    assert scheduler.run(catch(faulty(), ZeroDivisionError, recover)) == 2.0
    assert calls == ["faulty", "recover", "faulty", "recover2"]


def test_catch_deep_task_react(scheduler: Scheduler) -> None:
    """
    Catch should react to new task hashes deep in the workflow.
    """

    @task()
    def deep(denom):
        calls.append("deep")
        return 1 / denom

    @task()
    def safe():
        calls.append("safe")
        return deep(1)

    @task()
    def faulty():
        calls.append("faulty")
        return 1 / 0

    @task()
    def recover(error):
        calls.append("recover")
        return 1.0

    # Running safe twice should cache.
    calls: List[str] = []
    assert scheduler.run(catch(safe(), ZeroDivisionError, recover)) == 1
    assert calls == ["safe", "deep"]
    assert scheduler.run(catch(safe(), ZeroDivisionError, recover)) == 1
    assert calls == ["safe", "deep"]

    # Updating deep task should force re-execution.
    @task()  # type: ignore[no-redef]
    def deep(denom):
        calls.append("deep2")
        return 2 / denom

    assert scheduler.run(catch(safe(), ZeroDivisionError, recover)) == 2
    assert calls == ["safe", "deep", "deep2"]


def test_catch_deep_recover_react(scheduler: Scheduler) -> None:
    """
    Catch should react to new task hashes deep in the recover workflow.
    """

    @task()
    def deep():
        calls.append("deep")
        return 1

    @task()
    def safe():
        calls.append("safe")
        return 1 / 1

    @task()
    def faulty():
        calls.append("faulty")
        return 1 / 0

    @task()
    def recover(error):
        calls.append("recover")
        return deep()

    # Running recover twice should cache.
    calls: List[str] = []
    assert scheduler.run(catch(faulty(), ZeroDivisionError, recover)) == 1
    assert calls == ["faulty", "recover", "deep"]
    assert scheduler.run(catch(faulty(), ZeroDivisionError, recover)) == 1
    assert calls == ["faulty", "recover", "deep"]

    # Updating deep task should force re-execution.
    @task()  # type: ignore[no-redef]
    def deep():
        calls.append("deep2")
        return 2

    assert scheduler.run(catch(faulty(), ZeroDivisionError, recover)) == 2
    assert calls == ["faulty", "recover", "deep", "deep2"]


def test_catch_all(scheduler: Scheduler) -> None:
    """
    Catch expression should handle exceptions.
    """

    @task
    def throw(error: Exception) -> None:
        raise error

    @task
    def good(x=10):
        return x

    @task
    def bad(message="boom"):
        raise ValueError(message)

    @task
    def recover(results):
        return map_nested_value(
            lambda result: (0 if isinstance(result, Exception) else result), results
        )

    # All subtasks succeed.
    assert scheduler.run(catch_all([good(1), good(2), good(3)], ValueError, recover)) == [1, 2, 3]

    # Failed tasks should be caught.
    assert scheduler.run(catch_all([good(4), bad(), good(5)], ValueError, recover)) == [4, 0, 5]
    assert scheduler.run(catch_all([good(6), bad(), bad(), good(7)], ValueError, recover)) == [
        6,
        0,
        0,
        7,
    ]

    # We should be able to catch multiple error classes.
    assert scheduler.run(
        catch_all(
            [throw(KeyError()), throw(ValueError()), good()], (KeyError, ValueError), recover
        )
    ) == [0, 0, 10]

    # Without a recovery, errors should reraise.
    with pytest.raises(ValueError):
        scheduler.run(catch_all([good(), bad(), good()]))

    # Different error_class should pass through.
    with pytest.raises(ValueError):
        scheduler.run(catch_all([good(), bad(), good()], ZeroDivisionError, recover))

    # Different error_class should pass through, even if others are caught.
    with pytest.raises(ValueError):
        scheduler.run(
            catch_all([throw(KeyError()), throw(ValueError()), good()], KeyError, recover)
        )

    # Any kind of nested value should work.
    assert scheduler.run(catch_all([good(8), [bad(), {"a": good(9)}]], ValueError, recover)) == [
        8,
        [0, {"a": 9}],
    ]


def test_config_args(scheduler: Scheduler) -> None:
    """
    Config_args should not contribute to the eval_hash.
    """

    @task(config_args=["x", "z"])
    def task1(x: int, y: int, z: str = "hello") -> int:
        return x

    eval_hash1, args_hash1 = hash_args_eval(scheduler.type_registry, task1, (10, 11), {})
    # Changing 'x' and 'z' does not change args_hash.
    eval_hash2, args_hash2 = hash_args_eval(scheduler.type_registry, task1, (11, 11), {"z": "bye"})
    assert eval_hash1 == eval_hash2
    assert args_hash1 == args_hash2


def test_variadic_args(scheduler: Scheduler) -> None:
    """
    Variadic arguments should contribute to the eval_hash.
    """

    @task
    def task1(x: int, y: int, *rest: int) -> int:
        return x

    eval_hash1, args_hash1 = hash_args_eval(scheduler.type_registry, task1, (10, 11, 12), {})
    eval_hash2, args_hash2 = hash_args_eval(
        scheduler.type_registry, task1, (10, 11, 12, 13, 14), {}
    )
    assert eval_hash1 != eval_hash2
    assert args_hash1 != args_hash2


def test_default_root(scheduler: Scheduler, session: Session) -> None:
    """
    We should always have one root job for an Execution.
    """

    @task()
    def task1(x: int) -> int:
        return x

    assert scheduler.run(task1(1) + task1(2)) == 3

    exec1 = session.query(Execution).one()

    # We should have the default root task.
    assert exec1.job.task.fullname == "redun.root_task"

    # The default root should have two child jobs.
    assert len(exec1.job.child_jobs) == 2


def test_no_default_root(scheduler: Scheduler, session: Session) -> None:
    """
    We should not use a default root task if top-level expression is a TaskExpression.
    """

    @task()
    def task1(x: int) -> int:
        return x

    assert scheduler.run(task1(1)) == 1

    exec1 = session.query(Execution).one()

    # We should have the default root task.
    assert exec1.job.task.name == "task1"


def test_default_args_expression(scheduler: Scheduler) -> None:
    """
    Task default arguments should support expressions.
    """

    @task
    def add(a: int, b: int) -> int:
        return a + b

    @task
    def main(x: int = add(1, 2)) -> int:
        return x

    assert scheduler.run(main()) == 3
    assert scheduler.run(main(4)) == 4

    @task
    def main2(x: List[int] = [add(1, 2)]) -> List[int]:
        # Default argument might contain an expression within a nested value (e.g. list).
        return x

    assert scheduler.run(main2()) == [3]


def test_job_clear(scheduler: Scheduler) -> None:
    """
    Job clean up should occur correctly even when child jobs fail.

    https://insitro.atlassian.net/browse/DE-4645
    """

    @task
    def child(x: int) -> int:
        if x == 2:
            raise ValueError("raised by child")
        return x

    @task
    def child2(x: int) -> int:
        return x

    @task
    def parent(n: int) -> List[int]:
        return [child2(child(i)) for i in range(n)]

    @task
    def main(n: int) -> List[int]:
        return parent(n)

    with pytest.raises(ValueError, match="raised by child"):
        scheduler.run(main(10))


def test_common_expression() -> None:
    """
    Scheduler should use Common Expression Elimination.
    """

    @task
    def id(x: int) -> int:
        return x

    @task
    def add(a: int, b: int) -> int:
        return a + b

    @task
    def boom(x: int) -> int:
        time.sleep(2)  # Ensure there's enough time for the pending job hit.
        raise ValueError("BOOM!")

    @task()
    def noop(any):
        """No-op used to ensure expressions are different."""
        return 1

    @task
    def wrapped_boom():
        return boom(noop(2))

    # There should be two jobs, although one will be cached
    scheduler = Scheduler()
    assert isinstance(scheduler.backend, RedunBackendDb)
    assert scheduler.backend.session
    assert scheduler.run([add(1, 2), add(1, 2)]) == [3, 3]
    assert (
        scheduler.backend.session.query(JobDb)
        .filter(JobDb.task_hash == add.hash, JobDb.cached == True)  # noqa: E712
        .count()
        == 0
    )
    assert (
        scheduler.backend.session.query(JobDb)
        .filter(JobDb.task_hash == add.hash, JobDb.cached == False)  # noqa: E712
        .count()
        == 1
    )

    # We should do CSE after argument expressions are evaluated, such as `id(2)`.
    scheduler = Scheduler()
    assert isinstance(scheduler.backend, RedunBackendDb)
    assert scheduler.backend.session

    # Cache id(2) so that we can force `add(1, id(2))` and `add(1, 2)` at the same time
    # and exercise the CSE logic on two different expressions.
    assert scheduler.run(id(2)) == 2
    assert scheduler.run([add(1, id(2)), add(1, 2)]) == [3, 3]
    assert (
        scheduler.backend.session.query(JobDb)
        .filter(JobDb.task_hash == add.hash, JobDb.cached == True)  # noqa: E712
        .count()
        == 1
    )
    assert (
        scheduler.backend.session.query(JobDb)
        .filter(JobDb.task_hash == add.hash, JobDb.cached == False)  # noqa: E712
        .count()
        == 1
    )

    # Expressions that have been collapsed by CSE should also propagate errors.
    scheduler = Scheduler()
    assert isinstance(scheduler.backend, RedunBackendDb)
    assert scheduler.backend.session
    with pytest.raises(ValueError, match="BOOM"):
        # This combination produces a pending CSE hit
        scheduler.run([boom(noop(1)), boom(noop(2))])
    assert (
        scheduler.backend.session.query(JobDb)
        .filter(JobDb.task_hash == boom.hash, JobDb.cached == True)  # noqa: E712
        .count()
        == 1
    )
    assert (
        scheduler.backend.session.query(JobDb)
        .filter(JobDb.task_hash == boom.hash, JobDb.cached == False)  # noqa: E712
        .count()
        == 1
    )


def test_expressions_evaluate_once() -> None:
    """
    Scheduler should not evaluate expression objects more than once.
    """
    calls = []

    @scheduler_task(namespace="redun.tests.test_scheduler")
    def echo(scheduler, parent_job, expr, x) -> Promise:
        calls.append("id")
        promise: Promise[int] = Promise()
        promise.do_resolve(x)
        return promise

    # We should call echo only once, even though scheduler tasks aren't cached.
    scheduler = Scheduler()
    x = echo(1)
    y = x + x
    assert scheduler.run(y) == 2
    assert calls == ["id"]

    # Expression cycle checking will find these as being the same
    calls = []
    y = echo(1) + echo(1)
    assert scheduler.run(y) == 2
    assert calls == ["id"]


def test_expressions_evaluate_once_cache(scheduler: Scheduler, session: Session) -> None:
    """
    Detecting repeated expression objects should work with cached expressions.
    """

    task1_value = 0

    @task(namespace="redun.tests.test_scheduler", cache_scope="NONE")
    def task1():
        nonlocal task1_value
        task1_value += 10
        return task1_value

    @task
    def add(x, y):
        return x + y

    calls = 0

    @task
    def main():
        nonlocal calls
        calls += 1

        x = task1()
        # Reuse the expression object.
        return {
            "a": add(x, 1),
            "b": add(x, 2),
            "c": x,
            "d": task1(),  # This is not a reuse, and should invoke another call.
        }

    # a-c all use the first call, and d gets the second one.
    result = scheduler.run(main())
    assert result["a"] == 11
    assert result["b"] == 12
    assert result["c"] == 10
    assert result["d"] == 10
    assert calls == 1
    assert task1_value == 10

    # If we run it again, `main` will get cached, returning an expression. Within that expression,
    # "a"-"c" share the expression for the nested call to `task1`, hence those get deduplicated.
    # However, "d" is a new expression and is not deduplicated.
    calls = 0
    result2 = scheduler.run(main())
    assert calls == 0  # Check that main was cached
    assert result2["a"] == 21
    assert result2["b"] == 22
    assert result2["c"] == 20
    assert result2["d"] == 20
    assert task1_value == 20


def test_extend_run(scheduler: Scheduler, session: Session) -> None:
    """
    Scheduler should support extending an Execution.
    """

    @task
    def main(x):
        return x + 1

    @task
    def fail():
        raise ValueError("BOOM")

    assert scheduler.run(main(10)) == 11
    job = session.query(JobDb).one()

    result = scheduler.extend_run(main(11), parent_job_id=job.id)
    assert result["result"] == 12

    # Ensure other execution info is present.
    assert result["job_id"]
    assert result["call_hash"]

    # The new job should be a child of the parent job.
    child_job = session.get(JobDb, result["job_id"])
    assert child_job.parent_job.id == job.id
    assert child_job.id in {j.id for j in job.child_jobs}

    # extend_run should also return errors.
    result = scheduler.extend_run(fail(), parent_job_id=job.id)
    assert isinstance(result["error"], ValueError)
    assert str(result["error"]) == "BOOM"
    assert result["job_id"]
    assert result["call_hash"]

    # Force session to refetch new data.
    job_id = job.id
    session.expunge_all()

    # The new job should be a child of the parent job.
    child_job = session.get(JobDb, result["job_id"])
    job = session.get(JobDb, job_id)
    assert child_job.parent_job.id == job.id
    assert child_job.id in {j.id for j in job.child_jobs}


def test_extend_run_dryrun(scheduler: Scheduler, session: Session) -> None:
    """
    Scheduler should support extending an Execution with dryrun.
    """

    @task
    def main(x):
        return x + 1

    @task
    def fail():
        raise ValueError("BOOM")

    assert scheduler.run(main(10)) == 11
    job = session.query(JobDb).one()

    # We should have an incomplete run due to dryrun=True.
    result = scheduler.extend_run(main(11), parent_job_id=job.id, dryrun=True)
    assert result["dryrun"]

    # If we finish the run and then do dryrun, we should get a complete run due to caching.
    result = scheduler.extend_run(main(11), parent_job_id=job.id)
    result = scheduler.extend_run(main(11), parent_job_id=job.id, dryrun=True)
    assert result["result"] == 12


@task
def foo(x):
    return {
        "pid": os.getpid(),
        "result": x,
    }


@use_tempdir
def test_federated_task() -> None:

    # Use a process executor
    config_dict = {
        "federated_tasks.sample_task": {
            "executor": "process",
            "namespace": "redun_test",
            "task_name": "module_task",
            "load_module": "redun.tests.test_scheduler",
            "config_dir": os.path.join(
                os.path.dirname(__file__),
                "test_data",
                "federated_configs",
                "federated_task_config",
            ),
            "new_execution": "True",
        },
        "executors.process": {
            "type": "local",
            "mode": "process",
            "start_method": "spawn",
        },
    }

    config = Config(config_dict=config_dict)
    config = postprocess_config(config, config_dir=os.getcwd())

    scheduler = Scheduler(config)
    scheduler.load()

    federated = federated_task("sample_task", 3)
    assert 6 == scheduler.run(federated)
    federated = federated_task("sample_task", 3, y=8)
    assert 11 == scheduler.run(federated)
    federated = federated_task("sample_task", x=4, y=8)
    assert 12 == scheduler.run(federated)

    # Check error on incorrect task
    federated = federated_task("wrong_task", task_args=[3])
    with pytest.raises(AssertionError, match="Could not find the entrypoint `wrong_task`"):
        assert 6 == scheduler.run(federated)

    # Check behavior on missing executor
    config_dict_broken = copy.deepcopy(config_dict)
    config_dict_broken["federated_tasks.sample_task"]["executor"] = "bad_executor"

    config = Config(config_dict=config_dict_broken)
    config = postprocess_config(config, config_dir=os.getcwd())

    scheduler = Scheduler(config)
    scheduler.load()

    federated = federated_task("sample_task", task_args=[4])
    with pytest.raises(
        SchedulerError,
        match='Unknown executor "bad_executor"',
    ):
        scheduler.run(federated)

    # Check behavior on missing entrypoint configs
    config_dict_broken = copy.deepcopy(config_dict)
    del config_dict_broken["federated_tasks.sample_task"]["executor"]

    config = Config(config_dict=config_dict_broken)
    config = postprocess_config(config, config_dir=os.getcwd())

    scheduler = Scheduler(config)
    scheduler.load()

    federated = federated_task("sample_task", task_args=[4])
    with pytest.raises(
        AssertionError,
        match="Federated task entry `sample_task` does not have the "
        "required keys, missing `{'executor'}`",
    ):
        scheduler.run(federated)
