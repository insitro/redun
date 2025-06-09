import asyncio
from unittest.mock import patch

import pytest

from redun import File, task
from redun.backends.db import Execution, RedunBackendDb
from redun.executors.local import LocalExecutor
from redun.scheduler import Scheduler, SchedulerError, catch

redun_namespace = "redun.tests.test_async"


def test_async_task(scheduler: Scheduler) -> None:
    """
    @task decorator should work with async functions.
    """

    @task(cache=False)
    async def add(x, y):
        return x + y

    assert add.is_async()
    assert scheduler.run(add(1, 2)) == 3


def test_call_async(scheduler: Scheduler):
    """
    Sync tasks should be able to call async tasks.
    """

    @task(cache=False)
    async def add(x, y):
        return x + y

    @task
    def main():
        return add(add(1, 2), add(3, 4))

    assert scheduler.run(main()) == 10


def test_await(scheduler: Scheduler) -> None:
    """
    Ensure we can await any kind of Expression inside an async task.
    """

    @task(cache=False)
    async def add(x, y):
        return x + y

    @task
    def mult(x, y):
        return x * y

    @task(cache=False)
    async def main():
        # We should be able to await tasks wrapping async functions.
        result = await add(1, 2)
        assert result == 3

        # We should be able to await tasks wrapping sync functions.
        result2 = await mult(3, 4)
        assert result2 == 12

        # We should be able to await on SimpleExpressions (+).
        result3 = await add(0, 1) + 1
        assert result3 == 2

        return add(result, result2) + result3

    assert scheduler.run(main()) == 17


def test_reject(scheduler: Scheduler) -> None:
    """
    Exceptions raised in an async task should be turned into normal redun job rejections.
    """

    @task(cache=False)
    async def boom():
        raise ValueError("BOOM")

    @task
    def main():
        return boom()

    with pytest.raises(ValueError):
        scheduler.run(main())

    @task
    def recover(error):
        return "OK"

    @task
    def main2():
        return catch(boom(), ValueError, recover)

    assert scheduler.run(main2()) == "OK"

    @task(cache=False)
    async def main3():
        try:
            result = await boom()
        except ValueError:
            return "OK"
        return result

    assert scheduler.run(main3()) == "OK"


@patch.object(LocalExecutor, "supports_async", return_value=False)
def test_wrong_executor(local_executor, scheduler: Scheduler) -> None:
    """
    Ensure we check whether an Executor can support async tasks.
    """

    @task(cache=False)
    async def add(a, b):
        return a + b

    with pytest.raises(SchedulerError):
        scheduler.run(add(1, 2))


def test_job_tree(scheduler: Scheduler, backend: RedunBackendDb) -> None:
    """
    Ensure the correct job tree is recorded.
    """
    assert backend.session

    @task(cache=False)
    async def add(a, b):
        await asyncio.sleep(0.05)
        return a + b

    @task(cache=False)
    async def chunk(i):
        await asyncio.sleep(0.05)
        c = await add(i, i + 1)
        return c

    @task(cache=False)
    async def main():
        chunks = [chunk(i) for i in range(5)]
        return chunks

    scheduler.run(main())
    exec = backend.session.query(Execution).one()

    """
    Job tree should be:
    - main
      - chunk
        - add
      - chunk
        - add
      - chunk
        - add
      - chunk
        - add
      - chunk
        - add
    """
    assert all([len(job.child_jobs) == 1 for job in exec.job.child_jobs])


def test_call_graph(scheduler: Scheduler, backend: RedunBackendDb) -> None:
    """
    Ensure the correct CallGraph is recorded.
    """
    assert backend.session

    @task(cache=False)
    async def add(a, b):
        await asyncio.sleep(0.05)
        return a + b

    @task(cache=False)
    async def main():
        x = await add(1, 2)
        y = await add(3, 4)
        return add(x, y)

    scheduler.run(main())
    exec = backend.session.query(Execution).one()

    # All three CallNodes for add() should be under CallNode for main(),
    # even though the first two are awaits.
    assert len(exec.job.call_node.children) == 3


def test_code_reactive(scheduler: Scheduler, backend: RedunBackendDb) -> None:
    """
    Ensure we are reactive to code changes in tasks called from await.
    """
    assert backend.session

    calls = []

    @task(cache=True, check_valid="shallow")
    async def add(a, b):
        calls.append("add")
        return a + b

    @task(cache=True, check_valid="shallow")
    async def main():
        x = await add(1, 2)
        return x + 1

    assert scheduler.run(main()) == 4
    assert calls == ["add"]

    # With caching, no more add() calls are performed.
    assert scheduler.run(main()) == 4
    assert calls == ["add"]

    # Update add() function with new logic.

    @task(cache=True, check_valid="shallow")  # type: ignore[no-redef]
    async def add(a, b):
        calls.append("add2")
        return a + b + 1

    assert scheduler.run(main()) == 5
    assert calls == ["add", "add2"]


@pytest.mark.skip(reason="This currently doesn't work.")
def test_file_reactive(scheduler: Scheduler, backend: RedunBackendDb, tmpdir: str) -> None:
    """
    Ensure we are reactive to files changes in tasks called from await.
    """
    assert backend.session

    path = "{tmpdir}/file"
    file = File(path)
    file.write("hello")

    calls = []

    @task
    def read(input: File):
        data = input.read()
        calls.append("read")
        return data

    @task
    async def main():
        data = await read(File(path))
        return data

    assert scheduler.run(main()) == "hello"
    assert calls == ["read"]

    # With caching, no more read() calls are performed.
    assert scheduler.run(main()) == "hello"
    assert calls == ["read"]

    # Update file.
    file.write("hello2")

    # read() should rerun and return new data.
    assert scheduler.run(main()) == "hello2"
    assert calls == ["read", "read2"]
