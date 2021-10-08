import pickle

from redun import File, Scheduler, task
from redun.tests.utils import use_tempdir
from redun.utils import pickle_dumps
from redun.value import FileCache


class Data:
    """
    Custom datatype.
    """

    def __init__(self, data):
        self.data = data


class DataType(FileCache):
    """
    Register Data to use file-based caching.
    """

    type = Data
    base_path = "tmp"

    def _serialize(self):
        # User defined serialization.
        return pickle_dumps(self.instance.data)

    @classmethod
    def _deserialize(cls, bytes):
        # User defined deserialization.
        return Data(pickle.loads(bytes))


@use_tempdir
def test_value_serialization() -> None:
    task_calls = []

    @task()
    def task1(data):
        task_calls.append("task1")
        return data

    @task()
    def main():
        data = Data("hello")
        return task1(data)

    scheduler = Scheduler()

    assert scheduler.run(main()).data == "hello"
    assert task_calls == ["task1"]

    # Value is cached to filesystem.
    assert File("tmp/773babcb7e0ba318c7981cb5595a5cbe640ab156").exists()

    assert scheduler.run(main()).data == "hello"
    assert task_calls == ["task1"]

    # Delete file.
    File("tmp/773babcb7e0ba318c7981cb5595a5cbe640ab156").remove()

    # Task will safely run again.
    assert scheduler.run(main()).data == "hello"
    assert task_calls == ["task1", "task1"]
