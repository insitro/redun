from sqlalchemy.orm import Session

from redun import Scheduler, task
from redun.backends.db import Argument, CallNode, Execution


def test_record_args():
    @task()
    def task1(a, b):
        return a + b

    @task()
    def task2(x):
        return x

    @task()
    def main():
        return task1(task2(2), task2(3))

    scheduler = Scheduler()
    scheduler.run(main())

    args = scheduler.backend.session.query(Argument).all()

    assert {arg.value_parsed for arg in args} == {2, 3}
    assert args[0].call_node.task.name == "task2"

    args = (
        scheduler.backend.session.query(Argument)
        .join(CallNode, CallNode.call_hash == Argument.call_hash)
        .filter(CallNode.task_hash == task1.hash)
        .order_by(Argument.arg_position)
        .all()
    )

    assert args[0].call_node.task.name == "task1"
    assert args[0].arg_position == 0
    assert args[0].value_parsed == 2
    assert args[0].upstream[0].task.name == "task2"

    assert args[1].call_node.task.name == "task1"
    assert args[1].arg_position == 1
    assert args[1].value_parsed == 3
    assert args[1].upstream[0].task.name == "task2"


def test_record_kwargs():
    @task()
    def task1(a=2, b=3):
        return a + b

    @task()
    def task2(x):
        return x

    @task()
    def main():
        return task1(a=task2(2), b=task2(3))

    scheduler = Scheduler()
    scheduler.run(main())

    args = (
        scheduler.backend.session.query(Argument)
        .join(CallNode, CallNode.call_hash == Argument.call_hash)
        .filter(CallNode.task_hash == task1.hash)
        .order_by(Argument.arg_position)
        .all()
    )

    assert args[0].call_node.task.name == "task1"
    assert args[0].arg_position is None
    assert args[0].arg_key == "a"
    assert args[0].value_parsed == 2
    assert args[0].upstream[0].task.name == "task2"

    assert args[1].call_node.task.name == "task1"
    assert args[1].arg_position is None
    assert args[1].arg_key == "b"
    assert args[1].value_parsed == 3
    assert args[1].upstream[0].task.name == "task2"


def test_record_args_nested():
    @task()
    def task1(a, b):
        return [i + j for i, j in zip(a, b)]

    @task()
    def task2(x):
        return x

    @task()
    def main():
        return task1(
            [task2(2), task2(5)],
            [task2(3), task2(4)],
        )

    scheduler = Scheduler()
    scheduler.run(main())

    args = (
        scheduler.backend.session.query(Argument)
        .join(CallNode, CallNode.call_hash == Argument.call_hash)
        .filter(CallNode.task_hash == task1.hash)
        .order_by(Argument.arg_position)
        .all()
    )

    assert args[0].upstream[0].task.name == "task2"
    assert args[0].upstream[1].task.name == "task2"
    assert args[1].upstream[0].task.name == "task2"
    assert args[1].upstream[1].task.name == "task2"


def test_record_args_dup_upstream(scheduler: Scheduler, session: Session) -> None:
    """
    Ensure an argument could have multiple subvalues from the same upstream.
    """

    @task()
    def task1(x):
        return x

    @task()
    def task2():
        return 1

    @task()
    def main():
        arg = task2()
        return task1([arg, arg])

    assert scheduler.run(main()) == [1, 1]

    execution = session.query(Execution).one()
    [task1_call_node] = [
        call_node
        for call_node in execution.job.call_node.children
        if call_node.task_name == "task1"
    ]
    assert task1_call_node.arguments[0].upstream[0].task_name == "task2"
