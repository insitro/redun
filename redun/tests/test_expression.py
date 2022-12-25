import pytest

from redun import Scheduler, task
from redun.expression import SimpleExpression, TaskExpression, ValueExpression
from redun.functools import identity
from redun.task import scheduler_task


def test_value_expression(scheduler: Scheduler) -> None:
    """
    ValueExpression should evaluate to itself.
    """
    # Test evaluation.
    expr = ValueExpression(10)
    assert scheduler.run(expr) == 10

    # Test hashing.
    assert expr.get_hash() == "8ce63809f39a33d99358ccb2b18c8761d6764aea"

    # Test caching.
    state = expr.__getstate__()
    assert state == {
        "value_type": "builtins.int",
        "value": b"\x80\x03K\n.",
    }

    # Test deserialization.
    expr2 = ValueExpression.__new__(ValueExpression)
    expr2.__setstate__(state)
    assert expr2.value == 10

    # Test valid checking.
    assert expr.is_valid()


def test_simple_expression(scheduler: Scheduler) -> None:
    # Test evaluation.
    expr: SimpleExpression[str] = SimpleExpression("getitem", (["a", "b", "c"], 1))
    assert scheduler.run(expr) == "b"

    expr = SimpleExpression("getitem", (["a", "b", "c"], 5))
    with pytest.raises(IndexError):
        scheduler.run(expr)

    expr = SimpleExpression("bad_func", (10,))
    with pytest.raises(NotImplementedError):
        scheduler.run(expr)

    # Test hashing.
    expr = SimpleExpression("getitem", (["a", "b", "c"], 1))
    assert expr.get_hash() == "52d8a5c4b3672fc3dc7d53d5eb7a675a51f62894"

    # Test caching.
    state = expr.__getstate__()
    assert state == {
        "func_name": "getitem",
        "args": (
            b"\x80\x03]q\x00(X\x01\x00\x00\x00aq\x01X\x01\x00\x00\x00bq\x02X\x01"
            b"\x00\x00\x00cq\x03eK\x01\x86q\x04."
        ),
        "kwargs": b"\x80\x03}q\x00.",
    }

    expr2 = SimpleExpression.__new__(SimpleExpression)
    expr2.__setstate__(state)
    assert expr2.func_name == "getitem"
    assert expr2._upstreams == [(["a", "b", "c"], 1), {}]
    assert scheduler.run(expr2) == "b"

    # Test valid checking.
    assert expr2.is_valid()


class Inner:
    def __init__(self, a: int):
        self.a = a


def test_lazy_operators(scheduler: Scheduler) -> None:
    @task()
    def task1(x: int, y: int = 1) -> int:
        return x + y

    @task()
    def task2(x: str, y: int = 3) -> str:
        return x * y

    @task()
    def get_task():
        return task1

    true_expressions = [
        task1(0) == ValueExpression(1),
        task1(1) != ValueExpression(1),
        task1(2) < task1(3),
        task1(2) <= task1(3),
        task1(3) > task1(2),
        task1(3) >= task1(2),
        task1(4) + task1(5) == 11,
        task1(3) - task1(1) == 2,
        task1(1) * task1(2) == 6,
        task1(3) / task1(1) == 2,
        task2("a") + task2("bc") == "aaabcbcbc",
        identity(True) & identity(True),
        identity(False) | identity(True),
        get_task()(7) == 8,
        ValueExpression(Inner(7)).a == 7,
        ValueExpression(["a", "b", "c"])[1] == "b",
    ]

    for expr in true_expressions:
        assert isinstance(expr, SimpleExpression)
        assert scheduler.run(expr) is True

    false_expressions = [
        task1(0) != ValueExpression(1),
        task1(1) == ValueExpression(1),
        task1(2) >= task1(3),
        task1(2) > task1(3),
        task1(3) <= task1(2),
        task1(3) < task1(2),
        task1(4) + task1(5) != 11,
        task1(3) - task1(1) != 2,
        task1(1) * task1(2) != 6,
        task1(3) / task1(1) != 2,
        task2("a") + task2("bc") == "aaabcbcbcd",
        identity(False) & identity(True),
        identity(False) | (False),
        get_task()(7) == 9,
        ValueExpression(Inner(7)).a == 8,
        ValueExpression(["a", "b", "c"])[1] == "f",
    ]

    for expr in false_expressions:
        assert isinstance(expr, SimpleExpression)
        assert scheduler.run(expr) is False


def test_task_expression(scheduler: Scheduler) -> None:
    @task()
    def task1(x, y=1):
        return x + y

    expr = task1(10)
    assert isinstance(expr, TaskExpression)
    assert expr.task_name == "task1"
    assert expr.args == (10,)
    assert expr.kwargs == {}
    assert scheduler.run(expr) == 11

    expr = task1(10, y=3)
    assert expr.kwargs == {"y": 3}
    assert scheduler.run(expr) == 13

    @task()
    def buggy():
        raise ValueError("bugs")

    with pytest.raises(ValueError):
        scheduler.run(buggy())

    # Test hashing.
    expr = TaskExpression("task1", (10,), {})
    assert expr.get_hash() == "7076d9536c59c05862286df0d767468098a4f0ed"

    # Test caching.
    state = expr.__getstate__()
    assert state == {
        "task_name": "task1",
        "args": b"\x80\x03K\n\x85q\x00.",
        "kwargs": b"\x80\x03}q\x00.",
        "task_options": {},
    }

    expr2 = TaskExpression.__new__(TaskExpression)
    expr2.__setstate__(state)
    assert expr2.task_name == "task1"
    assert scheduler.run(expr2) == 11

    # Test valid checking.
    assert expr2.is_valid()
    expr2.task_name = "bad_task"
    assert not expr2.is_valid()

    # Test call expressions.
    assert scheduler.run(SimpleExpression("call", (task1, (2,), {"y": 3}))) == 5


def test_ban_bool_coerce() -> None:
    """
    Coercision of an Expression to bool should be ban to avoid common bugs.
    """
    expr = ValueExpression(10)
    with pytest.raises(TypeError):
        if expr:
            pass


def test_ban_iter() -> None:
    """
    Ban iteration on expression to prevent common misuse.
    """

    @task()
    def task1():
        return [1, 2, 3]

    expr = task1()

    with pytest.raises(TypeError):
        for x in expr:
            pass

    with pytest.raises(TypeError):
        _ = iter(expr)


def test_repr() -> None:
    """
    Test the repr of an Expression matches the expected syntax.
    """

    @task
    def add(a, b):
        pass

    @scheduler_task()
    def cond(scheduler, parent_job, sexpr, pred, then, else_):
        pass

    # Common task calls.
    assert repr(add(1, 2)) == "add(1, 2)"
    assert repr(add(1, b=2)) == "add(1, b=2)"
    assert repr(add(a=True, b="2")) == "add(a=True, b='2')"
    assert repr(add([1, 2, 3], [4])) == "add([1, 2, 3], [4])"

    # Recursive task calls.
    assert repr(add(add(1, 2), add(3, 4))) == "add(add(1, 2), add(3, 4))"

    # SchedulerTask calls.
    assert repr(cond(True, add(1, 2), add(3, 4))) == "cond(True, add(1, 2), add(3, 4))"

    # Simple expression operators.
    assert repr(add(1, 2) + 3) == "(add(1, 2) + 3)"
    assert repr(add(1, 2) + 3 + 4) == "((add(1, 2) + 3) + 4)"
    assert repr(add(1, 2) == 3) == "(add(1, 2) == 3)"
    assert repr(add(1, 2) & add(3, 4)) == "(add(1, 2) & add(3, 4))"

    # Simple expressions: getitem, getattr, call.
    assert repr(add(1, 2)["key"]) == "add(1, 2)['key']"
    assert repr(add(1, 2)[1]) == "add(1, 2)[1]"
    assert repr(add(1, 2)[1:3]) == "[add(1, 2)[1], add(1, 2)[2]]"
    assert repr(add(1, 2)[:2]) == "[add(1, 2)[0], add(1, 2)[1]]"
    assert repr(add(1, 2).attr) == "add(1, 2).attr"
    assert repr(add(1, 2).func(1, 2)) == "add(1, 2).func(1, 2)"
