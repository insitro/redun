import operator
from itertools import chain
from typing import Any, Callable, Dict, Generic, Iterator, List, Optional, Tuple, TypeVar, Union

from redun.hashing import hash_arguments, hash_bytes, hash_struct
from redun.utils import iter_nested_value, pickle_dumps
from redun.value import Value, get_type_registry

# Type for an Expression or an arbitrary user value.
AnyExpression = Any
Result = TypeVar("Result")


def derive_expression(
    orig_expr: "Expression", derived_value: Union["Expression", Any]
) -> "Expression":
    """
    Record derived_value as downstream of orig_expression.
    """
    if not isinstance(derived_value, Expression):
        derived_expr: Expression = ValueExpression(derived_value)
    else:
        derived_expr = derived_value
    derived_expr._upstreams = [orig_expr]
    return derived_expr


class Expression(Value, Generic[Result]):
    """
    Base class for lazy expressions.

    Lazy expressions are used to defer compute until the redun scheduler is ready
    to execute it.
    """

    def __init__(self):
        self._hash: Optional[str] = None
        self._upstreams: List[Any] = []
        self._length: Optional[int] = None

    def get_hash(self, data: Optional[bytes] = None) -> str:
        if self._hash is None:
            self._hash = self._calc_hash()
        return self._hash

    def _calc_hash(self) -> str:
        raise NotImplementedError()

    def __getstate__(self) -> dict:
        return {}

    def __setstate__(self, state: dict) -> None:
        self._hash = None
        self._upstreams = []
        self._length = None

    def __call__(self, *args, **kwargs) -> "SimpleExpression":
        """need to keep structure for lazy function calls due to backwards compatibility"""
        return SimpleExpression("call", (self, args, kwargs))

    def __getitem__(self, key: Any) -> AnyExpression:
        """
        Provides lazy list and dict access by returning Expression.
        """
        if isinstance(key, slice):
            # Act like a list or tuple and return a slice of subpromises.
            subexprs = [self[i] for i in range(key.start or 0, key.stop, key.step or 1)]
            return subexprs

        else:
            return self._get_single_item(key)

    def __getattr__(self, key) -> "SimpleExpression":
        """needed for mypy; implemented via lazy operator"""
        raise NotImplementedError

    def _get_single_item(self, key) -> "SimpleExpression":
        """separate function needed for accessing single element; implemented via lazy operator"""
        raise NotImplementedError

    def __bool__(self) -> bool:
        raise TypeError("Expressions cannot be coerced to bool.")

    def __iter__(self) -> Iterator:
        if self._length:
            return (self[i] for i in range(self._length))
        raise TypeError("Expressions of unknown length cannot be iterated.")


class ApplyExpression(Expression[Result]):
    """
    Lazy expression for applying a function or task to arguments.
    """

    def __init__(self, args: Tuple, kwargs: dict):
        super().__init__()
        self.args = args
        self.kwargs = kwargs
        self._upstreams = [args, kwargs]


def format_arguments(args, kwargs) -> str:
    """
    Format Task arguments.
    """
    kwargs_items = sorted(kwargs.items())
    text = ", ".join(
        chain((repr(arg) for arg in args), (f"{key}={repr(value)}" for key, value in kwargs_items))
    )
    return text


class TaskExpression(ApplyExpression[Result]):
    """
    Lazy expression for applying a task to arguments.
    """

    def __init__(
        self,
        task_name: str,
        args: Tuple,
        kwargs: dict,
        task_options: Optional[dict] = None,
        length: Optional[int] = None,
    ):
        super().__init__(args, kwargs)
        self.task_name = task_name
        self.call_hash: Optional[str] = None
        self.task_expr_options = task_options or {}
        self._length = length

    def __repr__(self) -> str:
        return f"{self.task_name}({format_arguments(self.args, self.kwargs)})"

    def _calc_hash(self) -> str:
        registry = get_type_registry()
        args_hash = hash_arguments(registry, self.args, self.kwargs)
        task_options_hash = hash_bytes(pickle_dumps(self.task_expr_options))
        return hash_struct(["TaskExpression", self.task_name, args_hash, task_options_hash])

    def __getstate__(self) -> dict:
        state = super().__getstate__()
        registry = get_type_registry()
        return {
            **state,
            "task_name": self.task_name,
            "args": registry.serialize(self.args),
            "kwargs": registry.serialize(self.kwargs),
            "task_options": self.task_expr_options,
        }

    def __setstate__(self, state: dict) -> None:
        super().__setstate__(state)
        registry = get_type_registry()
        self.task_name = state["task_name"]
        self.args = registry.deserialize("builtins.tuple", state["args"])
        self.kwargs = registry.deserialize("builtins.dict", state["kwargs"])
        self.task_expr_options = state.get("task_options", {})
        self._upstreams = [self.args, self.kwargs]

    def is_valid(self) -> bool:
        from redun.task import get_task_registry

        return bool(get_task_registry().get(self.task_name)) and all(
            not isinstance(value, Value) or value.is_valid()
            for value in iter_nested_value((self.args, self.kwargs))
        )


class SimpleExpression(ApplyExpression[Result]):
    """
    Lazy expression for a simple computation (e.g. getattr, getitem, call).
    """

    def __init__(self, func_name: str, args: Tuple = (), kwargs: dict = {}):
        super().__init__(args, kwargs)
        self.func_name = func_name

    def __repr__(self) -> str:
        if self.func_name in _operator_name2symbol and len(self.args) == 2:
            # Binary operator.
            left, right = self.args
            return f"({repr(left)} {_operator_name2symbol[self.func_name]} {repr(right)})"

        elif self.func_name == "getitem" and len(self.args) == 2:
            left, right = self.args
            return f"{repr(left)}[{repr(right)}]"

        elif self.func_name == "getattr" and len(self.args) == 2:
            left, right = self.args
            return f"{repr(left)}.{right}"

        elif self.func_name == "call" and len(self.args) == 3:
            this, args, kwargs = self.args
            return f"{repr(this)}({format_arguments(args, kwargs)})"

        else:
            return "SimpleExpression('{func_name}', {args}, {kwargs})".format(
                func_name=self.func_name, args=repr(self.args), kwargs=repr(self.kwargs)
            )

    def _calc_hash(self) -> str:
        registry = get_type_registry()
        args_hash = hash_arguments(registry, self.args, self.kwargs)
        return hash_struct(["SimpleExpression", self.func_name, args_hash])

    def __getstate__(self) -> dict:
        state = super().__getstate__()
        registry = get_type_registry()
        return {
            **state,
            "func_name": self.func_name,
            "args": registry.serialize(self.args),
            "kwargs": registry.serialize(self.kwargs),
        }

    def __setstate__(self, state: dict) -> None:
        super().__setstate__(state)
        registry = get_type_registry()
        self.func_name = state["func_name"]
        self.args = registry.deserialize("builtins.tuple", state["args"])
        self.kwargs = registry.deserialize("builtins.dict", state["kwargs"])
        self._upstreams = [self.args, self.kwargs]


class SchedulerExpression(TaskExpression[Result]):
    """
    Lazy expression that is evalulated within the scheduler for redun-specific operations.
    """

    def _calc_hash(self) -> str:
        registry = get_type_registry()
        args_hash = hash_arguments(registry, self.args, self.kwargs)
        return hash_struct(["SchedulerExpression", self.task_name, args_hash])


class ValueExpression(Expression[Result]):
    """
    Lifts a concrete value into an Expression type.
    """

    def __init__(self, value: Result):
        super().__init__()
        assert not isinstance(value, Expression)
        self.value = value

    def __repr__(self) -> str:
        return f"ValueExpression({repr(self.value)})"

    def _calc_hash(self) -> str:
        registry = get_type_registry()
        value_hash = registry.get_hash(self.value)
        return hash_struct(["ValueExpression", value_hash])

    def __getstate__(self) -> dict:
        state = super().__getstate__()
        registry = get_type_registry()
        return {
            **state,
            "value_type": registry.get_type_name(type(self.value)),
            "value": registry.serialize(self.value),
        }

    def __setstate__(self, state: dict) -> None:
        super().__setstate__(state)
        registry = get_type_registry()
        self.value = registry.deserialize(state["value_type"], state["value"])


class QuotedExpression(Generic[Result]):
    """
    A quoted expression that does will not evaluate until forced.
    """

    def __init__(self, expr: Result):
        self._expr = expr

    def __repr__(self) -> str:
        return f"QuotedExpression({repr(self._expr)})"

    def eval(self) -> Result:
        """
        Evaluate the quoted expression.
        """
        return self._expr


def quote(expr: Result) -> QuotedExpression[Result]:
    """
    Quote an Expression so that it does not evaluate.
    """
    return QuotedExpression(expr)


_lazy_operation_registry: Dict[str, Callable] = {}


def get_lazy_operation(name: str) -> Optional[Callable]:
    """
    Retrieves lazy operation by registered name
    """
    return _lazy_operation_registry.get(name)


def lazy_operation(
    method: Optional[str] = None,
    name: Optional[str] = None,
) -> Callable[[Callable], Callable]:
    """
    Function decorator to declare lazy operations on Expression object.

    Args:
        method: Name of method registered to Expression class. Defaults to None.
        name: Registered name of operation by which it will be retrieved. Defaults to None.
    """

    def deco(func: Callable) -> Callable:
        _method: str = method or func.__name__
        _name: str = name or func.__name__

        def wrapper(self, *args, **kwargs) -> SimpleExpression:
            return SimpleExpression(_name, (self, *args), kwargs)

        setattr(Expression, _method, wrapper)
        _lazy_operation_registry[_name] = func

        return func

    return deco


# register standard operators as lazy operations on Expressions
lazy_operators: Dict[Callable, Dict[str, str]] = {
    operator.eq: {"method": "__eq__"},
    operator.ne: {"method": "__ne__"},
    operator.lt: {"method": "__lt__"},
    operator.le: {"method": "__le__"},
    operator.gt: {"method": "__gt__"},
    operator.ge: {"method": "__ge__"},
    operator.add: {"method": "__add__"},
    operator.sub: {"method": "__sub__"},
    operator.mul: {"method": "__mul__"},
    operator.truediv: {"method": "__truediv__", "name": "div"},
    operator.and_: {"method": "__and__", "name": "and"},
    operator.or_: {"method": "__or__", "name": "or"},
}

[lazy_operation(**kwargs)(func) for func, kwargs in lazy_operators.items()]

_operator_name2symbol = {
    "eq": "==",
    "ne": "!=",
    "lt": "<",
    "le": "<=",
    "gt": ">",
    "ge": ">=",
    "add": "+",
    "sub": "-",
    "mul": "*",
    "div": "/",
    "and": "&",
    "or": "|",
}


def _lazy_call(self, args=(), kwargs={}):
    """
    Lazy evaluation for callable.
    """
    return self(*args, **kwargs)


_lazy_operation_registry["call"] = _lazy_call


@lazy_operation(name="getattr", method="__getattr__")
def _lazy_getattr(self, field: str):
    """
    Lazy evaluation for namedtuple.
    """
    return getattr(self, field)


@lazy_operation(name="getitem", method="_get_single_item")
def _lazy_getitem(self, key: Any):
    """
    Provides lazy list and dict access.
    """
    return self[key]
