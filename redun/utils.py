import dataclasses
import inspect
import io
import itertools
import json
import pickle
import re
import sys
import threading
from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Iterable as IterableABC
from contextlib import contextmanager
from functools import lru_cache, wraps
from pickle import Unpickler
from pickle import dump as allowed_dump_func
from pickle import dumps as allowed_dumps_func
from typing import (
    IO,
    Any,
    Callable,
    Dict,
    Generic,
    Iterable,
    Iterator,
    List,
    NoReturn,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)

try:
    from typing import Protocol
except ImportError:
    from typing_extensions import Protocol  # type: ignore


T = TypeVar("T")
NULL = object()
PICKLE_PROTOCOL = 3

# Additional python import paths added by user.
_redun_import_paths: List[str] = []

_local = threading.local()


def assert_never(value: NoReturn) -> NoReturn:
    """
    Helper method for exhaustive type checking.

    https://hakibenita.com/python-mypy-exhaustive-checking#type-narrowing-in-mypy
    """
    assert False, "Unhandled value: " + repr(value)


def str2bool(text: str) -> bool:
    """
    Parse a string into a bool.
    """
    text = text.lower()
    if text == "true":
        return True
    elif text == "false":
        return False
    else:
        raise ValueError(f"Cannot parse bool: '{text}'")


def add_import_path(path: str) -> None:
    """
    Add a path to the python import paths.
    """
    # Record redun import path.
    _redun_import_paths.insert(0, path)

    safe_path = path if path != "." else ""
    sys.path.insert(0, safe_path)


def get_import_paths() -> List[str]:
    """
    Returns extra import paths that have been added.
    """
    return _redun_import_paths


def clear_import_paths() -> None:
    """
    Clear all extra python import paths.
    """
    _redun_import_paths[:] = []


def json_dumps(value: Any) -> str:
    """
    Convert JSON-like values into a normalized string.

    Keys are sorted and no whitespace is used around delimiters.
    """
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


def pickle_dump(obj: Any, file: IO) -> None:
    """
    Official pickling method for redun.

    This is used to standardize the protocol used for serialization.
    """
    return allowed_dump_func(obj, file, protocol=PICKLE_PROTOCOL)


def pickle_dumps(obj: Any) -> bytes:
    """
    Official pickling method for redun.

    This is used to standardize the protocol used for serialization.
    """
    return allowed_dumps_func(obj, protocol=PICKLE_PROTOCOL)


def pickle_loads(data: bytes) -> Any:
    """
    Official unpickling method for redun.

    This is used to allow pickle previews with a preview context is active.
    """
    if getattr(_local, "num_pickle_preview_active", 0) > 0:
        return pickle_preview(data, raise_error=_local.pickle_preview_raise_error)
    else:
        return pickle.loads(data)


@contextmanager
def with_pickle_preview(raise_error: bool = False) -> Iterator[None]:
    """
    Enable pickle preview within a context.

    We keep a count of how many nested contexts are active.
    """
    num_pickle_preview_active = getattr(_local, "num_pickle_preview_active", 0)
    _local.num_pickle_preview_active = num_pickle_preview_active + 1
    _local.pickle_preview_raise_error = raise_error
    yield
    _local.num_pickle_preview_active -= 1


def iter_nested_value_children(value: Any) -> Iterable[Tuple[bool, Any]]:
    """
    Helper function that iterates through the children of a possibly nested value.

    Yields: (is_leaf: Bool, value: Any)
    """

    value_type = type(value)

    # Note: this works for list, set, dict, tuple, namedtuple.
    if value_type in (list, tuple, set) or isinstance(value, tuple) and hasattr(value, "_fields"):
        for item in value:
            yield False, item

    elif value_type == dict:
        for key in value.keys():
            yield False, key
        for val in value.values():
            yield False, val

    elif dataclasses.is_dataclass(value_type):
        for field in dataclasses.fields(value):
            yield False, value.__dict__[field.name]

    else:
        # Visit leaf values.
        yield True, value


def iter_nested_value(value: Any) -> Iterable[Any]:
    """
    Iterate through the leaf values of a nested value.
    """
    stack = [(False, value)]
    while stack:
        is_leaf, value = stack.pop()
        if is_leaf:
            yield value
        else:
            stack.extend(iter_nested_value_children(value))


def map_nested_value(func: Callable, value: Any) -> Any:
    """
    Map a func to the leaves of a nested value.
    """
    value_type = type(value)

    if value_type == list:
        return [map_nested_value(func, item) for item in value]

    elif value_type == tuple:
        return tuple([map_nested_value(func, item) for item in value])

    elif isinstance(value, tuple) and hasattr(value, "_fields"):
        # Namedtuple.
        return value_type(*[map_nested_value(func, item) for item in value])

    elif value_type == set:
        return {map_nested_value(func, item) for item in value}

    elif value_type == dict:
        return {
            map_nested_value(func, key): map_nested_value(func, val) for key, val in value.items()
        }

    elif dataclasses.is_dataclass(value_type):
        mapped_value = value_type(
            **{
                field.name: map_nested_value(func, value.__dict__[field.name])
                for field in dataclasses.fields(value)
            }
        )
        # Copy over any non-field items from the origin value __dict__  (such as __orig_class__,
        # which exists for subscripted generic objects) that haven't made it to the mapped value.
        mapped_value.__dict__ = {
            **value.__dict__,
            **mapped_value.__dict__,
        }
        return mapped_value

    else:
        return func(value)


def trim_string(text: str, max_length: int = 200, ellipsis: str = "...") -> str:
    """
    If text is longer than max_length then return a trimmed string.
    """
    assert max_length >= len(ellipsis)
    if len(text) > max_length:
        return text[: max_length - len(ellipsis)] + ellipsis
    else:
        return text


def merge_dicts(dicts: List[T]) -> T:
    """
    Merge a list of (nested) dicts into a single dict.
    .. code-block:: python
        assert merge_dicts([
            {"a": 1, "b": 2},
            {"b": 3, "c": 4},
        ]) == {"a": 1, "b": 3, "c": 4}
        assert merge_dicts([
            {"a": {"a1": 1}},
            {"a": {"a2": 2}}
        ]) == {"a": {"a1": 1, "a2": 2}}
    """
    if len(dicts) == 1:
        return dicts[0]

    elif any(not isinstance(dct, dict) for dct in dicts):
        # For non-dicts, last value takes precedence.
        return dicts[-1]

    else:
        # Group by keys.
        key2values = defaultdict(list)
        for dct in dicts:
            # At this point, we can safely assume dct has type dict, so use cast.
            for key, value in cast(dict, dct).items():
                key2values[key].append(value)

        # Recursively merge values.
        return cast(T, {key: merge_dicts(values) for key, values in key2values.items()})


def json_cache_key(args: tuple, kwargs: dict) -> str:
    """
    Returns a cache key for arguments that are JSON compatible.
    """
    return json.dumps([args, kwargs], sort_keys=True)


class CacheArgs:
    """
    Arguments for a cached function.
    """

    def __init__(self, cache_key, args, kwargs):
        self.cache_key = cache_key
        self.args = args
        self.kwargs = kwargs

    def __hash__(self):
        """
        The normal @lru_cache will call this to build a cache key, so we
        can inject custom cache key (e.g. json_cache_key) here.
        """
        return hash(self.cache_key(self.args, self.kwargs))


def lru_cache_custom(maxsize: int, cache_key=lambda x: x):
    """
    LRU cache with custom cache key.
    """

    @lru_cache(maxsize=maxsize)
    def call_func(func: Callable[..., T], args: CacheArgs) -> T:
        return func(*args.args, **args.kwargs)

    def deco(func):
        @wraps(func)
        def wrapped(*args, **kwargs):
            return call_func(func, CacheArgs(cache_key, args, kwargs))

        return wrapped

    return deco


def get_func_source(func: Callable) -> str:
    """
    Return the source code for a function.
    """
    source = inspect.getsource(func)

    # Try to trim away the decorators.
    lines = source.split("\n")
    for i, line in enumerate(lines):
        if re.match(r"^ *def ", line):
            return "\n".join(lines[i:])

    return source


def format_table(table: List[List], justs: str, min_width: int = 0) -> Iterator[str]:
    """
    Format table with justified columns.
    """
    assert len(justs) == len(table[0])

    # Convert to strings.
    table = [[str(cell) for cell in row] for row in table]

    column_widths = [
        max(max(len(table[i][j]) for i in range(len(table))), min_width)
        for j in range(len(table[0]))
    ]

    def justify(text, just, width):
        if just == "l":
            return text.ljust(width)
        elif just == "r":
            return text.rjust(width)
        raise NotImplementedError(just)

    for i, row in enumerate(table):
        if i == 1:
            yield ""
        yield " ".join(
            justify(cell, just, width) for cell, just, width in zip(row, justs, column_widths)
        )


class PreviewClass(ABC):
    """
    Generic class to use for unknown classes in a pickle
    """

    _module = "module"
    _name = "name"

    def __init__(self, *args, **kwargs):
        self._preview_args = args
        self.__dict__.update(kwargs)

    def __new__(cls, *args, **kwargs):
        self = super().__new__(cls)
        self._preview_args = args
        self.__dict__.update(kwargs)
        return self

    def __repr__(self):
        attr = []
        if hasattr(self, "_preview_args"):
            # Object was instantiated with constructor or with __new__.
            attr.extend(map(repr, self._preview_args))

        # Object was populated through __dict__.
        attr.extend(
            f"{key}={repr(value)}"
            for key, value in self.__dict__.items()
            if key != "_preview_args"
        )
        return f"{self._name}({', '.join(attr)})"

    def __getitem__(self, index: int) -> Any:
        """
        Mimic tuple type.
        """
        return self._preview_args[index]

    # This is explicitly defined so mypy type checking works on PreviewClass instances.
    def __getattr__(self, name: str) -> Any:
        if name in self.__dict__:
            return self.__dict__[name]
        raise AttributeError(f"{name} not an attribute of {self._name} PreviewClass")


class PreviewingUnpickler(Unpickler):
    """
    A specialized unpickler that allows previewing of class attributes for
    unknown classes.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Registry of stub classes for unknown classes.
        self._classes: Dict[str, Type] = {}

    def find_class(self, module: str, name: str) -> Type:
        try:
            return super().find_class(module, name)

        except (AttributeError, ModuleNotFoundError):
            class_name = f"{module}.{name}"
            cached_class = self._classes.get(class_name)
            if cached_class:
                return cached_class

            class PreviewSubclass(PreviewClass):
                _module = module
                _name = name

            PreviewSubclass.__name__ = name
            PreviewSubclass.__module__ = module

            self._classes[class_name] = PreviewSubclass
            return PreviewSubclass


class PreviewUnpicklable:
    def __init__(self, error):
        self.error = error

    def __repr__(self) -> str:
        return "Unpicklable('{}')".format(self.error)


def pickle_preview(data: bytes, raise_error: bool = False) -> Any:
    """
    Loads a pickle file, falling back to previewing objects as needed.

    For example, if the class is unknown, will return a PreviewClass
    subclass that can be used to peek at attributes.
    """
    infile = io.BytesIO(data)
    unpickler = PreviewingUnpickler(infile)

    try:
        with with_pickle_preview(raise_error):
            return unpickler.load()
    except Exception as error:
        if raise_error:
            raise
        else:
            return PreviewUnpicklable(error)


class Comparable(Protocol):
    """Protocol for annotating comparable types."""

    @abstractmethod
    def __lt__(self: T, other: T) -> bool:
        pass


Key = TypeVar("Key", bound="Comparable")
Value = TypeVar("Value", bound="Comparable")


def _multidict2items(dct: Dict[Key, List[Value]]) -> List[Tuple[Key, Value]]:
    return [(key, value) for key, values in dct.items() for value in values]


class MultiMap(Generic[Key, Value]):
    """
    An associative array with repeated keys.
    """

    def __init__(self, items: Iterable[Union[Tuple[Key, Value], List]] = ()):
        self._data: Dict[Key, List[Value]] = {}
        self._len = 0
        for key, value in items:
            if key not in self._data:
                self._data[key] = []
            self._data[key].append(value)
            self._len += 1

    def __repr__(self) -> str:
        return "MultiMap({})".format(list(self.items()))

    def __len__(self) -> int:
        return self._len

    def __getitem__(self, key: Key) -> List[Value]:
        return self._data[key]

    def __iter__(self) -> Iterator[Tuple[Key, Value]]:
        for key, values in self._data.items():
            for value in values:
                yield (key, value)

    def __contains__(self, key: Key) -> bool:
        return key in self._data

    def _equal_dicts(self, dict1: Dict[Key, List[Value]], dict2: Dict[Key, List[Value]]) -> bool:
        return sorted(_multidict2items(dict1)) == sorted(_multidict2items(dict2))

    def __eq__(self, other: object) -> bool:
        if isinstance(other, MultiMap):
            return self._equal_dicts(self._data, other._data)

        elif isinstance(other, dict):
            return self._equal_dicts(self._data, other)

        elif isinstance(other, IterableABC):
            return sorted(self) == sorted(other)

        else:
            return False

    def has_item(self, key: Key, value: Value) -> bool:
        return value in self._data.get(key, ())

    def get(self, key: Key, default: Any = NULL) -> List[Value]:
        if default is NULL:
            default = []
        return self._data.get(key, default)

    def keys(self) -> Iterator[Key]:
        return iter(self._data.keys())

    def values(self) -> Iterator[Value]:
        return itertools.chain.from_iterable(self._data.values())

    def items(self) -> Iterator[Tuple[Key, Value]]:
        return iter(self)

    def as_dict(self) -> Dict[Key, List[Value]]:
        return self._data

    def add(self, key: Key, value: Value) -> None:
        if key not in self._data:
            self._data[key] = []
        self._data[key].append(value)
        self._len += 1
