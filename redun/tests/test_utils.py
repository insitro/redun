import os
import pickle
import sys
from collections import defaultdict, namedtuple
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
from typing import Any, DefaultDict, Dict, Generator, Generic, NamedTuple, Optional, TypeVar

import pytest

from redun.tests.utils import use_tempdir
from redun.utils import (
    MultiMap,
    PreviewClass,
    PreviewUnpicklable,
    add_import_path,
    clear_import_paths,
    get_import_paths,
    iter_nested_value,
    map_nested_value,
    merge_dicts,
    pickle_dumps,
    pickle_preview,
)

T = TypeVar("T", int, float, str)


@dataclass
class Data(Generic[T]):
    x: T
    y: Dict[str, Any]
    z: Optional["Data"]  # noqa: F821

    def echo(self, string: str) -> str:
        return string


@dataclass
class CustomData1(Data[float]):
    x: float


CustomData2 = Data[int]


def test_iter_nested_value() -> None:
    value = {
        "a": [1, [2, 3]],
        "b": {4.0, True, None},
        "c": ("7", "888", 9),
        "d": CustomData1(x=1.0, y={"foo": "bar"}, z=CustomData2(x=42, y={}, z=None)),
    }
    assert set(iter_nested_value(value)) == {
        "a",
        "b",
        "c",
        "d",
        1,
        2,
        3,
        4.0,
        True,
        None,
        "7",
        "888",
        9,
        1.0,
        "foo",
        "bar",
        42,
    }


def test_map_nested_value() -> None:
    """
    map_nested_value should map functions on supported container types:
    list, dict, set, tuple, nested tuple.

    map_nested_value should not map functions to other types currently
    """

    def func(value) -> str:
        return str(value) + "a"

    # Nested values are supported for namedtuple, but not defaultdict currently
    my_namedtuple = namedtuple("my_namedtuple", "a")
    test_defaultdict: DefaultDict[str, int] = defaultdict(int)
    test_defaultdict["a"]
    test_namedtuple = my_namedtuple(a=1)
    test_dataclass = CustomData1(x=1.0, y={"foo": "bar"}, z=CustomData2(x=42, y={}, z=None))

    value = [
        [1, [2]],
        {"a": 1, "b": {"c": 2}},
        (1, (2,)),
        {1, (2,)},
        test_namedtuple,
        test_defaultdict,
        test_dataclass,
    ]

    assert map_nested_value(func, value) == [
        ["1a", ["2a"]],
        {"aa": "1a", "ba": {"ca": "2a"}},
        ("1a", ("2a",)),
        {("2a",), "1a"},
        my_namedtuple(a="1a"),
        "defaultdict(<class 'int'>, {'a': 0})a",
        CustomData1(
            x="1.0a", y={"fooa": "bara"}, z=CustomData2(x="42a", y={}, z="Nonea")  # type: ignore
        ),
    ]


@pytest.mark.parametrize(
    "dicts,expected",
    [
        # Base cases.
        ([], {}),
        ([{}], {}),
        ([{"a": 1}], {"a": 1}),
        # Combine keys.
        (
            [
                {"a": 1, "b": 2},
                {"b": 3, "c": 4},
            ],
            {"a": 1, "b": 3, "c": 4},
        ),
        (
            [{"a": 1, "b": 2}, {"b": 3, "c": 4}, {"d": 5}],
            {"a": 1, "b": 3, "c": 4, "d": 5},
        ),
        (
            [
                {"a": {"a1": 1}},
                {"a": {"a2": 2}},
            ],
            {"a": {"a1": 1, "a2": 2}},
        ),
        # Mix dict vs non-dict.
        (
            [
                {"a": {"b": 1}},
                {"a": 2},
                {"a": 3},
            ],
            {"a": 3},
        ),
    ],
)
def test_merge_dicts(dicts, expected) -> None:
    """
    Should be able to merge nested dictionaries together.
    """
    assert merge_dicts(dicts) == expected


def test_import_paths() -> None:
    """
    We should be able to prepend new import paths, retrieve them, clear them.
    """
    # New paths should prepend.
    original_paths = list(sys.path)
    add_import_path("mypath")
    add_import_path("mypath2")
    assert sys.path == ["mypath2", "mypath"] + original_paths

    # We should be able to retrieve just the redun paths.
    assert get_import_paths() == ["mypath2", "mypath"]

    # We should be able to clear redun paths.
    clear_import_paths()
    assert get_import_paths() == []


# Class to pickle for test functions.
class User:
    def __init__(self, name: str, age: int, friend: "User" = None):
        self.name = name
        self.age = age
        self.friend = friend


@contextmanager
def hide_class(name) -> Generator:
    user_class = globals()[name]
    del globals()[name]
    yield
    globals()[name] = user_class


def test_pickle_preview_class() -> None:
    """Tests pickle preview when pickled class is not imported"""

    y = [2, 3]
    x = [1, y, User("Alice", 30, User("Bob", 31))]
    data = pickle_dumps(x)

    with hide_class("User"):
        # AttributeError should raise since User and myfunc can't be found.
        with pytest.raises(AttributeError):
            pickle.loads(data)

        # However, pickle_preview() still works by using stub classes.
        x2 = pickle_preview(data)
    assert (
        str(x2) == "[1, [2, 3], User(name='Alice', age=30,"
        " friend=User(name='Bob', age=31, friend=None))]"
    )

    assert isinstance(x2[2], PreviewClass)
    assert x2[2].name == "Alice"
    assert x2[2].age == 30
    assert isinstance(x2[2].friend, PreviewClass)
    assert x2[2].friend.name == "Bob"
    assert x2[2].friend.age == 31

    with pytest.raises(AttributeError):
        assert x2[2].invalid == "whoops"


def test_pickle_preview_dataclass() -> None:
    """Tests pickle preview when pickled dataclass is not imported"""
    x = 1
    y = {"2": {"3": "4"}}
    z = CustomData1(x, y, None)
    obj = CustomData1(x, y, z)

    data = pickle_dumps(obj)

    with hide_class("CustomData1"):
        # AttributeError should raise since Data can't be found.
        with pytest.raises(AttributeError):
            pickle.loads(data)

        # However, pickle_preview() still works by using stub classes.
        obj2 = pickle_preview(data)
    assert (
        str(obj2)
        == "CustomData1(x=1, y={'2': {'3': '4'}}, z=CustomData1(x=1, y={'2': {'3': '4'}}, z=None))"
    )

    assert isinstance(obj2, PreviewClass)
    assert obj2.x == 1
    assert obj2.y == {"2": {"3": "4"}}
    assert isinstance(obj2.z, PreviewClass)
    assert obj2.z.x == 1
    assert obj2.z.y == {"2": {"3": "4"}}
    assert obj2.z.z is None

    with pytest.raises(AttributeError):
        assert obj2.invalid == "whoops"


@use_tempdir
def test_pickle_preview_module() -> None:
    """Tests pickle preview when pickled module is not importable"""

    # External module containing class definition.
    with open("the_class.py", "w") as temp:
        temp.write(
            """
class ConfusedUser:
    def __init__(self, name: str, confusion: int):
        self.name = name
        self.confusion = confusion
        """
        )

    # Create a class instance and pickle it.
    sys.path.append(".")
    from the_class import ConfusedUser

    x = ConfusedUser("Robin", 9000)
    data = pickle_dumps(x)

    # Delete the module and unimport it.
    os.remove("the_class.py")
    del sys.modules["the_class"]

    with pytest.raises(ModuleNotFoundError):
        pickle.loads(data)

    x2 = pickle_preview(data)

    assert isinstance(x2, PreviewClass)
    assert x2.name == "Robin"
    assert x2.confusion == 9000


def test_pickle_unpicklable() -> None:
    """Tests pickle preview of un-unpicklable objects"""

    data = b"This is not a valid pickle file!"

    with pytest.raises(pickle.UnpicklingError):
        pickle.loads(data)

    x2 = pickle_preview(data)
    assert isinstance(x2, PreviewUnpicklable)
    assert isinstance(x2.error, pickle.UnpicklingError)
    assert str(x2.error) == "pickle data was truncated"


class Color(Enum):
    RED = 1
    GREEN = 2
    BLUE = 3


def test_pickle_preview_enum() -> None:
    """
    Pickle preview should instantiate Enums.
    """
    data = pickle_dumps(Color.RED)

    with hide_class("Color"):
        with pytest.raises(AttributeError):
            pickle.loads(data)

        obj = pickle_preview(data)

    assert repr(obj) == "Color(1)"
    assert obj[0] == 1


class UserTuple(NamedTuple):
    name: str
    age: int


def test_pickle_preview_namedtuple() -> None:
    """
    Pickle preview should instantiate Enums.
    """
    data = pickle_dumps(UserTuple("Alice", 30))

    with hide_class("UserTuple"):
        with pytest.raises(AttributeError):
            pickle.loads(data)

        obj = pickle_preview(data)

    assert repr(obj) == "UserTuple('Alice', 30)"
    assert obj[0] == "Alice"
    assert obj[1] == 30


def test_multimap() -> None:
    """
    Test that a MultiMap supports its operations.
    """
    mmap: MultiMap = MultiMap([["a", 1], ["a", 2], ["b", "X"], ["c", "Y"]])

    assert len(mmap) == 4

    assert mmap["a"] == [1, 2]
    assert mmap["b"] == ["X"]
    assert mmap.get("c") == ["Y"]
    assert mmap.get("d") == []
    assert mmap.get("e", [True]) == [True]

    assert "c" in mmap
    assert "d" not in mmap
    assert mmap.has_item("a", 2)
    assert not mmap.has_item("a", 3)
    assert not mmap.has_item("d", 3)

    assert dict(iter(mmap)) == {"a": 2, "b": "X", "c": "Y"}
    assert sorted(mmap.keys()) == ["a", "b", "c"]
    assert set(mmap.values()) == {1, 2, "X", "Y"}
    assert list(mmap) == [("a", 1), ("a", 2), ("b", "X"), ("c", "Y")]
    assert mmap.as_dict() == {
        "a": [1, 2],
        "b": ["X"],
        "c": ["Y"],
    }

    mmap2 = MultiMap(mmap)
    assert mmap == mmap2
    assert mmap != MultiMap([[10, 20], [30, 40]])
    assert mmap == {
        "a": [1, 2],
        "b": ["X"],
        "c": ["Y"],
    }
    assert mmap == list(mmap)

    # Add key-value pairs.
    mmap.add("d", 7)
    mmap.add("d", 8)
    mmap.add("e", 9)
    assert mmap.get("d") == [7, 8]
    assert len(mmap) == 7
