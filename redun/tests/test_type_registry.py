from enum import Enum

import pytest

from redun import File, Handle, Task, task
from redun.expression import Expression
from redun.task import PartialTask
from redun.utils import pickle_dumps
from redun.value import Bool, ProxyValue, Set, Value, get_type_registry


def test_unbound_method_call():
    # Define a custom type.
    class Row:
        def __init__(self, hash, x):
            self.hash = hash
            self.x = x

    # Create a stub Value class just for augmenting Row.
    class RowValue(Value):
        type = Row

        def get_hash(self, data=None):
            return self.hash

    row = Row("aaa", 10)

    # We can call unbound methods with a different type.
    assert RowValue.get_hash(row) == "aaa"


def test_registry_raw_value():
    """
    TypeRegistry should work with raw values.
    """
    registry = get_type_registry()

    assert isinstance(registry.get_value(10), ProxyValue)
    assert registry.get_type(int) is ProxyValue
    assert registry.get_type(int, use_default=False) is None
    assert registry.get_type_name(int) == "builtins.int"
    assert registry.get_hash(10) == "279a8e0bc0a09216ec58a8d6c9f4563e21067661"
    assert registry.serialize(10) == pickle_dumps(10)
    assert registry.deserialize("builtins.int", pickle_dumps(10)) == 10
    assert registry.preprocess(10, {}) == 10
    assert registry.postprocess(10, {}) == 10
    assert registry.parse_arg(int, "10") == 10


def test_registry_value():
    """
    TypeRegistry should with Values.
    """
    registry = get_type_registry()

    # Define a custom type.
    class Row(Value):
        type_name = "myapp.Row"

        def __init__(self, hash, x):
            self.hash = hash
            self.x = x

        def __eq__(self, other):
            return self.hash == other.hash and self.x == other.x

        def get_hash(self):
            return self.hash

        def serialize(self):
            return (self.hash + ":" + str(self.x)).encode("utf8")

        @classmethod
        def deserialize(cls, raw_type, data):
            hash, x = data.decode("utf").split(":")
            return Row(hash, int(x))

        def preprocess(self, preprocess_args):
            return Row(self.hash, self.x + 1)

        def postprocess(self, postprocess_args):
            return Row(self.hash, self.x - 1)

        @classmethod
        def parse_arg(cls, raw_type, arg):
            hash, x = arg.split(":")
            return Row(hash, int(x))

    row = Row("aaa", 10)
    assert registry.get_value(row) is row
    assert registry.get_type(Row) is Row
    assert registry.get_type_name(Row) == "myapp.Row"
    assert registry.get_hash(Row("aaa", 10)) == "aaa"
    assert registry.serialize(Row("aaa", 10)) == b"aaa:10"
    assert registry.deserialize("myapp.Row", b"aaa:10") == Row("aaa", 10)
    assert registry.preprocess(Row("aaa", 10), {}) == Row("aaa", 11)
    assert registry.postprocess(Row("aaa", 11), {}) == Row("aaa", 10)
    assert registry.parse_arg(Row, "aaa:10") == Row("aaa", 10)


def test_registry_proxy_value():
    """
    TypeRegistry should work with ProxyValue.
    """
    registry = get_type_registry()

    # Define a custom type.
    class Row:
        def __init__(self, hash, x):
            self.hash = hash
            self.x = x

        def __eq__(self, other):
            return self.hash == other.hash and self.x == other.x

    # Augment custom type Row with Value methods.
    class RowValue(ProxyValue):
        type_name = "myapp.Row"
        type = Row

        def get_hash(self, data=None):
            return self.instance.hash

        def serialize(self):
            return (self.instance.hash + ":" + str(self.instance.x)).encode("utf8")

        @classmethod
        def deserialize(cls, raw_type, data):
            hash, x = data.decode("utf").split(":")
            return Row(hash, int(x))

        def preprocess(self, preprocess_args):
            return Row(self.instance.hash, self.instance.x + 1)

        def postprocess(self, postprocess_args):
            return Row(self.instance.hash, self.instance.x - 1)

        @classmethod
        def parse_arg(cls, raw_type, arg):
            hash, x = arg.split(":")
            return Row(hash, int(x))

    row = Row("aaa", 10)
    assert registry.get_value(row).instance is row
    assert registry.get_type(Row) is RowValue
    assert registry.get_type_name(Row) == "myapp.Row"
    assert registry.get_hash(Row("aaa", 10)) == "aaa"
    assert registry.serialize(Row("aaa", 10)) == b"aaa:10"
    assert registry.deserialize("myapp.Row", b"aaa:10") == Row("aaa", 10)
    assert registry.preprocess(Row("aaa", 10), {}) == Row("aaa", 11)
    assert registry.postprocess(Row("aaa", 11), {}) == Row("aaa", 10)
    assert registry.parse_arg(Row, "aaa:10") == Row("aaa", 10)


def test_registry_redun_values():
    registry = get_type_registry()

    class MyValue(Value):
        def get_hash(self, data=None):
            return "bbb"

    assert registry.get_hash(File("hello.txt")) == "18fc941eb35e16331a0709710df7e8dc75ae2619"
    assert registry.get_hash(MyValue()) == "bbb"

    assert registry.get_type_name(File) == "redun.File"
    assert registry.get_type_name(MyValue) == "redun.tests.test_type_registry.MyValue"

    # Hash using Value.get_hash()
    @task()
    def task1():
        return 10

    assert registry.get_hash(task1) == "ec0f851458d39044ae657dd250fa9526209ae6df"

    # Hash using default serialization.
    assert registry.get_hash(10) == "279a8e0bc0a09216ec58a8d6c9f4563e21067661"


class DbHandle(Handle):
    # Example Handle subclass with no type or type_name class attributes.
    def __init__(self, name, uri):
        self.uri = uri


class DbHandle2(Handle):
    type_name = "myapp.DbHandle"

    def __init__(self, name, uri):
        self.uri = uri


def test_type_name():
    pairs = [
        # Builtin python types.
        (int, "builtins.int"),
        (float, "builtins.float"),
        (bool, "builtins.bool"),
        (str, "builtins.str"),
        (dict, "builtins.dict"),
        (list, "builtins.list"),
        (set, "builtins.set"),
        # Special case python type.
        (type(None), "builtins.NoneType"),
        # redun Values.
        (Expression, "redun.expression.Expression"),
        (File, "redun.File"),
        (Task, "redun.Task"),
        (PartialTask, "redun.PartialTask"),
        (Handle, "redun.Handle"),
        (DbHandle, "redun.tests.test_type_registry.DbHandle"),
        (DbHandle2, "myapp.DbHandle"),
    ]

    registry = get_type_registry()
    for value_type, type_name in pairs:
        assert registry.get_type_name(value_type) == type_name
        if value_type is bool:
            assert registry.parse_type_name(type_name) == Bool
        elif value_type is set:
            assert registry.parse_type_name(type_name) == Set
        else:
            assert registry.parse_type_name(type_name) == value_type


def test_parse_arg():
    registry = get_type_registry()

    assert registry.parse_arg(int, "10") == 10
    assert registry.parse_arg(str, "10") == "10"
    assert registry.parse_arg(bool, "True") is True
    assert registry.parse_arg(bool, "False") is False
    assert registry.parse_arg(File, "a/b/c.txt").path == "a/b/c.txt"


def test_bool():
    registry = get_type_registry()

    assert registry.parse_arg(bool, "True")
    assert not registry.parse_arg(bool, "False")

    assert registry.serialize(True) == pickle_dumps(True)
    assert registry.serialize(False) == pickle_dumps(False)

    assert registry.deserialize("builtins.bool", pickle_dumps(True))
    assert not registry.deserialize("builtins.bool", pickle_dumps(False))
    assert registry.get_hash(True) == "4ea83061d616c782dfc446a993f359ae97cac6c8"
    assert registry.get_hash(False) == "4b0801b82e09ef3af7a2be73506b07fe7a7d5924"


def test_set():
    registry = get_type_registry()

    assert registry.get_hash({1, 2, 3}) == "0141c474b340215635cbff413fd50b019f3d6244"
    assert registry.get_hash({3, 2, 1}) == "0141c474b340215635cbff413fd50b019f3d6244"
    assert registry.get_hash([1, 2, 3]) == "e7d06e9958da58a4162eff3ccbfc83f58b7fb71a"

    assert registry.deserialize("builtins.set", registry.serialize({1, 2, 3})) == {1, 2, 3}


def test_enum() -> None:
    """
    Enum arguments should be parsable from the command line.
    """

    class Color(Enum):
        red = 1
        green = 2
        blue = 3

    registry = get_type_registry()
    assert registry.parse_arg(Color, "red") == Color.red
    with pytest.raises(ValueError):
        assert registry.parse_arg(Color, "brown") == "boom"
