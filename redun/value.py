import datetime
import importlib
import os
from enum import Enum
from typing import Any, Callable, Dict, Iterator, Optional, Type, cast

from dateutil.parser import parse as parse_date

from redun.hashing import hash_bytes, hash_struct, hash_tag_bytes
from redun.utils import get_func_source, iter_nested_value, pickle_dumps, pickle_loads

MIME_TYPE_PICKLE = "application/python-pickle"


class TypeError(Exception):
    pass


class InvalidValueError(Exception):
    pass


# Python does not provide a type for None, so we compute it here.
NoneType: Type = type(None)


def get_raw_type_name(raw_type: Any) -> str:
    return "{}.{}".format(raw_type.__module__, raw_type.__name__)


class TypeRegistry:
    """
    A registry for redun types and a dispatch for value methods.
    """

    def __init__(self):
        self._type_name2value_type: Dict[str, Type["Value"]] = {}
        self._raw2proxy_type: Dict[Any, Type["ProxyValue"]] = {}

    def get_type_name(self, raw_type: Type) -> str:
        """
        Returns the type name for a python type.
        """
        value_type = self.get_type(raw_type, use_default=False)
        if value_type:
            assert value_type.type_name
            return value_type.type_name

        # All Value classes should be registered at this point.
        assert not issubclass(raw_type, Value)

        return get_raw_type_name(raw_type)

    def parse_type_name(self, type_name: str) -> Type:
        """
        Parse a type name into a python type.
        """
        if type_name == "builtins.NoneType":
            return NoneType

        value_type = self._type_name2value_type.get(type_name)
        if value_type:
            return value_type

        try:
            module_name, raw_type_name = type_name.rsplit(".", 1)
            return importlib.import_module(module_name).__dict__[raw_type_name]
        except (ValueError, ModuleNotFoundError, KeyError):
            raise TypeError('Unable to import type "{}"'.format(type_name))

    def register(self, value_type: Type["Value"]) -> None:
        """
        Register a Value class with the type system.
        """
        if getattr(value_type, "__args__", None):
            # In python 3.6, parameterized types try to register themselves
            # and can conflict with registering the unparameterized type.
            # We do not need them registered, so we ignore them here.
            return

        # Note: getattr is used because Values start registering even before ProxyValue is defined.
        if getattr(value_type, "proxy", False) and getattr(value_type, "type", None):
            # value_type is a proxy for another raw type.
            self._raw2proxy_type[getattr(value_type, "type")] = cast(Type[ProxyValue], value_type)

        assert isinstance(value_type.type_name, str)
        self._type_name2value_type[cast(str, value_type.type_name)] = value_type

    def _get_proxy_type(self, raw_type: type) -> Optional[Type["Value"]]:
        """
        Search through a type's superclasses for a registered ProxyValue.
        """
        for super_raw_type in raw_type.__class__.mro(raw_type):
            proxy_type = self._raw2proxy_type.get(super_raw_type)
            if proxy_type:
                self._raw2proxy_type[raw_type] = proxy_type
                return proxy_type
        return None

    def get_value(self, raw_value: Any) -> "Value":
        """
        Return a Value instance for a raw python type.
        """
        raw_type = type(raw_value)
        proxy_type = self._get_proxy_type(raw_type)
        if proxy_type:
            return proxy_type(raw_value)

        elif isinstance(raw_value, Value):
            return raw_value

        else:
            return ProxyValue(raw_value)

    def get_type(self, raw_type: Type, use_default: bool = True) -> Optional[Type["Value"]]:
        """
        Return a registered Value class for a raw python type.
        """
        proxy_type = self._get_proxy_type(raw_type)
        if proxy_type:
            return proxy_type

        elif isinstance(raw_type, type) and issubclass(raw_type, Value):
            return raw_type

        elif use_default:
            return ProxyValue

        else:
            return None

    def is_valid(self, value: Any) -> bool:
        """
        Returns True if the value is still good to use since being cached.

        For example, if the value was a File reference and the corresponding
        file on the filesystem has since been removed/altered, the File
        reference is no longer valid.
        """
        return self.get_value(value).is_valid()

    def is_valid_nested(self, nested_value: Any) -> bool:
        """
        Returns True if nested value is still good to use since being cached.
        """
        return all(map(self.is_valid, iter_nested_value(nested_value)))

    def get_hash(self, value: Any, data: Optional[bytes] = None) -> str:
        """
        Hashes a value according to the hash method precedence.
        """
        return self.get_value(value).get_hash()

    def get_serialization_format(self, value: Any) -> str:
        """
        Returns mimetype of serialization.
        """
        return self.get_value(value).get_serialization_format()

    def serialize(self, value: Any) -> bytes:
        """
        Serializes the Value into a byte sequence.
        """
        return self.get_value(value).serialize()

    def deserialize(self, type_name: str, data: bytes) -> Any:
        """
        Returns a deserialization of bytes `data` into a new Value.
        """
        try:
            raw_type = self.parse_type_name(type_name)
        except TypeError:
            raise InvalidValueError(f"Unknown type: {type_name}")
        value_type = self.get_type(raw_type)
        assert value_type
        return value_type.deserialize(raw_type, data)

    def preprocess(self, raw_value: Any, preprocess_args: dict) -> Any:
        """
        Preprocess a value before passing to a Task.
        """
        return self.get_value(raw_value).preprocess(preprocess_args)

    def postprocess(self, raw_value: Any, postprocess_args: dict) -> Any:
        """
        Post process a value resulting from a Task.
        """
        return self.get_value(raw_value).postprocess(postprocess_args)

    def parse_arg(self, raw_type: Type, arg: str) -> Any:
        """
        Parse a command-line argument of type value_type.
        """
        value_type = self.get_type(raw_type, use_default=False)
        if value_type:
            return value_type.parse_arg(raw_type, arg)
        else:
            # Try to parse argument using raw_type constructor.
            return raw_type(arg)

    def iter_subvalues(self, raw_value: Any) -> Iterator["Value"]:
        """
        Iterates through the Value's subvalues.
        """
        return self.get_value(raw_value).iter_subvalues()


# Global singleton instance of TypeRegistry.
_type_registry = TypeRegistry()


def get_type_registry() -> TypeRegistry:
    """
    Return the global singleton instance of TypeRegistry.
    """
    return _type_registry


class MetaValue(type):
    """
    Registers Value classes as they are defined.
    """

    def __init__(cls, name, bases, dct, **kwargs):
        # Use cls.__dict__ to look for class attributes defined directly on class
        # and ignore what is set on super classes.
        if not cls.__dict__.get("type_name"):
            if cls.__dict__.get("type"):
                cls.type_name = get_raw_type_name(cls.type)
            else:
                cls.type_name = get_raw_type_name(cls)

        if getattr(cls, "register", True):
            registry = get_type_registry()
            registry.register(cls)


# In Python 3.6, in order to use metaclass for a Generic class we need to
# inherit from GenericMeta.
# https://github.com/python/typing/issues/449
try:
    # Types are ignored here because GenericMeta has been removed in python 3.7+
    from typing import GenericMeta  # type: ignore

    class MetaValue(MetaValue, GenericMeta):  # type: ignore
        def __init__(cls, name, bases, dct, **kwargs):
            super().__init__(name, bases, dct, **kwargs)

except ImportError:
    pass


class Value(metaclass=MetaValue):
    """
    The base class for tracking inputs and outputs in redun, encapsulating both value
    and state, in a hashable and serializable form.

    This class underpins the implementation of memoization of `redun.Task`s. Hashing is used
    as the sole, unique identifier for a value; there is no fallback to deep equality testing.
    Hence, we can hash `Task` inputs to identify repeated invocations and supply the cached
    result instead. Serialization makes it feasible to cache outputs of tasks.

    In addition to handling ordinary values, i.e., data or objects represented in memory, complex
    workflows often require reasoning about state that may be complex or external, such as
    a database or filesystem. Redun accomplishes reasoning over this state by converting it into
    value-like semantics, after which, redun can proceed without differentiating between values
    and state. Specifically, this class provides that abstraction. Therefore, this class offers
    a more complex lifecycle than would be required for ordinary values.

    The default hashing and serialization methods are designed for simple object values. Child
    classes may customize these as needed; see below for the API details.

    We introduce two atypical lifecycle steps that are needed for more complex cases, especially
    those involving values that wrap state: validity checking and pre/post processing.
    The scheduler will arrange for pre- and post-processing hooks to be called on `Value`
    objects around their use in `Task` implementations. This allows users to implement some
    more complex concepts, such as lazy initialization or to temporarily allocate resources.

    See `docs/source/design.md` for more information about "Validity" of values, which is an
    important concept for this class.
    """

    type_name: Optional[str] = None

    def __init__(self, *args, **kwargs):
        pass

    def is_valid(self) -> bool:
        """
        Returns `True` if the value may be used. For ordinary values, this will typically always
        be true. For state-tracking values, the deserialized cache value may no longer be valid.
        If this method returns `False`, the object will typically be discarded.

        This method may be called repeatedly on the same object.

        For example, if the value was a `File` reference and the corresponding
        file on the filesystem has since been removed/altered, the `File`
        reference is no longer valid.
        """
        return True

    def get_hash(self, data: Optional[bytes] = None) -> str:
        """
        Returns a hash for the value. Users may override to provide custom behavior.

        Most basic objects are covered by the default approach of hashing the binary
        pickle serialization.
        """
        if data is None:
            data = pickle_dumps(self)
        return hash_tag_bytes("Value", data)

    def get_serialization_format(self) -> str:
        """
        Returns mimetype of serialization.
        """
        return MIME_TYPE_PICKLE

    def serialize(self) -> bytes:
        """
        Serializes the Value into a byte sequence.
        """
        return pickle_dumps(self)

    @classmethod
    def deserialize(cls, raw_type: type, data: bytes) -> Any:
        """
        Returns a deserialization of bytes `data` into a new Value.
        """
        return pickle_loads(data)

    def __getstate__(self) -> dict:
        """
        Returns a plain python datastructure for serialization.

        This is the standard pickle method, which users may implement as needed.
        """
        raise NotImplementedError()

    def __setstate__(self, state: dict) -> None:
        """
        Populates the value from state during deserialization.

        This is the standard pickle method, which users may implement as needed.
        """
        raise NotImplementedError()

    def preprocess(self, preprocess_args: dict) -> "Value":
        """
        Preprocess a value before passing it to a Task, and return the preprocessed value.

        This is a scheduler lifecycle method and is not typically invoked by users.
        This method may be called repeatedly on a particular instance, for example, if a single
        `Value` is passed to multiple `Task`s.

        Parameters
        ----------
        preprocess_args : dict
            Extra data provided by the scheduler. Implementations that are not part of redun
            should discard this argument.
        """
        # Ordinary values don't need to do anything.
        return self

    def postprocess(self, postprocess_args: dict) -> "Value":
        """
        Post process a value resulting from a Task, and return the postprocessed value.

        This is a scheduler lifecycle method and is not typically invoked by users.
        This method may be called repeatedly on a particular instance, for example, if it is
        returned recursively.

        Parameters
        ----------
        postprocess_args : dict
            Extra data provided by the scheduler. Implementations that are not part of redun
            should discard this argument.
        """
        # Ordinary values don't need to do anything.
        return self

    @classmethod
    def parse_arg(cls, raw_type: type, arg: str) -> Any:
        """
        Parse a command line argument in a new Value.
        """
        # By default, try to use constructor to parse command line argument.
        return cls(arg)

    def iter_subvalues(self) -> Iterator["Value"]:
        """
        Iterates through the Value's subvalues.
        """
        if False:
            # By default, Values have no subvalues, but we need the yield
            # statement here to convert this method into an iterator.
            # Subclasses can then override this method for their own needs.
            yield
        return


class ProxyValue(Value):
    """
    Class for overriding Value behavior (hashing, serialization) for raw python types.
    """

    proxy = True
    type: Any = None

    def __init__(self, instance: Any):
        self.instance = instance

    def get_hash(self, data: Optional[bytes] = None) -> str:
        """
        Returns a hash for the value.
        """
        if data is None:
            data = pickle_dumps(self.instance)
        return hash_tag_bytes("Value", data)

    def serialize(self) -> bytes:
        """
        Serializes the Value into a byte sequence.
        """
        return pickle_dumps(self.instance)

    def preprocess(self, preprocess_args) -> "Value":
        """
        Preprocess a value before passing to a Task.
        """
        return self.instance

    def postprocess(self, postprocess_args) -> "Value":
        """
        Post process a value resulting from a Task.
        """
        return self.instance

    def iter_subvalues(self) -> Iterator["Value"]:
        """
        Iterates through the Value's subvalues.
        """
        if isinstance(self.instance, Value):
            # No subvalues.
            return
        for subvalue in iter_nested_value(self.instance):
            if isinstance(subvalue, Value):
                yield subvalue
                yield from subvalue.iter_subvalues()


class Bool(ProxyValue):
    """
    Augment builtins.bool to support argument parsing.
    """

    type = bool
    type_name = "builtins.bool"

    @classmethod
    def parse_arg(cls, raw_type: Type, arg: str) -> Any:
        arg = arg.lower()
        if arg == "true":
            return True
        elif arg == "false":
            return False
        else:
            raise ValueError('Unknown bool "{}"'.format(arg))


class Set(ProxyValue):
    """
    Augment builtins.set to support stable hashing.
    """

    type = set
    type_name = "builtins.set"

    def get_hash(self, data: Optional[bytes] = None) -> str:
        # Sort the set to ensure stable serialization and hashing.
        bytes = pickle_dumps(sorted(self.instance))

        # Use a unique tag to distinguish from hashing a list.
        return hash_tag_bytes("Value.set", bytes)


class EnumType(ProxyValue):
    """
    Augment enum.Enum to support argument parsing with choices.
    """

    type = Enum
    type_name = "enum.Enum"

    @classmethod
    def parse_arg(cls, raw_type: Type, arg: str) -> Any:
        name2item = {item.name: item for item in raw_type}
        try:
            if "." in arg:
                enum_name, arg = arg.split(".", 1)
                if enum_name != raw_type.__name__:
                    raise KeyError
            return name2item[arg]
        except KeyError:
            raise ValueError("{} is not a valid {}".format(arg, raw_type.__name__))


class DatetimeType(ProxyValue):
    """
    Augment datetime.datetime to support argument parsing.
    """

    type = datetime.datetime
    type_name = "datetime.datetime"

    @classmethod
    def parse_arg(cls, raw_type: Type, arg: str) -> datetime.datetime:
        return parse_date(arg)


# Get the type for python functions.
function_type = type(lambda: None)


def make_unknown_function(func_name: str) -> Callable:
    """
    Returns a stub function in place of a function that could not be reimported.
    """

    def unknown_function(*args, **kwargs):
        raise ValueError(f"Function '{func_name}' cannot be found.")

    # Mark this function as an unknown function.
    unknown_function.unknown_function = True  # type: ignore
    return unknown_function


def is_unknown_function(func: Callable) -> bool:
    """
    Returns True if the function was unknown when trying to reimport.
    """
    return getattr(func, "unknown_function", False)


class Function(ProxyValue):
    """
    Value class to allow redun to hash and cache plain Python functions.
    """

    type = function_type
    type_name = "builtins.function"

    def __init__(self, instance: Callable):
        super().__init__(instance)
        self.module = instance.__module__
        self.name = instance.__name__
        self._hash = self._calc_hash()

        if not is_unknown_function(instance) and "<locals>" in instance.__qualname__:
            raise InvalidValueError(
                "functions used as redun arguments or results must be defined globally."
            )

    def __repr__(self) -> str:
        return f"<function {self.fullname}>"

    @property
    def fullname(self) -> str:
        """
        Return a fullname for a function including its module.
        """
        return f"{self.module}.{self.name}"

    def _calc_hash(self) -> str:
        # Hash based on fully qualified function name and source code.
        hash = hash_struct(
            [
                "Value.function",
                self.fullname.encode("utf8"),
                "source",
                get_func_source(self.instance),
            ]
        )
        return hash

    def __getstate__(self) -> dict:
        return {
            "hash": self._hash,
            "module": self.module,
            "name": self.name,
        }

    def __setstate__(self, state: dict) -> None:
        self._hash = state["hash"]
        self.module = state["module"]
        self.name = state["name"]
        try:
            # Try to reassociate the function based on its name.
            module = importlib.import_module(self.module)
            self.instance = getattr(module, self.name)
        except (ModuleNotFoundError, AttributeError):
            self.instance = make_unknown_function(self.fullname)

    def get_hash(self, data: Optional[bytes] = None) -> str:
        return self._hash

    def is_valid(self) -> bool:
        """
        Value is valid to use from the cache if we can reassociate the function.
        """
        return not is_unknown_function(self.instance)

    def serialize(self) -> bytes:
        # Serialize the Function wrapper.
        return pickle_dumps(self)

    @classmethod
    def deserialize(self, raw_type: Any, data: bytes) -> Any:
        # Deserialize the Function wrapper and return the inner python function.
        func_wrapper = pickle_loads(data)
        return func_wrapper.instance

    @classmethod
    def parse_arg(cls, raw_type: Type, arg: str) -> Any:
        """
        Parses function by fully qualified name from command-line.
        """
        try:
            module_name, func_name = arg.rsplit(".", 1)
        except ValueError:
            raise ValueError(f"Unexpected format for function name: {arg}")

        try:
            module = importlib.import_module(module_name)
            func = getattr(module, func_name)
        except (ModuleNotFoundError, AttributeError):
            raise ValueError(f"Function not found: {arg}")

        return func


class FileCache(ProxyValue):
    """
    Baseclass for caching values to Files.

    This baseclass can be used to control where the serialized data of
    potentially large values is stored.

    .. code-block:: python

        # Example of user-specific data type.
        class Data:
            def __init__(self, data):
                self.data = data

        # Subclassing from FileCache and setting `type = Data` will register
        # with redun that File-caching should be used for any Value of type
        # `Data`.
        class DataType(FileCache):
            type = Data

            # This path specifies where the files containing serialized
            # values should be stored. This variable accepts all paths
            # acceptable to File (e.g. local, s3, etc).
            base_path = "tmp"

        @task()
        def task1() -> Data:
            # This value will be serialized to a File instead of the default
            # redun database.
            return Data("my data")
    """

    base_path = "."

    def _serialize(self) -> bytes:
        # User defined serialization.
        return pickle_dumps(self.instance)

    @classmethod
    def _deserialize(cls, data: bytes) -> Any:
        # User defined deserialization.
        return pickle_loads(data)

    def serialize(self) -> bytes:
        from redun.file import File

        # Serialize data.
        bytes = self._serialize()

        # Write (possibly large) data to file.
        filename = os.path.join(self.base_path, hash_bytes(bytes))
        File(filename).write(bytes, mode="wb")

        # Only cache filename.
        return filename.encode("utf8")

    @classmethod
    def deserialize(cls, raw_type: type, filename: bytes) -> Any:
        from redun.file import File

        # Read data from file.
        file = File(filename.decode("utf8"))
        if not file.exists():
            # File has since been delete. Treat as cache miss.
            raise InvalidValueError()

        data = cast(bytes, file.read("rb"))
        return cls._deserialize(data)
