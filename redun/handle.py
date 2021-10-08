from typing import Any, Optional, Tuple, Type

from redun.hashing import hash_struct
from redun.value import Value, get_type_registry


def get_handle_class(handle_class_name: str) -> Type["Handle"]:
    """
    Returns a Handle class from the TypeRegistry.
    """
    klass = get_type_registry().parse_type_name(handle_class_name)
    return klass


def get_fullname(namespace: Optional[str], name: str) -> str:
    """
    Constructs a fullname from a namespace and a name.
    """
    if namespace:
        return namespace + "." + name
    else:
        return name


class HandleInfo:
    """
    Handle state information that is stored under `handle.__handle__`.
    """

    def __init__(
        self,
        name: str,
        args: Tuple,
        kwargs: dict,
        class_name: str,
        namespace: Optional[str] = None,
        call_hash: str = "",
        hash: Optional[str] = None,
        key: str = "",
    ):
        self.name = name
        self.namespace = namespace
        self.fullname = get_fullname(self.namespace, self.name)
        self.args = args
        self.kwargs = kwargs
        self.class_name = class_name
        self.call_hash = call_hash
        self.key = key
        self.hash = hash or self.get_hash()

        # Cache of recent parent handles used by redun to aid recording.
        self.is_recorded = False  # TODO: Would be nice to move this to backend.
        self.fork_parent: Optional[Handle] = None

    def get_state(self) -> dict:
        """
        Returns serializable state dict.
        """
        return {
            "name": self.name,
            "namespace": self.namespace,
            "args": self.args,
            "kwargs": self.kwargs,
            "class_name": self.class_name,
            "call_hash": self.call_hash,
            "key": self.key,
            "hash": self.hash,
        }

    def get_hash(self) -> str:
        """
        Returns hash of the handle.
        """
        if self.call_hash:
            # Derived state from a call_node.
            return hash_struct(["Handle", self.fullname, "call_hash", self.key, self.call_hash])
        else:
            # Initial state.
            return hash_struct(["Handle", self.fullname, "init", self.key, self.args, self.kwargs])

    def update_hash(self) -> None:
        self.hash = self.get_hash()

    def apply_call(self, handle: "Handle", call_hash: str) -> "Handle":
        handle2 = self.clone(handle)
        handle2.__handle__.call_hash = call_hash
        handle2.__handle__.key = ""
        handle2.__handle__.update_hash()
        return handle2

    def fork(self, handle: "Handle", key: str) -> "Handle":
        handle2 = self.clone(handle)

        # Note: When forking, the previous Handle hash is used as the call_hash.
        handle2.__handle__.call_hash = handle.__handle__.hash
        handle2.__handle__.key = key
        handle2.__handle__.update_hash()
        handle2.__handle__.fork_parent = handle
        return handle2

    def clone(self, handle: "Handle") -> "Handle":
        # Create new handle instance.
        klass = get_handle_class(self.class_name)
        handle2 = klass.__new__(klass, self.name, *self.args, **self.kwargs)

        # Copy over attributes to new handle.
        ignore_attrs = {"__handle__"}
        for key, value in handle.__dict__.items():
            if key not in ignore_attrs:
                handle2.__dict__[key] = value

        return handle2


class Handle(Value):
    """
    A Value that accumulates state as it passes through Tasks.
    """

    type_name = "redun.Handle"

    def __new__(cls, name: Optional[str] = None, *args, **kwargs) -> "Handle":
        import redun.scheduler

        handle = super().__new__(cls)

        # Note: name is None when loading from a pickle.
        if name is not None:
            # Set HandleInfo within __new__ so that user can't forget.
            handle.__handle__ = HandleInfo(
                name=name,
                namespace=redun.scheduler.get_current_job_namespace(required=False),
                args=args,
                kwargs=kwargs,
                class_name=cls.type_name,
            )
        return handle

    def __init__(self, name: str, args: Tuple = (), kwargs: dict = {}):
        pass

    def __repr__(self) -> str:
        return "{class_name}(fullname={fullname}, hash={hash})".format(
            class_name=self.__handle__.class_name,
            fullname=self.__handle__.fullname,
            hash=(self.__handle__.hash or "")[:8],
        )

    def __getattr__(self, attr: str) -> Any:
        """
        Proxy attribute access to `self.instance` if it exists.
        """
        if "instance" in self.__dict__:
            return getattr(self.instance, attr)
        else:
            raise AttributeError(
                "'{}' object has no attribute '{}'".format(type(self).__name__, attr)
            )

    def __getstate__(self) -> dict:
        """
        Returns dict for serialization.
        """
        return self.__handle__.get_state()

    def __setstate__(self, state: dict) -> None:
        """
        Sets state from dict for deserialization.
        """
        self.__handle__ = HandleInfo(**state)
        self.__handle__.is_recorded = True
        self.__init__(state["name"], *state["args"], **state["kwargs"])  # type: ignore

    def apply_call(self, call_hash: str) -> "Handle":
        """
        Returns a new Handle derived from this one assumin passage through a call with call_hash.
        """
        return self.__handle__.apply_call(self, call_hash)

    def fork(self, key: str) -> "Handle":
        """
        Forks the handle into a second one for use in parallel tasks.
        """
        return self.__handle__.fork(self, key)

    def is_valid(self) -> bool:
        """
        Returns True if handle is still valid (has not been rolled back).
        """
        from redun.scheduler import get_current_scheduler

        if self.type_name != self.__handle__.class_name:
            # Handle class_name might be out of date from deserialization.
            return False

        scheduler = get_current_scheduler()
        assert scheduler
        return scheduler.backend.is_valid_handle(self)

    def get_hash(self, data: Optional[bytes] = None) -> str:
        """
        Returns a hash of the handle.
        """
        return self.__handle__.hash

    def preprocess(self, preprocess_args: dict) -> "Handle":
        """
        Forks a handle as it passes into a task.
        """
        call_order = preprocess_args["call_order"]
        return self.fork(self.__handle__.key or str(call_order))

    def postprocess(self, postprocess_args: dict) -> "Handle":
        """
        Applies the call_hash to the handle as it returns from a task.
        """
        return self.apply_call(postprocess_args["pre_call_hash"])


def create_handle(state: dict) -> Handle:
    """
    Returns a new Handle created from a state dict.
    """
    handle_class = get_handle_class(state["class_name"])
    handle = handle_class.__new__(handle_class, state["name"], *state["args"], **state["kwargs"])
    handle.__init__(state["name"], *state["args"], **state["kwargs"])  # type: ignore
    return handle
