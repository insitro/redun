import hashlib
import typing
from typing import IO, Any, Dict, List, Sequence, Tuple

from redun.bcoding import bencode
from redun.utils import json_dumps

if typing.TYPE_CHECKING:
    from redun.value import TypeRegistry


class Hash:
    """
    A convenience class for creating hashes.
    """

    def __init__(self, length=40):
        self.message = hashlib.sha512()
        self.length = length

    def update(self, data):
        self.message.update(data)

    def hexdigest(self) -> str:
        return self.message.hexdigest()[: self.length]


def hash_struct(struct: Any) -> str:
    """
    Hash a structure by using canonical serialization using bencode.
    """
    m = Hash()
    m.update(bencode(struct))
    return m.hexdigest()


def hash_bytes(bytes: bytes) -> str:
    """
    Hash a byte sequence.
    """
    m = Hash()
    m.update(bytes)
    return m.hexdigest()


def hash_tag_bytes(tag: str, bytes: bytes) -> str:
    """
    Hash a tag followed by a byte sequence.
    """
    m = Hash()
    m.update(bencode([tag]))
    m.update(bytes)
    return m.hexdigest()


def hash_stream(stream: IO, block_size: int = 1024) -> str:
    """
    Hash a stream of bytes.
    """
    m = Hash()
    while True:
        block = stream.read(block_size)
        if not block:
            # Zero bytes indicates the end of the stream.
            break
        m.update(block)
    return m.hexdigest()


def hash_text(text: str) -> str:
    """
    Returns the hash for a string.
    """
    m = Hash()
    m.update(text.encode("utf-8"))
    return m.hexdigest()


def hash_positional_args(type_registry: "TypeRegistry", args: Sequence) -> List[str]:
    """
    Hash a list of arguments.
    """
    return [type_registry.get_hash(arg) for arg in args]


def hash_kwargs(type_registry: "TypeRegistry", kwargs: Dict[str, Any]) -> Dict[str, str]:
    """
    Hash a list of arguments.
    """
    return {key: type_registry.get_hash(arg) for key, arg in kwargs.items()}


def hash_arguments(type_registry: "TypeRegistry", args: Sequence, kwargs: dict):
    """
    Hash the arguments for a Task call.
    """
    return hash_struct(
        [
            "TaskArguments",
            hash_positional_args(type_registry, args),
            hash_kwargs(type_registry, kwargs),
        ]
    )


def hash_eval(
    type_registry: "TypeRegistry", task_hash: str, args: Sequence, kwargs: dict
) -> Tuple[str, str]:
    """
    Hash Task evaluation and arguments.
    """
    args_hash = hash_arguments(type_registry, args, kwargs)
    return hash_struct(["Eval", task_hash, args_hash]), args_hash


def hash_tag(entity_id: str, key: str, value: Any, parents: List[str]) -> str:
    """
    Hash a CallGraph Tag.
    """
    return hash_struct(["Tag", entity_id, key, json_dumps(value), parents])


def hash_call_node(
    task_hash: str, args_hash: str, result_hash: str, child_call_hashes: List[str]
) -> str:
    """
    Calculates the call_hash for a CallNode.
    """
    return hash_struct(["CallNode", task_hash, args_hash, result_hash, sorted(child_call_hashes)])
