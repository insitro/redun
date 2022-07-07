import os
from typing import Tuple

from redun.file import File


class ValueStore:
    """
    Stores redun Values on filesystem (local or remote).

    The value store can be configured within the `[backend]` section using
    the `value_store_path` and `value_store_min_size` variables.

    Values in this store are organized similarly to git objects:

    <value_store_path>/ab/cdef... for value with hash abcdef...

    """

    def __init__(self, root_path: str, use_subdir: bool = True):
        self.root_path = root_path
        self.use_subdir = use_subdir

    def get_value_path(self, value_hash: str) -> str:
        """
        Return the path for a value.
        """
        if self.use_subdir:
            suffix = value_hash[:2] + "/" + value_hash[2:]
        else:
            suffix = value_hash

        return os.path.join(self.root_path, suffix)

    def has(self, value_hash: str) -> bool:
        """
        Return True if value is in store.
        """
        return File(self.get_value_path(value_hash)).exists()

    def put(self, value_hash: str, data: bytes) -> None:
        """
        Store Value data.
        """
        if self.has(value_hash):
            return

        with File(self.get_value_path(value_hash)).open("wb") as out:
            out.write(data)

    def get(self, value_hash: str) -> Tuple[bytes, bool]:
        """
        Retrieve Value data.
        """
        file = File(self.get_value_path(value_hash))
        try:
            with file.open("rb") as infile:
                return infile.read(), True
        except FileNotFoundError:
            return b"", False

    def size(self, value_hash: str) -> int:
        """
        Returns the size in bytes of a Value.
        """
        try:
            return File(self.get_value_path(value_hash)).size()
        except FileNotFoundError:
            return -1
