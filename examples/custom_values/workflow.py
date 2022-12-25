import hashlib
from typing import Optional

from pydantic import BaseModel

from redun import task
from redun.value import ProxyValue

redun_namespace = "redun.examples.custom_values"


class User(BaseModel):
    id: int
    name = "Jane Doe"


class UserValue(ProxyValue):
    type = User
    type_name = "redun.examples.custom_values.User"

    def parse_arg(self, arg: str) -> User:
        # Custom parsing of JSON from the command line.
        return User.parse_raw(arg)

    def get_hash(self, data: Optional[bytes] = None) -> str:
        # Custom definition of hashing.
        message = hashlib.sha1()
        message.update(self.instance.json().encode("utf8"))
        return message.hexdigest()

    def get_serialization_format(self) -> str:
        # Document the serialization content-type.
        return "application/json"

    def serialize(self) -> bytes:
        # Custom serialization for caching.
        return self.instance.json().encode("utf8")

    def deserialize(self, type_name: str, data: bytes) -> User:
        # Custom deserialization from cache.
        return User.parse_raw(data)


@task()
def main(user: User) -> str:
    return f"Hello, {user.name}. Your id is {user.id}"
