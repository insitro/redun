import json
import os
import sys
import typing
import uuid
import warnings
from collections import defaultdict
from copy import copy as shallowcopy
from datetime import datetime, timedelta
from itertools import chain
from typing import (
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    NamedTuple,
    Optional,
    Set,
    Tuple,
    Union,
    cast,
)
from urllib.parse import quote_plus, urlparse, urlunparse

import sqlalchemy as sa
from alembic.command import downgrade, upgrade
from alembic.config import Config as AConfig
from sqlalchemy import Boolean
from sqlalchemy import Column as BaseColumn
from sqlalchemy import (
    DateTime,
    Enum,
    ForeignKey,
    Integer,
    LargeBinary,
    String,
    and_,
    create_engine,
    inspect,
    or_,
    select,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Session, backref, declarative_base, relationship, sessionmaker
from sqlalchemy.orm.session import object_session
from sqlalchemy.schema import Index
from sqlalchemy.sql import exists
from sqlalchemy.sql.expression import cast as sa_cast
from sqlalchemy.types import TypeDecorator

from redun.backends.base import KeyValue, RedunBackend, TagEntity, TagMap
from redun.backends.db import serializers
from redun.backends.value_store import ValueStore
from redun.config import Section, create_config_section
from redun.db_utils import filter_in, get_or_create, query_filter_in
from redun.expression import AnyExpression, Expression, SchedulerExpression, TaskExpression
from redun.file import File as BaseFile
from redun.file import get_proto
from redun.handle import Handle as BaseHandle
from redun.hashing import hash_call_node, hash_struct, hash_tag
from redun.logging import logger as _logger
from redun.task import CacheCheckValid, CacheResult, CacheScope
from redun.task import Task as BaseTask
from redun.utils import (
    MultiMap,
    iter_nested_value,
    json_dumps,
    pickle_loads,
    pickle_preview,
    str2bool,
    trim_string,
    with_pickle_preview,
)
from redun.value import MIME_TYPE_PICKLE, InvalidValueError
from redun.value import TypeError as RedunTypeError

if typing.TYPE_CHECKING:
    from redun.scheduler import Job as BaseJob


NULL = object()
DEFAULT_DB_URI = "sqlite:///.redun/redun.db"
DEFAULT_DB_USERNAME_ENV = "REDUN_DB_USERNAME"
DEFAULT_DB_PASSWORD_ENV = "REDUN_DB_PASSWORD"
DEFAULT_MAX_VALUE_SIZE = 1000000000
DEFAULT_VALUE_STORE_MIN_SIZE = 1024
REDUN_DB_UNKNOWN_VERSION = 99
MAX_VALUE_SIZE_PREVIEW = 1000000

SA_DIALECT_POSTGRESQL = "postgresql"


class DBVersionInfo(NamedTuple):
    """
    A redun repo database version and migration id.
    """

    migration_id: str
    major: int
    minor: int
    description: str

    def __lt__(self, other: tuple) -> bool:
        if not isinstance(other, DBVersionInfo):
            raise TypeError(f"Expected DBVersionInfo: {other}")
        return (self.major, self.minor) < (other.major, other.minor)

    def __le__(self, other: tuple) -> bool:
        if not isinstance(other, DBVersionInfo):
            raise TypeError(f"Expected DBVersionInfo: {other}")
        return (self.major, self.minor) <= (other.major, other.minor)

    def __gt__(self, other: tuple) -> bool:
        if not isinstance(other, DBVersionInfo):
            raise TypeError(f"Expected DBVersionInfo: {other}")
        return (self.major, self.minor) > (other.major, other.minor)

    def __ge__(self, other: tuple) -> bool:
        if not isinstance(other, DBVersionInfo):
            raise TypeError(f"Expected DBVersionInfo: {other}")
        return (self.major, self.minor) >= (other.major, other.minor)

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, DBVersionInfo):
            raise TypeError(f"Expected DBVersionInfo: {other}")
        return (self.major, self.minor) == (other.major, other.minor)

    def __str__(self) -> str:
        if self.minor == REDUN_DB_UNKNOWN_VERSION:
            return f"{self.major}.?"
        else:
            return f"{self.major}.{self.minor}"


# List of all available redun repo database versions and migrations.
# Note these are sorted from oldest to newest.
null_db_version = DBVersionInfo("000000000000", -1, 0, "No schema present.")
REDUN_DB_VERSIONS = [
    DBVersionInfo("806f5dcb11bf", 1, 0, "Prototype schema."),
    DBVersionInfo("647c510a77b1", 2, 0, "Initial production schema."),
    DBVersionInfo("30ffbaee18cd", 2, 1, "Backfill companion values for tasks"),
    DBVersionInfo("71ec303c90e4", 2, 2, "Add indexes for id prefix search"),
    DBVersionInfo("d4af139b6f53", 2, 3, "Add job.execution_id"),
    DBVersionInfo("cd2d53191748", 3, 0, "Make job.execution_id non-nullable"),
    DBVersionInfo("cc4f663817b6", 3, 1, "Add Tag schemas."),
    DBVersionInfo("eb7b95e4e8bf", 3, 2, "Remove length restriction on value type names."),
]
REDUN_DB_MIN_VERSION = DBVersionInfo("", 3, 1, "")  # Min db version needed by redun library.
REDUN_DB_MAX_VERSION = DBVersionInfo("", 3, 99, "")  # Max db version needed by redun library.


def parse_db_version(version_str: str) -> DBVersionInfo:
    """
    Parses a db version string such as "2.0" or "3" into a DbVersionInfo.
    """
    if version_str == "latest":
        return REDUN_DB_VERSIONS[-1]

    dots = version_str.count(".")
    if dots == 0:
        major, minor = (int(version_str), 0)
    elif dots == 1:
        major, minor = tuple(map(int, version_str.split(".")))
    else:
        raise ValueError(f"Invalid db version format: {version_str}")

    for version in REDUN_DB_VERSIONS:
        if version.major == major and version.minor == minor:
            return version

    raise RedunVersionError(f"Unknown db version: {version_str}")


HASH_LEN = 40

Base: Any = declarative_base()
Engine = Any


class RedunDatabaseError(Exception):
    pass


class RedunVersionError(RedunDatabaseError):
    pass


class Column(BaseColumn):
    """
    Use non-null Columns by default.
    """

    inherit_cache = True

    def __init__(self, *args, **kwargs):
        kwargs["nullable"] = kwargs.get("nullable", False)
        super().__init__(*args, **kwargs)


class JSON(TypeDecorator):
    """
    This custom column type allows use of JSON across both sqlite and postgres.

    In potsgres, the column acts like a JSONB column. In sqlite, it acts like
    a string with normalization (e.g. sorted keys, etc) applied to the JSON
    serialization. This allows us to do exact matching indexing across both
    sqlite and postgres.
    """

    cache_ok = True
    impl = String
    _string = String()
    _jsonb = JSONB()

    def load_dialect_impl(self, dialect):
        # Use a different column implementation depending on the database.
        if dialect.name == SA_DIALECT_POSTGRESQL:
            return self._jsonb
        else:
            return self._string

    def process_bind_param(self, value: Any, dialect):
        if dialect.name == SA_DIALECT_POSTGRESQL:
            # No additional processing is needed for postgres.
            return value
        else:
            # Serialize the value with normalization.
            return json_dumps(value)

    def process_result_value(self, value: Any, dialect):
        if dialect.name == SA_DIALECT_POSTGRESQL:
            # No additional processing is needed for postgres.
            return value
        else:
            return json.loads(value)


class RedunVersion(Base):
    """
    Version of redun database.
    """

    __tablename__ = "redun_version"

    id = Column(String, default=lambda: str(uuid.uuid4()), primary_key=True)
    version = Column(Integer, comment="Current database version.")
    timestamp = Column(DateTime, default=datetime.utcnow)


class RedunMigration(Base):
    """
    Migration history of redun database (aka alembic table).
    """

    __tablename__ = "alembic_version"

    version_num = Column(String(32), nullable=False, primary_key=True)


class Subvalue(Base):
    """
    A Value that is a subvalue of another Value.
    """

    __tablename__ = "subvalue"

    value_hash = Column(
        String(HASH_LEN), ForeignKey("value.value_hash"), primary_key=True, index=True
    )
    parent_value_hash = Column(
        String(HASH_LEN), ForeignKey("value.value_hash"), primary_key=True, index=True
    )

    parent = relationship("Value", foreign_keys=[parent_value_hash], backref="child_edges")
    child = relationship("Value", foreign_keys=[value_hash], backref="parent_edges")


class Value(Base):
    """
    A value used as input (Argument) or output (Result) from a Task.
    """

    __tablename__ = "value"

    value_hash = Column(String(HASH_LEN), primary_key=True)
    type = Column(String(length=None))
    format = Column(String(100))
    value = Column(LargeBinary())

    value_hash_idx = Index(
        "ix_value_value_hash_vpo",
        value_hash,
        unique=True,
        postgresql_ops={
            "value_hash": "varchar_pattern_ops",
        },
    )

    children = relationship(
        "Value",
        secondary=Subvalue.__table__,
        primaryjoin=(value_hash == Subvalue.parent_value_hash),
        secondaryjoin=(value_hash == Subvalue.value_hash),
        backref="parents",
        viewonly=True,
        sync_backref=False,
    )
    tags = relationship(
        "Tag",
        foreign_keys=[value_hash],
        primaryjoin="(Value.value_hash == Tag.entity_id) & (Tag.is_current == True)",
        backref="values",
        uselist=True,
    )

    @property
    def preview(self) -> Any:
        """
        Returns a deserialized value, or a preview if there is an error or value is too large.
        """
        backend = cast(RedunSession, object_session(self)).backend
        size = backend._get_value_size(self)

        if size > MAX_VALUE_SIZE_PREVIEW:
            # Use a preview if the value is too large to load efficiently.
            return PreviewValue(self, size)
        else:
            with with_pickle_preview():
                data, has_value = backend._get_value_data(self)
                if not has_value:
                    # Data is missing from value store, just show preview.
                    warnings.warn(
                        "Value data is missing from the value store. "
                        "Does `value_store_path` need to be configured?"
                    )
                    return PreviewValue(self, size)

                value, has_value = backend._deserialize_value(self.type, data)
                if has_value:
                    return value
                elif self.format == MIME_TYPE_PICKLE:
                    # Fallback to direct pickle preview loading.
                    return pickle_loads(data)
                else:
                    # If deserialization failed and we don't know the format,
                    # default to just a preview.
                    return PreviewValue(self, size)

    @property
    def value_parsed(self) -> Optional[Any]:
        """
        Returns a deserialized value, or a preview if there is an error.
        """
        backend = cast(RedunSession, object_session(self)).backend
        with with_pickle_preview():
            value, _ = backend._get_value(self)
            return value

    @property
    def in_value_store(self) -> bool:
        """
        Returns True if value data is in a ValueStore.
        """
        # We use a zero-length byte string in the db to denote that value data
        # is in a ValueStore.
        return len(self.value) == 0

    def __repr__(self) -> str:
        return "Value(hash='{value_hash}', value={value})".format(
            value_hash=self.value_hash[:8],
            value=trim_string(repr(self.value_parsed)),
        )


class PreviewValue:
    """
    A preview value if the value is too large or if there is an error.
    """

    def __init__(self, value: Value, size: int):
        self.value = value
        self.size = size

    def __repr__(self) -> str:
        return f"{self.value.type}(hash={self.value.value_hash[:8]}, size={self.size})"


class File(Base):
    """
    A File used as a Value by a Task.
    """

    __tablename__ = "file"

    value_hash = Column(String(HASH_LEN), ForeignKey("value.value_hash"), primary_key=True)
    path = Column(String(1024), index=True)

    value = relationship(
        "Value", foreign_keys=[value_hash], backref=backref("file", uselist=False)
    )
    parent_values = relationship(
        "Value",
        secondary=Subvalue.__table__,
        primaryjoin=(value_hash == Subvalue.value_hash),
        secondaryjoin=(Subvalue.parent_value_hash == Value.value_hash),
        uselist=True,
        backref=backref("subfiles", uselist=True),
        viewonly=True,
        sync_backref=False,
    )

    def __repr__(self) -> str:
        return "File(hash='{value_hash}', path='{path}')".format(
            value_hash=self.value_hash[:8],
            path=trim_string(self.path),
        )

    @property
    def values(self) -> List[Value]:
        values = []
        if self.value:
            values.append(self.value)
        values.extend(self.parent_values)
        return values


class ArgumentResult(Base):
    """
    Many-to-many relationship between results and Arguments.
    """

    __tablename__ = "argument_result"

    arg_hash = Column(
        String(HASH_LEN), ForeignKey("argument.arg_hash"), primary_key=True, index=True
    )
    result_call_hash = Column(
        String(HASH_LEN), ForeignKey("call_node.call_hash"), primary_key=True, index=True
    )

    arg = relationship("Argument", foreign_keys=[arg_hash], backref="arg_results")
    result_call_node = relationship(
        "CallNode", foreign_keys=[result_call_hash], backref="arg_results"
    )


class Argument(Base):
    """
    Input value for a called Task.
    """

    __tablename__ = "argument"

    arg_hash = Column(String(HASH_LEN), primary_key=True)
    call_hash = Column(String(HASH_LEN), ForeignKey("call_node.call_hash"), index=True)
    value_hash = Column(String(HASH_LEN), ForeignKey("value.value_hash"), index=True)
    arg_position = Column(Integer, nullable=True)
    arg_key = Column(String(100), nullable=True)

    value = relationship("Value", foreign_keys=[value_hash], backref="arguments")
    call_node = relationship("CallNode", foreign_keys=[call_hash], backref="arguments")
    upstream = relationship(
        "CallNode",
        secondary=ArgumentResult.__table__,
        primaryjoin=(arg_hash == ArgumentResult.arg_hash),
        secondaryjoin=(
            lambda: ArgumentResult.result_call_hash == CallNode.call_hash  # type: ignore
        ),
        backref="downstream",
        viewonly=True,
        sync_backref=False,
    )

    @property
    def value_parsed(self) -> Optional[Any]:
        return self.value.value_parsed

    def __repr__(self) -> str:
        return "Argument(task_name='{task_name}', pos={pos_or_key}, value={value})".format(
            task_name=self.call_node.task.fullname,
            pos_or_key=(self.arg_key if self.arg_key else self.arg_position),
            value=trim_string(str(self.value.value_parsed)),
        )


class Evaluation(Base):
    """
    Cache table for evaluations.

    eval_hash (hash of task_hash and args_hash) is the cache key, and
    value_hash is the cached value.
    """

    __tablename__ = "evaluation"

    eval_hash = Column(String(HASH_LEN), primary_key=True)
    task_hash = Column(String(HASH_LEN), ForeignKey("task.hash"), index=True)
    args_hash = Column(String(HASH_LEN))
    value_hash = Column(String(HASH_LEN), ForeignKey("value.value_hash"), index=True)

    task = relationship("Task", uselist=False, foreign_keys=[task_hash], backref="evals")
    value = relationship("Value", foreign_keys=[value_hash], backref="evals")

    @property
    def value_parsed(self) -> Optional[Any]:
        return self.value.value_parsed


class CallEdge(Base):
    """
    An edge in the CallGraph.

    This is a many-to-many table for CallNode.
    """

    __tablename__ = "call_edge"

    parent_id = Column(
        String(HASH_LEN), ForeignKey("call_node.call_hash"), primary_key=True, index=True
    )
    child_id = Column(
        String(HASH_LEN), ForeignKey("call_node.call_hash"), primary_key=True, index=True
    )
    call_order = Column(Integer, primary_key=True)

    parent_node = relationship("CallNode", foreign_keys=[parent_id], backref="child_edges")
    child_node = relationship("CallNode", foreign_keys=[child_id], backref="parent_edges")


class CallNode(Base):
    """
    A CallNode in the CallGraph.
    """

    __tablename__ = "call_node"

    call_hash = Column(String(HASH_LEN), primary_key=True)
    task_name = Column(String(1024))
    task_hash = Column(String(HASH_LEN), ForeignKey("task.hash"), index=True)
    args_hash = Column(String(HASH_LEN))

    # TODO later:
    # eval_hash = Column(String(HASH_LEN), ForeignKey('evaluation.eval_hash'), index=True, nullable=True)  # noqa: E501

    value_hash = Column(String(HASH_LEN), ForeignKey("value.value_hash"), index=True)
    timestamp = Column(DateTime, default=datetime.utcnow)

    call_hash_idx = Index(
        "ix_call_node_call_hash_vpo",
        call_hash,
        unique=True,
        postgresql_ops={
            "call_hash": "varchar_pattern_ops",
        },
    )

    task = relationship("Task", uselist=False, foreign_keys=[task_hash], backref="call_nodes")
    value = relationship("Value", uselist=False, foreign_keys=[value_hash], backref="results")
    task_set = relationship(
        "CallSubtreeTask",
        primaryjoin=(lambda: CallNode.call_hash == CallSubtreeTask.call_hash),  # type: ignore
        foreign_keys=[call_hash],
        backref="call_node",
    )
    tags = relationship(
        "Tag",
        foreign_keys=[call_hash],
        primaryjoin="(CallNode.call_hash == Tag.entity_id) & (Tag.is_current == True)",
        backref="call_nodes",
        viewonly=True,
        uselist=True,
    )

    def __repr__(self) -> str:
        return "CallNode(hash='{call_hash}', task_name='{task_name}', args={args})".format(
            call_hash=self.call_hash[:8],
            task_name=self.task.fullname if self.task else "",
            args=self.args_display,
        )

    @property
    def value_parsed(self) -> Optional[Any]:
        return self.value.value_parsed

    # NOTE: Ideally we could use relationships for children and parents, instead
    # of properties. However, we can have repeat children and parents, and
    # distinct_target_key=False doesn't seem to help prevent a distinct
    # behavior in the SQL query. Perhaps there is still a missing piece in the
    # strategy below.
    #
    # children = relationship(
    #     "CallNode",
    #     secondary=CallEdge.__table__,
    #     primaryjoin=(call_hash == CallEdge.parent_id),
    #     secondaryjoin=(call_hash == CallEdge.child_id),
    #     backref="parents",
    #     order_by=CallEdge.call_order,
    #     distinct_target_key=False,  # Must handle repeated calls to same CallNode.
    # )

    @property
    def children(self) -> List["CallNode"]:
        if sa.__version__.startswith("1.3."):
            child_nodes = (
                object_session(self)
                .query(CallNode)
                .join(CallEdge, CallEdge.child_id == CallNode.call_hash)
                .filter(CallEdge.parent_id == self.call_hash)
            )
            # https://github.com/sqlalchemy/sqlalchemy/issues/4395
            child_nodes._has_mapper_entities = False
            return child_nodes.all()

        else:
            # New style available in sqlalchemy>=1.4.0
            # https://docs.sqlalchemy.org/en/14/changelog/migration_20.html#joinedload-not-uniqued
            child_nodes = (
                object_session(self)
                .execute(
                    select(CallNode, CallEdge.call_order)
                    .join(CallEdge, CallEdge.child_id == CallNode.call_hash)
                    .filter(CallEdge.parent_id == self.call_hash)
                    .order_by(CallEdge.call_order)
                )
                .columns(CallNode)
            )
            return [node for (node,) in child_nodes]

    @property
    def parents(self) -> List["CallNode"]:
        parent_nodes = (
            object_session(self)
            .execute(
                select(CallNode, CallEdge.call_order)
                .join(CallEdge, CallEdge.parent_id == CallNode.call_hash)
                .filter(CallEdge.child_id == self.call_hash)
                .order_by(CallEdge.call_order)
            )
            .columns(CallNode)
        )
        return [node for (node,) in parent_nodes]

    @property
    def args_display(self) -> str:
        return trim_string(
            repr([arg.value_parsed for arg in self.arguments]), max_length=100, ellipsis="...]"
        )


class CallSubtreeTask(Base):
    __tablename__ = "call_subtree_task"

    call_hash = Column(
        String(HASH_LEN), ForeignKey("call_node.call_hash"), primary_key=True, index=True
    )
    task_hash = Column(String(HASH_LEN), ForeignKey("task.hash"), primary_key=True, index=True)


class HandleEdge(Base):
    __tablename__ = "handle_edge"

    parent_id = Column(String(HASH_LEN), ForeignKey("handle.hash"), primary_key=True, index=True)
    child_id = Column(String(HASH_LEN), ForeignKey("handle.hash"), primary_key=True, index=True)


class Handle(Base):
    __tablename__ = "handle"

    hash = Column(String(HASH_LEN), primary_key=True)
    fullname = Column(String(1024), index=True)
    value_hash = Column(String(HASH_LEN), ForeignKey("value.value_hash"), index=True)
    key = Column(String(1024))
    is_valid = Column(Boolean, default=True)

    value = relationship(
        "Value", foreign_keys=[value_hash], backref=backref("handle", uselist=False), uselist=False
    )
    parent_values = relationship(
        "Value",
        secondary=Subvalue.__table__,
        primaryjoin=(value_hash == Subvalue.value_hash),
        secondaryjoin=(Subvalue.parent_value_hash == Value.value_hash),
        uselist=True,
        backref=backref("subhandles", uselist=True),
        viewonly=True,
        sync_backref=False,
    )
    children = relationship(
        "Handle",
        secondary=HandleEdge.__table__,
        primaryjoin=(hash == HandleEdge.parent_id),
        secondaryjoin=(hash == HandleEdge.child_id),
        backref="parents",
        viewonly=True,
        sync_backref=False,
    )

    def __repr__(self) -> str:
        return "Handle(hash='{hash}', name='{fullname}')".format(
            hash=self.hash[:8], fullname=self.fullname
        )

    @property
    def values(self) -> List[Value]:
        values = []
        if self.value:
            values.append(self.value)
        values.extend(self.parent_values)
        return values


class Execution(Base):
    __tablename__ = "execution"

    id = Column(String, primary_key=True)
    args = Column(String)
    job_id = Column(
        String, ForeignKey("job.id", deferrable=True, initially="deferred"), index=True
    )

    id_idx = Index(
        "ix_execution_id_vpo",
        id,
        unique=True,
        postgresql_ops={
            "id": "varchar_pattern_ops",
        },
    )

    job = relationship("Job", foreign_keys=[job_id], uselist=False)
    tags = relationship(
        "Tag",
        foreign_keys=[id],
        primaryjoin="(Execution.id == Tag.entity_id) & (Tag.is_current == True)",
        backref="executions",
        uselist=True,
    )

    def __repr__(self):
        return "Execution(id='{id}', task_name='{task_name}', args={args})".format(
            id=self.id[:8],
            args=self.args,
            task_name=self.task.fullname if self.task else "None",
        )

    @property
    def call_node(self) -> Optional["CallNode"]:
        if not self.job:
            return None
        return self.job.call_node

    @property
    def task(self) -> Optional["Task"]:
        if not self.job:
            return None
        if not self.job.call_node:
            return None
        return self.job.call_node.task

    @property
    def status(self) -> str:
        if self.job and self.job.status in {"DONE", "CACHED"}:
            return "DONE"
        else:
            return "FAILED"


class Job(Base):
    __tablename__ = "job"

    id = Column(String, primary_key=True)
    start_time = Column(DateTime)
    end_time = Column(DateTime, nullable=True)
    task_hash = Column(String(HASH_LEN), ForeignKey("task.hash"), index=True)
    cached = Column(Boolean, default=False)
    call_hash = Column(
        String(HASH_LEN), ForeignKey("call_node.call_hash"), nullable=True, index=True
    )
    parent_id = Column(String, ForeignKey("job.id"), nullable=True, index=True)
    execution_id = Column(
        String, ForeignKey("execution.id", deferrable=True, initially="deferred"), index=True
    )

    id_idx = Index(
        "ix_job_id_vpo",
        id,
        unique=True,
        postgresql_ops={
            "id": "varchar_pattern_ops",
        },
    )

    task = relationship("Task", uselist=False, backref="jobs")
    call_node = relationship("CallNode", uselist=False, backref="jobs")
    child_jobs = relationship(
        "Job", backref=backref("parent_job", remote_side=id), order_by="Job.start_time"
    )
    execution = relationship(
        "Execution", foreign_keys=[execution_id], uselist=False, backref="jobs"
    )
    tags = relationship(
        "Tag",
        foreign_keys=[id],
        primaryjoin="(Job.id == Tag.entity_id) & (Tag.is_current == True)",
        backref="jobs",
        uselist=True,
    )

    def __repr__(self) -> str:
        return "Job(id='{id}', start_time='{start_time}', task_name={task_name})".format(
            id=self.id[:8],
            start_time=self.start_time.strftime("%Y-%m-%d %H:%M:%S"),
            task_name=repr(self.task.fullname if self.task else "None"),
        )

    @property
    def duration(self) -> Optional[timedelta]:
        """
        Returns duration of the Job or None if Job end_time is not recorded.
        """
        if not self.end_time:
            return None
        return self.end_time - self.start_time

    @property
    def status(self) -> str:
        """
        Returns Job status (DONE, CACHED, FAILED, RUNNING).

        Currently, we denote a FAILED Job by not recording its end_time.
        In the future, we will likely record an explicit status so that we can
        record end_times for FAILED Jobs as well.
        """
        if not self.end_time:
            return "FAILED"
        elif self.cached:
            return "CACHED"
        else:
            return "DONE"


class Task(Base):
    __tablename__ = "task"

    hash = Column(String(HASH_LEN), primary_key=True)
    name = Column(String)
    namespace = Column(String)
    source = Column(String)

    hash_idx = Index(
        "ix_task_hash_vpo",
        hash,
        unique=True,
        postgresql_ops={
            "hash": "varchar_pattern_ops",
        },
    )
    name_idx = Index(
        "ix_task_name_vpo",
        name,
        postgresql_ops={
            "name": "varchar_pattern_ops",
        },
    )
    namespace_idx = Index(
        "ix_task_namespace_vpo",
        namespace,
        postgresql_ops={
            "namespace": "varchar_pattern_ops",
        },
    )

    value = relationship(
        "Value",
        foreign_keys=[Value.value_hash],
        primaryjoin=(hash == Value.value_hash),
        backref=backref("task", uselist=False),
        uselist=False,
        viewonly=True,
    )
    tags = relationship(
        "Tag",
        foreign_keys=[hash],
        primaryjoin="(Task.hash == Tag.entity_id) & (Tag.is_current == True)",
        backref="tasks",
        uselist=True,
    )

    @property
    def fullname(self) -> str:
        if self.namespace:
            return self.namespace + "." + self.name
        else:
            return self.name

    def __repr__(self) -> str:
        return f"Task(hash='{self.hash[:8]}', name='{self.fullname}')"

    def show_source(self) -> None:
        print(self.source)


class TagEdit(Base):
    __tablename__ = "tag_edit"

    parent_id = Column(String, ForeignKey("tag.tag_hash"), primary_key=True)
    child_id = Column(String, ForeignKey("tag.tag_hash"), primary_key=True)

    parent = relationship("Tag", foreign_keys=[parent_id], backref="child_edits", viewonly=True)
    child = relationship("Tag", foreign_keys=[child_id], backref="parent_edits", viewonly=True)


class Tag(Base):
    __tablename__ = "tag"

    tag_hash = Column(String(HASH_LEN), primary_key=True)
    entity_type = Column(Enum(TagEntity))
    entity_id = Column(String, index=True)
    key = Column(String, index=True)
    value = Column(JSON, index=True)
    is_current = Column(Boolean, default=True)

    # Partial index allows finding the current Tags faster.
    tag_hash_idx = Index(
        "ix_tag_tag_hash_current",
        tag_hash,
        unique=True,
        postgresql_where=is_current,
        sqlite_where=is_current,
    )

    parents = relationship(
        "Tag",
        secondary=TagEdit.__table__,
        primaryjoin="Tag.tag_hash == TagEdit.child_id",
        secondaryjoin="Tag.tag_hash == TagEdit.parent_id",
        backref="children",
    )

    def __repr__(self) -> str:
        return "Tag(tag_hash={tag_hash}, entity_id={entity_id}, key={key}, value={value})".format(
            tag_hash=self.tag_hash[:8],
            entity_id=self.entity_id[:8],
            key=self.key,
            value=json.dumps(self.value, sort_keys=True),
        )

    @property
    def entity(self) -> Union[Execution, Job, CallNode, Task, Value]:
        session = object_session(self)
        if self.entity_type == TagEntity.Execution:
            return session.query(Execution).filter_by(id=self.entity_id).one()

        elif self.entity_type == TagEntity.Job:
            return session.query(Job).filter_by(id=self.entity_id).one()

        elif self.entity_type == TagEntity.CallNode:
            return session.query(CallNode).filter_by(call_hash=self.entity_id).one()

        elif self.entity_type == TagEntity.Task:
            return session.query(Task).filter_by(hash=self.entity_id).one()

        elif self.entity_type == TagEntity.Value:
            return session.query(Value).filter_by(value_hash=self.entity_id).one()

        else:
            raise AssertionError(f"Invalid entity_type {self.entity_type}")

    @property
    def base_key(self) -> str:
        return self.key.rsplit(".", 1)[-1]

    @property
    def namespace(self) -> str:
        if "." in self.key:
            return self.key.rsplit(".", 1)[0]
        else:
            return ""

    @staticmethod
    def get_delete_tag() -> "Tag":
        """
        Returns a delete tag, which can be used to mark parent tags deleted.
        """
        return Tag(
            tag_hash="",  # Hash should be computed by caller.
            entity_type=TagEntity.Null,
            entity_id="",
            key="",
            value=None,
        )


#
# Methods for walking the database by ownership edges. Used for database syncing.
#

RecordEdgeType = Tuple[str, Base, str]


def get_execution_child_edges(session: Session, ids: Iterable[str]) -> Iterable[RecordEdgeType]:
    # Get Execution child ids.
    for (job_id,) in filter_in(session.query(Execution.job_id), Execution.id, ids):
        yield "Execution.job", Job, job_id


def get_job_child_edges(session: Session, ids: Iterable[str]) -> Iterable[RecordEdgeType]:
    # Get Job child Task and CallNode ids.
    for (task_hash, call_hash) in filter_in(
        session.query(Job.task_hash, Job.call_hash), Job.id, ids
    ):
        yield "Job.task", Task, task_hash
        yield "Job.call_hash", CallNode, call_hash

    # Get Job child Jobs.
    for (job_id,) in filter_in(session.query(Job.id), Job.parent_id, ids):
        yield "Job.child_job", Job, job_id


def get_call_node_child_edges(session: Session, ids: Iterable[str]) -> Iterable[RecordEdgeType]:
    # Get CallNode task and result ids.
    for (task_hash, value_hash) in filter_in(
        session.query(CallNode.task_hash, CallNode.value_hash), CallNode.call_hash, ids
    ):
        yield "CallNode.task", Task, task_hash
        yield "CallNode.result", Value, value_hash

    query = session.query(
        Argument.arg_hash, Argument.value_hash, ArgumentResult.result_call_hash
    ).outerjoin(ArgumentResult)
    seen_args = set()
    for arg_hash, value_hash, upstream_call_hash in filter_in(query, Argument.call_hash, ids):
        # Get CallNode argument value ids.
        if arg_hash not in seen_args:
            yield "CallNode.arg", Value, value_hash
            seen_args.add(arg_hash)

        # Get CallNode upstream CallNode ids.
        if upstream_call_hash:
            yield "CallNode.upstream", CallNode, upstream_call_hash

    # Get CallNode child ids.
    for (child_id,) in filter_in(session.query(CallEdge.child_id), CallEdge.parent_id, ids):
        yield "CallNode.child_call_node", CallNode, child_id


def get_value_child_edges(session: Session, ids: Iterable[str]) -> Iterable[RecordEdgeType]:
    # Get Value subvalue ids.
    for (subvalue_id,) in filter_in(
        session.query(Subvalue.value_hash), Subvalue.parent_value_hash, ids
    ):
        yield "Value.subvalue", Value, subvalue_id


def get_tag_child_edges(session: Session, ids: Iterable[str]) -> Iterable[RecordEdgeType]:
    """
    Get parents and children of the tags with the supplied `ids`.
    """

    # Get Tag parents.
    for (parent_id,) in filter_in(session.query(TagEdit.parent_id), TagEdit.child_id, ids):
        yield "Tag.parent", Tag, parent_id

    # Get Tag children.
    for (child_id,) in filter_in(session.query(TagEdit.child_id), TagEdit.parent_id, ids):
        yield "Tag.child", Tag, child_id


def get_tag_entity_child_edges(session: Session, ids: Iterable[str]) -> Iterable[RecordEdgeType]:
    """
    Iterates through the child edges for Tags of a set of entity ids.
    """

    # Get current Entity Tags.
    for (tag_hash,) in filter_in(session.query(Tag.tag_hash), Tag.entity_id, ids):
        yield "Entity.tag", Tag, tag_hash


def get_abs_path(root_dir: str, path: str) -> str:
    """
    Get the absolute path of `path` if it is a local path.
    """
    if get_proto(path) == "local" and not os.path.isabs(path):
        return os.path.join(root_dir, path)
    else:
        return path


class RedunSession(Session):
    """
    Sqlalchemy Session with a reference to the redun backend.

    This is used to give Sqlalchemy models access to the redun backend API.
    """

    def __init__(self, backend: "RedunBackendDb", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.backend: "RedunBackendDb" = backend


class RedunBackendDb(RedunBackend):
    """
    A database-based Backend for managing the CallGraph (provenance) and cache for a Scheduler.

    This backend makes use of SQLAlchemy to provide support for both a sqlite and postgresql
    databases.
    """

    def __init__(
        self,
        db_uri: Optional[str] = None,
        config: Optional[Section] = None,
        logger: Optional[Any] = None,
        *args: Any,
        **kwargs: Any,
    ):
        super().__init__(*args, **kwargs)

        if not config:
            config = create_config_section()
        self.logger = logger or _logger

        self.db_uri: str = RedunBackendDb._get_uri(db_uri, config)
        self.connect_args = {}
        if self.db_uri.startswith("sqlite:///"):
            self.connect_args["check_same_thread"] = False
            default_automigrate = "True"
        else:
            default_automigrate = "False"

        self.automigrate = str2bool(config.get("automigrate", default_automigrate))

        self.engine: Optional[Engine] = None
        self.session: Optional[Session] = None
        self._record_serializer: Optional[serializers.RecordSerializer] = None

        self.value_store: Optional[ValueStore] = None
        self.value_store_min_size: int = int(
            config.get("value_store_min_size", str(DEFAULT_VALUE_STORE_MIN_SIZE))
        )
        if config.get("value_store_path"):
            default_config_dir = os.path.join(os.getcwd(), ".redun")
            value_store_path = get_abs_path(
                config.get("config_dir", default_config_dir), config["value_store_path"]
            )
            self.value_store = ValueStore(value_store_path)

        # Execution state.
        self.current_execution: Optional[Execution] = None

        # User can use redun.ini set a smaller max size.
        self._max_value_size: int = int(config.get("max_value_size", str(DEFAULT_MAX_VALUE_SIZE)))

        self._database_loaded = False
        self._db_echo = bool(os.environ.get("REDUN_DB_ECHO"))

    def clone(self, session: Session = None):
        """
        Return a copy of the backend that shares the instantiated database engine
        """
        if self._database_loaded:
            # Shallow-copying, and then manually setting the session attribute, so that clones
            # share the database engine (which is thread safe) but get a new database session
            # (which is not)
            cloned_backend = shallowcopy(self)
            cloned_backend.session = session or self.Session(backend=self)
            return cloned_backend
        else:
            raise RedunDatabaseError(
                "RedunBackendDb can be cloned only after the database has been loaded"
            )

    def create_engine(self) -> Engine:
        """
        Initializes a database connection.
        """
        self.engine = create_engine(
            self.db_uri, connect_args=self.connect_args, echo=self._db_echo, future=True
        )
        self.Session = sessionmaker(bind=self.engine, class_=RedunSession)
        self.session = self.Session(backend=self)
        return self.engine

    @staticmethod
    def get_db_version_required() -> Tuple[DBVersionInfo, DBVersionInfo]:
        """
        Returns the DB version range required by this library.
        """
        return REDUN_DB_MIN_VERSION, REDUN_DB_MAX_VERSION

    @staticmethod
    def get_all_db_versions() -> List[DBVersionInfo]:
        """
        Returns list of all DB versions and their migration ids.
        """
        return REDUN_DB_VERSIONS

    def get_db_version(self) -> DBVersionInfo:
        """
        Returns the current DB version (major, minor).
        """
        assert self.engine
        assert self.session

        inspector = inspect(self.engine)
        table_names = inspector.get_table_names()

        # Infer the db major version from RedunVersion table.
        if RedunVersion.__tablename__ in table_names:
            version_row = (
                self.session.query(RedunVersion.version)
                .order_by(RedunVersion.timestamp.desc())
                .first()
            )
            if not version_row:
                return null_db_version
            major = version_row[0]
        else:
            # This database doesn't appear to be setup for redun schema yet.
            return null_db_version

        # Infer the db minor version based on the applied alembic migration id.
        applied_migration_ids = {
            migration_id
            for (migration_id,) in self.session.query(RedunMigration.version_num).all()
        }
        for version_info in reversed(self.get_all_db_versions()):
            if version_info.major == major and version_info.migration_id in applied_migration_ids:
                return version_info

        # If client does not recognize the migration id, it is too new for the client.
        return DBVersionInfo(
            "ffffffffffff", major, REDUN_DB_UNKNOWN_VERSION, "DB version newer than client."
        )

    def migrate(
        self, desired_version: Optional[DBVersionInfo] = None, upgrade_only: bool = False
    ) -> None:
        """
        Migrate database to desired version.

        Parameters
        ----------
        desired_version: Optional[DBVersionInfo]
            Desired version to update redun database to. If null, update to latest version.
        upgrade_only: bool
            By default, this function will perform both upgrades and downgrades.
            Set this to true to prevent downgrades (such as during automigration).
        """
        assert self.engine
        assert self.session

        _, newest_allowed_version = self.get_db_version_required()

        db_dir = os.path.dirname(__file__)
        alembic_config_file = os.path.join(db_dir, "alembic.ini")
        alembic_script_location = os.path.join(db_dir, "alembic")

        config = AConfig(alembic_config_file)
        config.set_main_option("script_location", alembic_script_location)
        config.session = self.session

        # Determine version in db.
        version = self.get_db_version()
        if version > newest_allowed_version:
            # db version is too new to work with, abort.
            raise RedunVersionError(
                f"redun database is too new for this program: {version} > {newest_allowed_version}"
            )

        if desired_version is None:
            # Default to latest version.
            desired_version = self.get_all_db_versions()[-1]

        # Perform migration.
        if desired_version > version:
            self.logger.info(f"Upgrading db from version {version} to {desired_version}...")
            upgrade(config, desired_version.migration_id)
        elif desired_version < version and not upgrade_only:
            self.logger.info(f"Downgrading db from version {version} to {desired_version}...")
            downgrade(config, desired_version.migration_id)
        else:
            # Already at desired version.
            self.logger.debug(f"Already at desired db version {version}.")
            return

        # Record migration has been applied.
        self.session.add(RedunVersion(version=desired_version.major))

        # Commit migrations. This also ensures there are no checkedout connections.
        self.session.commit()

        # Reset the serializer to the new db version.
        self._record_serializer = serializers.RecordSerializer(desired_version)

    def is_db_compatible(self) -> bool:
        """
        Returns True if database is compatible with library.
        """
        min_version, max_version = self.get_db_version_required()
        return min_version <= self.get_db_version() <= max_version

    def load(self, migrate: Optional[bool] = None) -> None:
        """
        Load backend database.

        For protection, only upgrades are allowed when automigrating. Downgrades
        could potentially drop data. Users should explicitly downgrade using
        something like `redun db downgrade XXX`.

        Parameters
        ----------
        migrate : Optional[bool]
            If None, defer to automigration config options. If True, perform
            migration after establishing database connection.
        """
        self.create_engine()
        if migrate is None:
            migrate = self.automigrate
        if migrate:
            self.migrate(upgrade_only=True)
        if not self.is_db_compatible():
            min_version, max_version = self.get_db_version_required()
            raise RedunVersionError(
                f"redun database is an incompatible version {self.get_db_version()}. "
                f"Version must be within >={min_version},<{max_version.major + 1}"
            )
        version = self.get_db_version()
        self._database_loaded = True

        # Setup serializer.
        self._record_serializer = serializers.RecordSerializer(version)

    def record_call_node(
        self,
        task_name: str,
        task_hash: str,
        args_hash: str,
        expr_args: Tuple[Tuple, dict],
        eval_args: Tuple[Tuple, dict],
        result_hash: str,
        child_call_hashes: List[str],
    ) -> str:
        """
        Record a completed CallNode.

        Parameters
        ----------
        task_name : str
            Fullname (with namespace) of task.
        task_hash : str
            Hash of task.
        args_hash : str
            Hash of all arguments of the call.
        expr_args : Tuple[Tuple, dict]
            Original expressions for the task arguments. These expressions are used
            to record the full upstream dataflow.
        eval_args : Tuple[Tuple, dict]
            The fully evaluated arguments of the task arguments.
        result_hash : str
            Hash of the result value of the call.
        child_call_hashes: List[str]
            call_hashes of any child task calls.

        Returns
        -------
        str
            The call_hash of the new CallNode.
        """
        assert self.session

        call_hash = hash_call_node(task_hash, args_hash, result_hash, child_call_hashes)

        if not self.session.query(CallNode).filter_by(call_hash=call_hash).first():
            self.session.add(
                CallNode(
                    call_hash=call_hash,
                    task_name=task_name,
                    task_hash=task_hash,
                    args_hash=args_hash,
                    value_hash=result_hash,
                )
            )

            for i, child_call_hash in enumerate(child_call_hashes):
                self.session.add(
                    CallEdge(parent_id=call_hash, child_id=child_call_hash, call_order=i)
                )
            self.session.commit()

            self._record_args(call_hash, expr_args, eval_args)

            self._record_call_subtree_tasks(task_hash, call_hash, child_call_hashes)
        return call_hash

    def _record_call_subtree_tasks(
        self, task_hash: str, call_hash: str, child_call_hashes: List[str]
    ):
        """
        Record task_set for subtree of call node.
        """
        assert self.session

        task_hashes = {
            row[0]
            for row in filter_in(
                self.session.query(CallSubtreeTask.task_hash)
                .join(CallNode, CallSubtreeTask.call_hash == CallNode.call_hash)
                .distinct(),
                CallSubtreeTask.call_hash,
                child_call_hashes,
            )
        }
        task_hashes.add(task_hash)

        for task_hash2 in task_hashes:
            self.session.add(CallSubtreeTask(call_hash=call_hash, task_hash=task_hash2))

        self.session.commit()

    def _find_arg_upstreams(self, expr_arg: AnyExpression) -> Iterator[str]:
        """
        Recurse through an expression to find upstream call hashes.

        .. code-block:: python

            x = task1()

            # Argument is a TaskExpression and is from a call to `task1()`.
            result = task2(x)

            # Argument is a SimpleExpression that is ultimately from `task1()`.
            result2 = task2(x["key"])
        """
        for value in iter_nested_value(expr_arg):
            if isinstance(value, TaskExpression) and not isinstance(value, SchedulerExpression):
                # TaskExpressions (non-scheduler) with completed Jobs are upstream.
                if value.call_hash:
                    yield value.call_hash
            elif isinstance(value, Expression):
                # Recurse into arguments of Simple and Scheduler Expressions.
                yield from self._find_arg_upstreams(value._upstreams)

    def _record_args(
        self, call_hash: str, expr_args: Tuple[Tuple, dict], eval_args: Tuple[Tuple, dict]
    ) -> None:
        """
        Record the Arguments for a CallNode.

        Parameters
        ----------
        call_hash : str
            Hash of CallNode of these arguments.
        expr_args : Tuple[Tuple, dict]
            Original expressions for the task arguments. These expressions are used
            to record the full upstream dataflow.
        eval_args : Tuple[Tuple, dict]
            The fully evaluated arguments of the task arguments.
        """
        assert self.session

        # Get default arguments (present in eval_args but not expr_args).
        eval_pos_args, eval_kwargs = eval_args
        expr_pos_args, expr_kwargs = expr_args
        default_args = [
            (None, key, eval_kwargs[key], eval_kwargs[key])
            for key in eval_kwargs
            if key not in expr_kwargs
        ]
        kw_keys = sorted(set(eval_kwargs) & set(expr_kwargs))

        # Combine positional and keyword arguments into one iterator.
        all_args = chain(
            (
                (i, None, expr_arg, eval_arg)
                for i, (expr_arg, eval_arg) in enumerate(zip(expr_pos_args, eval_pos_args))
            ),
            ((None, key, expr_kwargs[key], eval_kwargs[key]) for key in kw_keys),
            default_args,
        )

        for i, key, expr_arg, eval_arg in all_args:
            # Record an Argument for the call.
            value_hash = self.record_value(eval_arg)
            arg_hash = hash_struct(["Argument", call_hash, str(i), str(key), value_hash])
            self.session.add(
                Argument(
                    arg_hash=arg_hash,
                    call_hash=call_hash,
                    value_hash=value_hash,
                    arg_position=i,
                    arg_key=key,
                )
            )

            # Record upstream CallNodes for an argument.
            for result_call_hash in set(self._find_arg_upstreams(expr_arg)):
                self.session.add(
                    ArgumentResult(
                        arg_hash=arg_hash,
                        result_call_hash=result_call_hash,
                    )
                )

        self.session.commit()

    def record_value(self, value: Any, data: Optional[bytes] = None) -> str:
        """
        Record a Value into the backend.

        Parameters
        ----------
        value : Any
            A value to record.
        data : Optional[bytes]
            Byte stream to record. If not given, usual value serialization is used.

        Returns
        -------
        str
            value_hash of recorded value.
        """
        assert self.session

        value_interface = self.type_registry.get_value(value)

        if data is None:
            data = value_interface.serialize()
        if len(data) > self._max_value_size:
            raise RedunDatabaseError(
                f"Value {trim_string(repr(value))} is too large (> {self._max_value_size}) "
                f"to store in the redun database. If you need to store larger values, "
                f"increase the `max_value_size` setting and consider using a value store "
                f"(`value_store_path`)."
            )

        value_hash = value_interface.get_hash(data=data)
        value_format = value_interface.get_serialization_format()

        if self.value_store and sys.getsizeof(data) >= self.value_store_min_size:
            # If defined, store binary data in ValueStore instead of db.
            self.value_store.put(value_hash, data)
            # Store an empty placeholder in the database to indicate that the value
            # has been written to the value store.
            # See `_get_value_data`
            data = b""

        value_row = self.session.query(Value).filter_by(value_hash=value_hash).first()
        if value_row:
            # Value already recorded.
            return value_hash

        type_name = self.type_registry.get_type_name(type(value))
        self.session.add(
            Value(
                value_hash=value_hash,
                type=type_name,
                format=value_format,
                value=data,
            )
        )
        self.session.commit()

        self._record_special_redun_values([value], [value_hash])

        # Record subvalues.
        subvalues = list(value_interface.iter_subvalues())
        if subvalues:
            self._record_subvalues(subvalues, value_hash)

        return value_hash

    def _record_special_redun_values(self, values: List[Any], value_hashes: List[str]):
        """
        Record special Values such as Files and Tasks
        """
        assert self.session

        existing_file_hashes = {
            row[0]
            for row in filter_in(
                self.session.query(File.value_hash),
                File.value_hash,
                value_hashes,
            )
        }
        existing_task_hashes = {
            row[0]
            for row in filter_in(
                self.session.query(Task.hash),
                Task.hash,
                value_hashes,
            )
        }

        new_inserts = False
        for value, value_hash in zip(values, value_hashes):
            if isinstance(value, BaseFile) and value_hash not in existing_file_hashes:
                existing_file_hashes.add(value_hash)
                new_inserts = True
                self.session.add(
                    File(
                        value_hash=value_hash,
                        path=value.path,
                    )
                )
            elif isinstance(value, BaseTask) and value_hash not in existing_task_hashes:
                existing_task_hashes.add(value_hash)
                new_inserts = True
                self.session.add(
                    Task(
                        hash=value_hash,
                        name=value.name,
                        namespace=value.namespace,
                        source=value.source,
                    )
                )

        if new_inserts:
            self.session.commit()

    def _record_subvalues(self, subvalues: List[Any], parent_value_hash: str):
        """
        Record subvalues for a parent Value (parent_value_hash).
        """
        assert self.session

        # Serialize and hash all subvalues.
        data = [self.type_registry.serialize(value) for value in subvalues]
        value_hashes = [
            self.type_registry.get_hash(value, data=datum) for value, datum in zip(subvalues, data)
        ]
        existing_value_hashes = {
            row[0]
            for row in filter_in(
                self.session.query(Value.value_hash),
                Value.value_hash,
                value_hashes,
            )
        }

        # Insert new Values into db.
        new_inserts = False
        for value, value_hash, datum in zip(subvalues, value_hashes, data):
            if value_hash in existing_value_hashes:
                continue
            existing_value_hashes.add(value_hash)

            type_name = self.type_registry.get_type_name(type(value))
            value_format = self.type_registry.get_serialization_format(value)
            new_inserts = True
            self.session.add(
                Value(
                    value_hash=value_hash,
                    type=type_name,
                    format=value_format,
                    value=datum,
                )
            )

        # Insert new Subvalue (child-parent) links into db.
        existing_parent_links = {
            row[0]
            for row in filter_in(
                self.session.query(Subvalue.value_hash).filter(
                    Subvalue.parent_value_hash == parent_value_hash
                ),
                Subvalue.value_hash,
                value_hashes,
            )
        }
        for value, value_hash, datum in zip(subvalues, value_hashes, data):
            if value_hash in existing_parent_links:
                continue
            existing_parent_links.add(value_hash)

            new_inserts = True
            self.session.add(
                Subvalue(
                    value_hash=value_hash,
                    parent_value_hash=parent_value_hash,
                )
            )

        if new_inserts:
            self.session.commit()

        self._record_special_redun_values(subvalues, value_hashes)

    def _deserialize_value(self, type_name: str, data: bytes) -> Tuple[Any, bool]:
        """
        Deserialize bytes into a Value using TypeRegistry.
        """
        try:
            value = self.type_registry.deserialize(type_name, data)
            return value, True
        except InvalidValueError:
            return None, False

    def _get_value_data(self, value_row: Value) -> Tuple[bytes, bool]:
        """
        Retrieve value data from db row or ValueStore.
        """
        if not value_row.in_value_store:
            # Use data in db row if defined.
            return value_row.value, True
        elif self.value_store:
            # Fall back to ValueStore to retrieve binary data.
            return self.value_store.get(value_row.value_hash)
        else:
            raise AssertionError("ValueStore is not defined.")

    def _get_value(self, value_row: Value) -> Tuple[Any, bool]:
        """
        Gets a value from a Value model.
        """
        data, has_value = self._get_value_data(value_row)
        if not has_value:
            return None, False
        return self._deserialize_value(value_row.type, data)

    def _get_value_size(self, value_row: Value) -> int:
        """
        Returns the size in bytes of a Value from db row or ValueStore.
        """
        if not value_row.in_value_store:
            # Use data in db row if defined.
            return len(value_row.value)
        elif self.value_store:
            # Fall back to ValueStore.
            return self.value_store.size(value_row.value_hash)
        else:
            raise AssertionError("ValueStore is not defined.")

    def get_value(self, value_hash: str) -> Tuple[Any, bool]:
        """
        Returns a Value from the datastore using the value content address (value_hash).

        Parameters
        ----------
        value_hash : str
            Hash of Value to fetch from ValueStore.

        Returns
        -------
        result, is_cached : Tuple[Any, bool]
            Returns the result `value` and `is_cached=True` if the value is in the
            ValueStore, otherwise returns (None, False).
        """
        assert self.session
        value_row = self.session.query(Value).filter_by(value_hash=value_hash).one_or_none()
        if not value_row:
            return None, False
        return self._get_value(value_row)

    def check_cache(
        self,
        task_hash: str,
        args_hash: str,
        eval_hash: str,
        execution_id: str,
        scheduler_task_hashes: Set[str],
        cache_scope: CacheScope,
        check_valid: CacheCheckValid,
        allowed_cache_results: Optional[Set[CacheResult]] = None,
    ) -> Tuple[Any, Optional[str], CacheResult]:
        """
        See parent method.
        """

        if allowed_cache_results is None:
            allowed_cache_results = set(CacheResult)

        assert self.session

        is_cached = False
        result = None
        call_hash: Optional[str] = None
        cache_type = CacheResult.MISS

        if cache_scope == CacheScope.NONE:
            return None, None, CacheResult.MISS

        if CacheResult.CSE in allowed_cache_results:
            # Check for CSE (Equivalent Job in same execution).
            call_node: CallNode = (
                self.session.query(CallNode)
                .join(Job, CallNode.call_hash == Job.call_hash)
                .filter(
                    Job.task_hash == task_hash,
                    Job.execution_id == execution_id,
                    CallNode.args_hash == args_hash,
                )
                .order_by(Job.start_time.desc())
                .first()
            )
            if call_node:
                result, is_cached = self.get_call_cache(call_node.call_hash)
                if is_cached:
                    return result, call_node.call_hash, CacheResult.CSE

        if (
            cache_scope == CacheScope.BACKEND
            and check_valid == CacheCheckValid.SHALLOW
            and CacheResult.ULTIMATE in allowed_cache_results
        ):
            # Check ultimate reduction cache.
            call_hash = self.get_call_hash(task_hash, args_hash, scheduler_task_hashes)
            if call_hash:
                result, is_cached = self.get_call_cache(call_hash)
                cache_type = CacheResult.ULTIMATE

        if (
            not is_cached
            and cache_scope == CacheScope.BACKEND
            and CacheResult.SINGLE in allowed_cache_results
        ):
            # Fallback to single reduction cache.
            result, is_cached = self.get_eval_cache(eval_hash)
            cache_type = CacheResult.SINGLE

        if is_cached:
            return result, call_hash, cache_type
        else:
            return None, None, CacheResult.MISS

    def get_eval_cache(self, eval_hash: str) -> Tuple[Any, bool]:
        """
        Checks the Evaluation cache for a cached result.

        Parameters
        ----------
        eval_hash : str
            Hash of the task and arguments used for call.

        Returns
        -------
        (result, is_cached) : Tuple[Any, bool]
            `result` is the cached result, or None if no result was found.
            `is_cached` is True if cache hit, otherwise is False.
        """
        assert self.session
        value_row = (
            self.session.query(Value)
            .join(Evaluation, Value.value_hash == Evaluation.value_hash)
            .filter(Evaluation.eval_hash == eval_hash)
            .one_or_none()
        )
        if not value_row:
            return None, False
        return self._get_value(value_row)

    def set_eval_cache(
        self, eval_hash: str, task_hash: str, args_hash: str, value: Any, value_hash: str = None
    ) -> None:
        """
        Sets a new value in the Evaluation cache.

        Parameters
        ----------
        eval_hash : str
            A hash of the combination of the task_hash and args_hash.
        task_hash : str
            Hash of Task used in the call.
        args_hash : str
            Hash of all arguments used in the call.
        value : Any
            Value to record in cache.
        value_hash : str
            Hash of value to record in cache.
        """
        assert self.session
        # Ensure value is recorded.
        if not value_hash:
            value_hash = self.record_value(value)

        # Update or create Evaluation entry.
        eval_row = self.session.query(Evaluation).filter_by(eval_hash=eval_hash).one_or_none()
        if eval_row:
            if eval_row.value_hash != value_hash:
                eval_row.value_hash = value_hash
                self.session.commit()
        else:
            try:
                self.session.add(
                    Evaluation(
                        eval_hash=eval_hash,
                        task_hash=task_hash,
                        args_hash=args_hash,
                        value_hash=value_hash,
                    )
                )
                self.session.commit()
            except sa.exc.IntegrityError:
                # If eval_hash has recently been added, do update instead.
                self.session.rollback()
                eval_row = self.session.get(Evaluation, eval_hash)
                eval_row.value_hash = value_hash
                self.session.commit()

    def _get_call_node(
        self, task_hash: str, args_hash: str, scheduler_task_hashes: Set[str]
    ) -> Optional[CallNode]:
        assert self.session

        # NOTE: For pure functions, there should not be two current CallNodes
        # with the same task_hash and args_hash. However, if we have re-executed
        # a CallNode because its previous result is now invalid (File or Handle)
        # then we prefer the results of the most recent CallNode.
        call_nodes = (
            self.session.query(CallNode)
            .filter_by(task_hash=task_hash, args_hash=args_hash)
            .order_by(CallNode.timestamp.desc())
        )

        # Intersect call_node task_hashes with current task hashes.
        call_hashes = {call_node.call_hash for call_node in call_nodes}
        call_task_pairs = filter_in(
            self.session.query(CallSubtreeTask), CallSubtreeTask.call_hash, call_hashes
        )
        call_node2task_hashes = defaultdict(set)
        for pair in call_task_pairs:
            call_node2task_hashes[pair.call_hash].add(pair.task_hash)

        current_call_nodes = [
            call_node
            for call_node in call_nodes
            if call_node2task_hashes[call_node.call_hash] <= scheduler_task_hashes
        ]

        if current_call_nodes:
            # Use the newest CallNode.
            return current_call_nodes[0]
        else:
            return None

    def get_call_hash(
        self, task_hash: str, args_hash: str, scheduler_task_hashes: Set[str]
    ) -> Optional[str]:
        """
        Returns the call_hash of a current CallNode with matching task and arg hashes.

        A CallNode is considered *current* only if its Task is currently in the
        TaskRegistry (same task_hash) and all its child CallNodes are current.

        Parameters
        ----------
        task_hash : str
            Hash of Task used in the call.
        args_hash : str
            Hash of all arguments used in the call.

        Returns
        -------
        Optional[str]
            Hash of the found CallNode, or None.
        """
        call_node = self._get_call_node(task_hash, args_hash, scheduler_task_hashes)
        if call_node:
            return call_node.call_hash
        else:
            return None

    def get_call_cache(self, call_hash: str) -> Tuple[Any, bool]:
        """
        Returns the result of a previously recorded CallNode.

        Parameters
        ----------
        call_hash : str
            Hash of a CallNode.

        Returns
        -------
        Any
            Recorded final result of a CallNode.
        """
        assert self.session
        value_row = (
            self.session.query(Value)
            .join(CallNode, Value.value_hash == CallNode.value_hash)
            .filter(CallNode.call_hash == call_hash)
            .one_or_none()
        )
        if not value_row:
            return None, False
        return self._get_value(value_row)

    def explain_cache_miss(self, task: "BaseTask", args_hash: str) -> Optional[Dict[str, Any]]:
        """
        Determine the reason for a cache miss.
        """
        assert self.session

        # Is there a previous CallNode with the same args and the same
        # task by name?
        call_node = (
            self.session.query(CallNode)
            .join(Task)
            .filter(
                CallNode.args_hash == args_hash,
                Task.name == task.name,
                Task.namespace == task.namespace,
            )
            .order_by(CallNode.timestamp.desc())
            .first()
        )
        if call_node:
            return {
                "reason": "new_task",
                "call_hash": call_node.call_hash,
                "call_task_hash": call_node.task_hash,
                "call_args_hash": call_node.args_hash,
            }

        # Is there a previous CallNode with the same task hash, but different
        # arguments?
        call_node = (
            self.session.query(CallNode)
            .filter(
                CallNode.task_hash == task.hash,
            )
            .order_by(CallNode.timestamp.desc())
            .first()
        )
        if call_node:
            return {
                "reason": "new_args",
                "call_hash": call_node.call_hash,
                "call_task_hash": call_node.task_hash,
                "call_args_hash": call_node.args_hash,
            }
        else:
            # This is a completely new call.
            return {
                "reason": "new_call",
            }

    def advance_handle(self, parent_handles: List[BaseHandle], child_handle: BaseHandle) -> None:
        """
        Record parent-child relationships between Handles.
        """
        assert self.session

        # Try to detect previous Handles that have skipped recording
        # such as due to multiple chained fork calls.
        queue = [
            parent_handle.__handle__.fork_parent
            for parent_handle in parent_handles
            if parent_handle.__handle__.fork_parent and not parent_handle.__handle__.is_recorded
        ]
        while queue:
            _handle = queue.pop()
            get_or_create(
                self.session,
                Handle,
                {
                    "hash": _handle.__handle__.hash,
                    "fullname": _handle.__handle__.fullname,
                    "key": _handle.__handle__.key,
                    "value_hash": self.record_value(_handle),
                },
                {"is_valid": True},
            )
            _handle.__handle__.is_recorded = True
            if _handle.__handle__.fork_parent:
                queue.append(_handle.__handle__.fork_parent)

        # Get or create child_handle.
        child_row, _ = get_or_create(
            self.session,
            Handle,
            {
                "hash": child_handle.__handle__.hash,
                "fullname": child_handle.__handle__.fullname,
                "key": child_handle.__handle__.key,
                "value_hash": self.record_value(child_handle),
            },
            {"is_valid": True},
        )
        child_handle.__handle__.is_recorded = True

        for parent_handle in parent_handles:
            # Get or create parent handle.
            parent_row, _ = get_or_create(
                self.session,
                Handle,
                {
                    "hash": parent_handle.__handle__.hash,
                    "fullname": parent_handle.__handle__.fullname,
                    "key": parent_handle.__handle__.key,
                    "value_hash": self.record_value(parent_handle),
                },
                {"is_valid": True},
            )
            parent_handle.__handle__.is_recorded = True

            # Get or create handle edge.
            handle_edge, _ = get_or_create(
                self.session,
                HandleEdge,
                {
                    "parent_id": parent_handle.__handle__.hash,
                    "child_id": child_handle.__handle__.hash,
                },
            )

        self.session.commit()

    def rollback_handle(self, handle: BaseHandle) -> None:
        """
        Rollback all descendant handles.
        """
        assert self.session

        # Gather all valid handles of the same name and their children ids
        # in order or perform the recursive search more efficiently in python.
        handles_same_name = (
            self.session.query(Handle.hash, HandleEdge.child_id)
            .join(HandleEdge, HandleEdge.parent_id == Handle.hash)
            .filter(Handle.fullname == handle.__handle__.fullname, Handle.is_valid.is_(True))
            .all()
        )

        # Build graph.
        lookups = defaultdict(list)
        for handle_hash, child_id in handles_same_name:
            lookups[handle_hash].append(child_id)

        # Past children of handle are invalid.
        invalid_children = [child_id for child_id in lookups[handle.__handle__.hash]]

        # Determine all descendants of invalid children.
        invalid_hashes = set()
        queue = invalid_children
        while queue:
            handle_hash = queue.pop()
            if handle_hash in invalid_hashes:
                # Don't recurse the same handle twice in this DAG.
                continue
            invalid_hashes.add(handle_hash)
            queue.extend(lookups[handle_hash])

        # Invalidate descendants.
        for query in query_filter_in(self.session.query(Handle), Handle.hash, invalid_hashes):
            query.update(
                {Handle.is_valid: False},
                synchronize_session=False,
            )
        # Query.update() skips around session, so we need to expire it.
        # https://docs.sqlalchemy.org/en/13/orm/query.html#sqlalchemy.orm.query.Query.update.params.synchronize_session
        self.session.expire_all()

    def is_valid_handle(self, handle: BaseHandle) -> bool:
        """
        A handle is valid if it current or ancestral to the current handle.
        """
        assert self.session
        (is_valid,) = (
            self.session.query(Handle.is_valid)
            .filter_by(hash=handle.__handle__.hash)
            .one_or_none()
        )
        # If handle isn't recorded, it is not valid.
        return is_valid is True

    def record_execution(self, exec_id: str, args: List[str]) -> None:
        """
        Records an Execution to the backend.

        Parameters
        ----------
        exec_id : str
            The id of the execution.
        args : List[str]
            Arguments used on the command line to start Execution.

        Returns
        -------
        str
            UUID of new Execution.
        """
        self.current_execution = Execution(
            id=exec_id,
            args=json.dumps(args),
        )

    def record_job_start(self, job: "BaseJob", now: Optional[datetime] = None) -> Job:
        """
        Records the start of a new Job.
        """
        assert self.session

        task = job.task
        assert task
        assert job.execution

        # Get or create task.
        self.record_value(task)

        if not job.parent_job:
            # Record top-level job for the execution.
            assert self.current_execution
            assert self.current_execution.job_id is None
            self.current_execution.job_id = job.id
            self.session.add(self.current_execution)

        if not now:
            now = datetime.now()

        db_job = Job(
            id=job.id,
            start_time=now,
            task_hash=task.hash,
            parent_id=(job.parent_job.id if job.parent_job else None),
            execution_id=job.execution.id,
        )
        self.session.add(db_job)
        self.session.commit()

        return db_job

    def record_job_end(
        self, job: "BaseJob", now: Optional[datetime] = None, status: Optional[str] = None
    ) -> None:
        """
        Records the end of a Job.

        Create the job if needed, in which case the job will be recorded with
        `start_time==end_time`
        """
        assert self.session

        # Currently, we denote failed Jobs by not recording the end_time.
        if status == "FAILED":
            now = None
        elif not now:
            now = datetime.now()
        db_job = self.session.query(Job).filter_by(id=job.id).first()
        if not db_job:
            db_job = self.record_job_start(job, now=now)
        db_job.cached = job.was_cached
        db_job.end_time = now
        db_job.call_hash = job.call_hash
        self.session.add(db_job)
        self.session.commit()

    def get_job(self, job_id: str) -> Optional[dict]:
        """
        Returns details for a Job.
        """
        assert self.session
        job = self.session.query(Job).filter_by(id=job_id).one_or_none()
        if not job:
            return None
        return {
            "job_id": job.id,
            "parent_id": job.parent_id,
            "execution_id": job.execution_id,
        }

    def record_tags(
        self,
        entity_type: TagEntity,
        entity_id: str,
        tags: Iterable[KeyValue],
        parents: Iterable[str] = (),
        update: bool = False,
        new: bool = False,
    ) -> List[Tuple[str, str, str, Any]]:
        """
        Record tags for an entity (Execution, Job, CallNode, Task, Value).

        Parameters
        ----------
        entity_type : TagEntity
            The type of the tagged entity (Execution, Job, etc).
        entity_id : str
            The id of the tagged entity.
        tags : Iterable[KeyValue]
            An iterable of key-value pairs to create as tags.
        parents : Iterable[str]
            Ids of tags to be superseded by the new tags.
        update : bool
            If True, automatically supersede any existing tags with keys matching those in `tags`.
            This also implies `new=True`.
        new : bool
            If True, force tags to be current.

        Returns
        -------

        [(tag_hash, entity_id, key, value)] : List[Tuple[str, str, str, Any]]
            Returns a list of the created tags.
        """
        if not tags:
            return []

        assert self.session

        # If updating, automatically determine parents.
        if update:
            keys = [key for key, _ in tags]
            parent_rows = self.session.query(Tag.tag_hash).filter(
                Tag.is_current.is_(True),
                Tag.entity_id == entity_id,
                Tag.key.in_(keys),
            )
            parents = list(parents)
            parents.extend(parent_hash for (parent_hash,) in parent_rows)
            new = True

        # Sort parent tag pks for hashing.
        parents = sorted(parents)

        # Prepare Tag rows.
        tag_rows = [
            Tag(
                tag_hash=hash_tag(entity_id, key, value, parents),
                entity_type=entity_type,
                entity_id=entity_id,
                key=key,
                value=value,
            )
            for key, value in tags
        ]

        if new:
            # Here, we force the tags to be current by walking down the
            # tag graph until we reach a leaf.
            # First, we detect whether any tags have been superseded.
            tag_hashes = [tag.tag_hash for tag in tag_rows]
            superseded_tag_hashes = {
                tag_hash
                for (tag_hash,) in self.session.query(TagEdit.parent_id).filter(
                    TagEdit.parent_id.in_(tag_hashes)
                )
            }
            superseded_tag_rows = [
                tag_row for tag_row in tag_rows if tag_row.tag_hash in superseded_tag_hashes
            ]

            # Filter out superseded tags and record them individually with their correct parent.
            tag_rows = [
                tag_row for tag_row in tag_rows if tag_row.tag_hash not in superseded_tag_hashes
            ]
            for tag_row in superseded_tag_rows:
                # By proposing new parents, we are walking down the tag graph.
                # Eventually, we will walk off the graph and create a new leaf node.
                self.record_tags(
                    entity_type,
                    entity_id,
                    [(tag_row.key, tag_row.value)],
                    parents=[tag_row.tag_hash],
                    new=True,
                )

        # Add new tags.
        tag_hashes = [tag.tag_hash for tag in tag_rows]
        existing_tags = {
            tag_hash
            for (tag_hash,) in self.session.query(Tag.tag_hash)
            .filter(Tag.tag_hash.in_(tag_hashes))
            .all()
        }
        new_tags = {tag for tag in tag_rows if tag.tag_hash not in existing_tags}

        # Add new TagEdits.
        tag_edits = [
            TagEdit(parent_id=parent, child_id=tag.tag_hash)
            for tag in tag_rows
            for parent in parents
        ]
        tag_edit_hashes = {(tag_edit.parent_id, tag_edit.child_id) for tag_edit in tag_edits}
        child_ids = {tag_edit.child_id for tag_edit in tag_edits}
        # We filter by child_ids to narrow the query.
        existing_tag_edits = {
            (row.parent_id, row.child_id)
            for row in self.session.query(TagEdit).filter(TagEdit.child_id.in_(child_ids)).all()
            if (row.parent_id, row.child_id) in tag_edit_hashes
        }

        new_tag_edits = {
            tag_edit
            for tag_edit in tag_edits
            if (tag_edit.parent_id, tag_edit.child_id) not in existing_tag_edits
        }

        # Invalidate old tags.
        self.session.query(Tag).filter(Tag.tag_hash.in_(parents)).update(
            {Tag.is_current: False}, False
        )

        # Write to db.
        self.session.add_all(new_tags)
        self.session.add_all(new_tag_edits)
        if new_tags or new_tag_edits:
            self.session.commit()

        return [(tag.tag_hash, entity_id, tag.key, tag.value) for tag in tag_rows]

    def delete_tags(
        self, entity_id: str, tags: Iterable[KeyValue], keys: Iterable[str] = ()
    ) -> List[Tuple[str, str, str, Any]]:
        """
        Delete tags.
        """
        assert self.session

        conditions = [
            and_(Tag.key == key, Tag.value == sa_cast(value, JSON)) for key, value in tags
        ]
        if keys:
            conditions.append(Tag.key.in_(keys))

        parents = [
            tag_hash
            for (tag_hash,) in self.session.query(Tag.tag_hash).filter(
                Tag.is_current.is_(True), Tag.entity_id == entity_id, or_(*conditions)
            )
        ]

        delete_tag = Tag.get_delete_tag()
        return self.record_tags(
            delete_tag.entity_type,
            delete_tag.entity_id,
            [(delete_tag.key, delete_tag.value)],
            parents=parents,
        )

    def update_tags(
        self,
        entity_type: TagEntity,
        entity_id: str,
        old_keys: Iterable[str],
        new_tags: Iterable[KeyValue],
    ) -> List[Tuple[str, str, str, Any]]:
        """
        Update tags.
        """
        assert self.session

        parents = [
            tag_hash
            for (tag_hash,) in self.session.query(Tag.tag_hash).filter(
                Tag.is_current.is_(True), Tag.entity_id == entity_id, Tag.key.in_(old_keys)
            )
        ]
        return self.record_tags(entity_type, entity_id, new_tags, parents=parents)

    def get_tags(self, entity_ids: List[str]) -> Dict[str, TagMap]:
        """
        Get the tags of an entity (Execution, Job, CallNode, Task, Value).
        """
        assert self.session

        tag_rows = (
            self.session.query(Tag)
            .filter(Tag.entity_id.in_(entity_ids) & Tag.is_current.is_(True))
            .all()
        )
        entity_tags: Dict[str, TagMap] = {}
        for tag in tag_rows:
            if tag.entity_id not in entity_tags:
                entity_tags[tag.entity_id] = MultiMap()
            entity_tags[tag.entity_id].add(tag.key, tag.value)
        return entity_tags

    # Serializable models.
    _model_pks = [
        (Execution, Execution.id),
        (Job, Job.id),
        (CallNode, CallNode.call_hash),
        (Value, Value.value_hash),
        (Tag, Tag.tag_hash),
    ]

    def get_records(self, ids: Iterable[str], sorted: bool = True) -> Iterable[dict]:
        """
        Returns serialized records for the given ids.

        Parameters
        ----------
        ids : Iterable[str]
            Iterable of record ids to fetch serialized records.
        sorted : bool
            If True, return records in the same order as the ids (Default: True).
        """
        assert self.session
        assert self._record_serializer

        ids = list(ids)
        if sorted:
            # Return records sorted by ids.
            id2record = {
                self._record_serializer.get_pk(record): record
                for record in self.get_records(ids, sorted=False)
            }
            for id in ids:
                record = id2record.get(id)
                if record:
                    yield record
            return
        for model, pk_field in self._model_pks:
            queries = query_filter_in(self.session.query(model), pk_field, ids)
            for query in queries:
                yield from self._record_serializer.serialize_query(query)

    def get_child_record_ids(
        self, model_ids: Iterable[Tuple[Base, str]]
    ) -> Iterable[RecordEdgeType]:
        """
        Iterates through record's ownership edges.

        Used for walking record graph for syncing.
        """
        assert self.session

        if not model_ids:
            return

        # Group record ids by models.
        model2ids = defaultdict(list)
        all_ids = []
        for model, id in model_ids:
            model2ids[model].append(id)
            all_ids.append(id)

        # Yield child records for each model type.
        model2edge_method = {
            Execution: get_execution_child_edges,
            Job: get_job_child_edges,
            CallNode: get_call_node_child_edges,
            Value: get_value_child_edges,
            Tag: get_tag_child_edges,
        }
        for model, edge_method in model2edge_method.items():
            if model in model2ids:
                yield from edge_method(self.session, model2ids[model])

        if all_ids:
            yield from get_tag_entity_child_edges(self.session, all_ids)

    def has_records(self, record_ids: Iterable[str]) -> List[str]:
        """
        Returns record_ids that exist in the database.
        """
        assert self.session

        record_ids = list(record_ids)
        existing_ids = {
            id
            for _, pk_field in self._model_pks
            for (id,) in filter_in(self.session.query(pk_field), pk_field, record_ids)
        }
        return [record_id for record_id in record_ids if record_id in existing_ids]

    def put_records(self, records: Iterable[dict]) -> int:
        """
        Writes records to the database and returns number of new records written.
        """
        assert self.session
        assert self._record_serializer

        records = list(records)
        record_ids = list(map(self._record_serializer.get_pk, records))
        existing_ids = set(self.has_records(record_ids))

        new_records = []
        for record_id, record in zip(record_ids, records):
            if record_id not in existing_ids:
                new_records.append(record)
                existing_ids.add(record_id)

        self.session.add_all(
            chain.from_iterable(map(self._record_serializer.deserialize, new_records))
        )

        # Post-process record writes.
        self._postprocess_new_records()
        self.session.commit()

        return len(new_records)

    def _postprocess_new_records(self) -> None:
        """
        Perform postprocessing after inserting new records into backend.
        """
        assert self.session

        # Update current Tags.
        # Only Tags with no children are current. Since new Tags are is_current=True
        # by default, and can only change to is_current=False, there is no need to
        # ever switch Tags back to is_current=True.

        # https://stackoverflow.com/questions/33153823/update-joined-table-via-sqlalchemy-orm-using-session-query
        (
            self.session.query(Tag)
            .filter(
                Tag.tag_hash
                == select(TagEdit.parent_id)
                .where(TagEdit.parent_id == Tag.tag_hash)
                .scalar_subquery()
            )
            .update({Tag.is_current: False}, synchronize_session=False)
        )

    def _get_record_types(self, record_ids: Iterable[str]) -> Iterator[Tuple[Base, str]]:
        """
        Determines the record type for each id.
        """
        assert self.session

        for model, pk_field in self._model_pks:
            for (record_id,) in filter_in(self.session.query(pk_field), pk_field, record_ids):
                yield model, record_id

    def iter_record_ids(self, root_ids: Iterable[str]) -> Iterator[str]:
        """
        Iterate the record ids of descendants of root_ids.
        """
        seen = set()

        # For database querying efficiency, we recurse through the graph a layer at a time.
        child_edges: Iterable[Tuple[str, Any, str]] = [
            ("", model, id) for model, id in self._get_record_types(root_ids)
        ]
        while True:
            # Determine unseen record ids and yield them.
            new_model_ids = set()
            for _, model, record_id in child_edges:
                if record_id not in seen:
                    new_model_ids.add((model, record_id))
                    seen.add(record_id)
                    yield record_id

            # No new record ids found, terminate.
            if not new_model_ids:
                break

            # Propose next edges to search.
            child_edges = self.get_child_record_ids(new_model_ids)

    @staticmethod
    def _get_uri(db_uri: Optional[str], conf: Section) -> str:
        db_aws_secret_name = conf.get("db_aws_secret_name", "")
        if db_aws_secret_name:
            return RedunBackendDb._get_uri_from_secret(db_aws_secret_name)

        base_uri = cast(str, db_uri or conf.get("db_uri", DEFAULT_DB_URI))

        # sqlite does not support credentials
        if base_uri.startswith("sqlite:/"):
            return base_uri
        return RedunBackendDb._get_credentialed_uri(base_uri, conf)

    @staticmethod
    def _get_uri_from_secret(secret_name: str) -> str:
        """
        Returns a DB URI from the supplied AWS secret stored with Secrets Manager.

        This will only work for RDS or other database secrets stored in Secrets Manager as the
        following keys are assumed to be present in the secret and those will be set for RDS and
        other database secrets in Secrets Manager:

            * engine
            * username
            * password
            * host
            * port
            * dbname

        """
        from redun.executors.aws_utils import DEFAULT_AWS_REGION, get_aws_client

        aws_region = os.environ.get("AWS_REGION", DEFAULT_AWS_REGION)
        client = get_aws_client("secretsmanager", aws_region)

        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        secret = get_secret_value_response["SecretString"]
        secret = json.loads(secret)

        engine2proto = {
            "postgres": "postgresql",
        }
        proto = engine2proto.get(secret["engine"], secret["engine"])

        # We need to quote the password in case there are any special characters in it that might
        # conflict with the parsing of the DB URI we are returning here.
        password = quote_plus(secret["password"])

        return (
            f"{proto}://{secret['username']}:{password}"
            f"@{secret['host']}:{secret['port']}/{secret['dbname']}"
        )

    @staticmethod
    def _get_credentialed_uri(base_uri: str, conf: Section) -> str:
        parts = urlparse(base_uri)

        if "@" in parts.netloc:
            raise RedunDatabaseError(
                "rejected db_uri containing credentials. use environment variables instead"
            )

        credentials = RedunBackendDb._get_login_credentials(conf)
        if credentials:
            parts = parts._replace(netloc=f"{credentials}@{parts.netloc}")
        return urlunparse(parts)

    @staticmethod
    def _get_login_credentials(conf: Section) -> Optional[str]:
        user = os.getenv(conf.get("db_username_env", DEFAULT_DB_USERNAME_ENV))
        password = quote_plus(os.getenv(conf.get("db_password_env", DEFAULT_DB_PASSWORD_ENV), ""))
        return user and password and f"{user}:{password}"
