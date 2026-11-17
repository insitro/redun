import functools
import json
import os
from collections import defaultdict
from contextlib import contextmanager, nullcontext
from threading import RLock
from typing import Any, Iterable, Iterator

import sqlalchemy as sa
from sqlalchemy import create_engine
from sqlalchemy.engine import Connection as SQLAConnection, Engine
from sqlalchemy.orm import Session

from redun.executors.aws_utils import DEFAULT_AWS_REGION, get_aws_client
from redun.handle import Handle


try:
    # pandas is an optional dependency.
    import pandas as pd
except ImportError:
    pass


class DatabaseConfig:
    """
    Configuration for connecting to a database.
    """

    def __init__(self, db_uri: str, connect_args: dict):
        self.db_uri = db_uri
        self.connect_args = dict(connect_args)

        if self.db_uri.startswith("sqlite:"):
            # We will use locks to ensure multi-threading is safe.
            self.connect_args.setdefault("check_same_thread", False)

    def resolve(self) -> "DatabaseConfig":
        """
        Fully resolve the config.
        """
        db_uri = self.db_uri
        connect_args = dict(self.connect_args)

        if "callback" in connect_args:
            # Obtain db_uri, connect_args using a callback function.
            callback = connect_args.pop("callback")
            connect_args2 = dict(callback())
            if "db_uri" in connect_args2:
                db_uri = connect_args2.pop("db_uri")
            connect_args = connect_args2 | connect_args

        if "aws_secret" in connect_args:
            # Configure database connection from AWS Secret.
            secret_id = connect_args.pop("aws_secret")
            aws_region = connect_args.pop("aws_region", "")
            db_config = get_aws_secret_db_config(secret_id, aws_region=aws_region)
            db_uri = db_config.db_uri
            connect_args = db_config.connect_args | connect_args

        return DatabaseConfig(db_uri, connect_args)


class Connection:
    """
    A database connection created from DbHandle.

    Provides simpler transaction and SQL execution API for common data science tasks.
    """

    def __init__(self, conn: SQLAConnection):
        self.conn: SQLAConnection = conn

    def execute(self, sql: str, **params: Any) -> Any:
        """
        Execute SQL query.
        """
        result = self.conn.execute(sa.text(sql), params)
        return result.fetchall() if result.returns_rows else None


@functools.lru_cache
def get_aws_secret_db_config(secret_id: str, aws_region: str | None) -> DatabaseConfig:
    """
    Returns a DatabaseConfig from an AWS secret.
    """
    if not aws_region:
        aws_region = os.environ.get("AWS_REGION", DEFAULT_AWS_REGION)
    client = get_aws_client("secretsmanager", aws_region)
    secret = json.loads(client.get_secret_value(SecretId=secret_id)["SecretString"])
    engine2proto = {
        "postgres": "postgresql",
    }
    proto = engine2proto.get(secret["engine"], secret["engine"])

    db_uri = f"{proto}://{secret['host']}:{secret['port']}/{secret['dbname']}"
    connect_args = {
        "user": secret["username"],
        "password": secret["password"],
    }
    return DatabaseConfig(db_uri, connect_args)


class DbHandle(Handle):
    """
    Wraps a database connection in a redun Handle.
    """

    _locks: dict[str, RLock] = defaultdict(RLock)

    def __init__(self, name: str, db_uri: str = ""):
        self.name = name
        self.db_uri = db_uri
        self.connect_args: dict[str, Any] = {}
        self._engine: Engine | None = None
        self._lock = self._locks[name]

    def acquire(self) -> None:
        if self.engine.dialect.name == "sqlite":
            # Lock is only needed for sqlite connections.
            self._lock.acquire()

    def release(self) -> None:
        if self.engine.dialect.name == "sqlite":
            # Lock is only needed for sqlite connections.
            self._lock.release()

    @contextmanager
    def begin(self) -> Iterator[Connection]:
        lock = self._lock if self.engine.dialect.name == "sqlite" else nullcontext()
        with lock, self.engine.begin() as conn:
            yield Connection(conn)

    def __enter__(self) -> "DbHandle":
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.release()

    @property
    def engine(self) -> Engine:
        """
        Returns the SQLAlchemy Engine for this connection.
        """
        if not self._engine:
            db_config = get_database_registry().get(self.name)
            self._engine = create_engine(db_config.db_uri, connect_args=db_config.connect_args)
        return self._engine

    def session(self) -> Session:
        """
        Returns the SQLAlchemy Session for this connection.
        """
        return Session(self.engine)

    def reset(self) -> None:
        """
        Reset database connection.
        """
        self._engine = None

    def execute(self, sql: str, **params) -> Any:
        """
        Execute a SQL query in a single statement transaction.
        """
        with self.begin() as trans:
            return trans.execute(sql, **params)

    def query(self, sql: str, **params) -> list[dict[str, Any]]:
        """
        Query db and return results as dicts.
        """
        with self.engine.begin() as conn:
            result = conn.execute(sa.text(sql), params)
            return [dict(row) for row in result.mappings()]

    def query_df(self, sql: str, **params) -> "pd.DataFrame":
        """
        Query db and return results as a DataFrame.
        """
        with self.engine.begin() as conn:
            result = conn.execute(sa.text(sql), params)
            return pd.DataFrame(result, columns=list(result.keys()))  # ty:ignore[invalid-argument-type]


class DatabaseRegistry:
    """
    Registry of databases user has registered to use in their workflows.
    """

    def __init__(self):
        self._configs: dict[str, DatabaseConfig] = {}

    def add(self, name: str, db_uri: str = "", **connect_args: Any) -> None:
        """
        Add a database configuration to the registry.
        """
        self._configs[name] = DatabaseConfig(db_uri, connect_args)

    def get(self, name: str) -> DatabaseConfig:
        """
        Returns a DatabaseConfig for a named database.
        """
        return self._configs[name].resolve()

    def __iter__(self) -> Iterable[tuple[str, DatabaseConfig]]:
        return iter(self._configs.items())


# Global singleton for the database registry.
_database_registry = DatabaseRegistry()


def get_database_registry() -> DatabaseRegistry:
    """
    Returns the default DatabaseRegistry.
    """
    return _database_registry


def get_db(db_name: str) -> DbHandle:
    """
    Returns a DbHandle for a registered database.
    """
    db_config = get_database_registry().get(db_name)
    return DbHandle(db_name, db_config.db_uri)


def register_db(db_name: str, db_uri: str = "", **connect_args: Any) -> None:
    """
    Registers a database with the default DatabaseRegistry.

    connect_args:
    - aws_secret: AWS Secret used to fetch db_uri and connect_args.
    - callback: Function to call when connecting to obtain db_uri and connect_args.
      callback should take no arguments and returns a dict with connect_args and optional "db_uri" key.
    """
    get_database_registry().add(db_name, db_uri=db_uri, **connect_args)
