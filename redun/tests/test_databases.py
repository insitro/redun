import json
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock

import pandas as pd
import pytest
import sqlalchemy as sa

from redun import task
from redun.databases import DbHandle, DatabaseConfig, DatabaseRegistry, get_db, register_db
from redun.scheduler import Scheduler
from redun.value import get_type_registry


@pytest.fixture
def sqlite_db_uri():
    """Create a temporary SQLite database."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    yield f"sqlite:///{db_path}"
    Path(db_path).unlink(missing_ok=True)


@pytest.fixture
def db_handle(sqlite_db_uri):
    """Create a configured DbHandle."""
    register_db("test_db", sqlite_db_uri)
    return get_db("test_db")


def test_dbhandle_config(sqlite_db_uri: str) -> None:
    """Test configuration."""
    config = DatabaseConfig(sqlite_db_uri, {"pool_size": 10})

    assert config.db_uri.startswith("sqlite:///")
    assert config.db_uri
    assert config.connect_args == {"pool_size": 10, "check_same_thread": False}


def test_dbhandle_config_postgres() -> None:
    """Test configuration with non-sqlite database."""
    config = DatabaseConfig("postgresql://localhost/test", {})
    assert "check_same_thread" not in config.connect_args


def test_dbhandle_config_aws_secret() -> None:
    """Test configuration."""

    mock_client = MagicMock()
    mock_client.get_secret_value.return_value = {
        "SecretString": json.dumps(
            {
                "engine": "postgres",
                "host": "myhost",
                "port": "123",
                "dbname": "db",
                "username": "alice",
                "password": "pwd",
            }
        )
    }

    with patch("redun.databases.get_aws_client", return_value=mock_client):
        config = DatabaseConfig("", {"aws_secret": "secret_name", "aws_region": "us-east-1"})
        assert config.db_uri == "postgresql://myhost:123/db"
        assert config.connect_args == {"user": "alice", "password": "pwd"}


def test_dbhandle_reset(db_handle: DbHandle) -> None:
    """Test reset clears engine and sessionmaker."""
    _ = db_handle.engine
    _ = db_handle.session()

    db_handle.reset()

    assert db_handle._engine is None


def test_dbhandle_execute_query(db_handle: DbHandle) -> None:
    """Test execute with SELECT returning all rows."""
    db_handle.execute(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)",
    )
    db_handle.execute("INSERT INTO users (name, age) VALUES ('Alice', 20)")
    db_handle.execute("INSERT INTO users (name, age) VALUES (:name, :age)", name="Bob", age=30)

    result = db_handle.execute("SELECT * FROM users")
    assert len(result) == 2

    result = db_handle.query("SELECT name, age FROM users ORDER BY name")
    assert len(result) == 2
    assert result[0]["name"] == "Alice"
    assert result[0]["age"] == 20

    df = db_handle.query_df("SELECT name, age FROM users")
    assert len(df) == 2
    assert list(df.columns) == ["name", "age"]


def test_dbhandle_transaction_session(db_handle: DbHandle) -> None:
    """Test transaction rollback on error."""
    db_handle.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")

    with db_handle.session() as session:
        session.execute(sa.text("INSERT INTO users (name) VALUES ('Alice')"))
        session.commit()

    result = db_handle.query("SELECT * FROM users")
    assert len(result) == 1


def test_dbhandle_transaction(db_handle: DbHandle) -> None:
    """Test transaction rollback on error."""
    db_handle.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")

    with db_handle.begin() as trans:
        trans.execute("INSERT INTO users (name) VALUES ('Alice')")

    result = db_handle.query("SELECT * FROM users")
    assert len(result) == 1


def test_dbhandle_transaction_rollback(db_handle: DbHandle) -> None:
    """Test transaction rollback on error."""
    db_handle.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")

    with pytest.raises(RuntimeError, match="^rollback$"):
        with db_handle.session() as session:
            session.execute(sa.text("INSERT INTO users (name) VALUES ('Alice')"))
            raise RuntimeError("rollback")

    result = db_handle.query("SELECT * FROM users")
    assert len(result) == 0


def test_registry_add_and_get(sqlite_db_uri: str) -> None:
    """Test adding and retrieving database."""
    register_db("test_db", sqlite_db_uri)
    conn = get_db("test_db")
    isinstance(conn, DbHandle)


def test_registry_iter(sqlite_db_uri: str) -> None:
    """Test iterating over registry."""
    registry = DatabaseRegistry()
    registry.add("db1", sqlite_db_uri)

    items = list(registry)
    assert isinstance(items[0], tuple)
    assert items[0][0] == "db1"
    assert isinstance(items[0][1], DatabaseConfig)


def test_workflow(scheduler: Scheduler, sqlite_db_uri: str) -> None:
    """
    Ensure DbHandle works across multiple tasks and with caching.
    """
    register_db("test_db", sqlite_db_uri)

    calls = []

    @task
    def init_db() -> DbHandle:
        calls.append("init_db")
        conn = get_db("test_db")
        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
        return conn

    @task
    def import_data(conn: DbHandle) -> DbHandle:
        calls.append("import_data")
        with conn.begin() as trans:
            trans.execute("DELETE FROM users")
            trans.execute("""
              INSERT INTO users (name, age) VALUES
                ('Alice', 20),
                ('Bob', 21),
                ('Claire', 21),
                ('Dan', 22),
                ('Elaine', 23),
                ('Fred', 24)
            """)
        return conn

    @task
    def query_data(conn: DbHandle) -> pd.DataFrame:
        calls.append("query_data")
        return conn.query_df("SELECT * FROM users")

    @task
    def main():
        conn = init_db()
        conn = import_data(conn)
        df = query_data(conn)
        return df

    # Run the workflow for the first time.
    df = scheduler.run(main())
    assert len(df) == 6
    assert calls == ["init_db", "import_data", "query_data"]

    # Rerunning workflow should be entirely cached.
    calls = []
    df = scheduler.run(main())
    assert len(df) == 6
    assert calls == []

    # Now change the import_data task.

    @task
    def import_data(conn: DbHandle) -> DbHandle:
        calls.append("import_data2")
        with conn.begin() as trans:
            trans.execute("DELETE FROM users")
            trans.execute("""
              INSERT INTO users (name, age) VALUES
                ('Alice', 20),
                ('Bob', 21)
            """)
        return conn

    # The init_db task should be cached.
    # import_data should rerun because it's task has changed.
    # import_data will use a DbHandle deserialized from the cache,
    # and it will be configured from the registry.
    # query_db should rerun because it's input handle has changed due to import_data changing.
    calls = []
    df = scheduler.run(main())
    assert len(df) == 2
    assert calls == ["import_data2", "query_data"]


def test_dbhandle_hash() -> None:
    # Both name and db_url should contribute to DbHandle hash.
    conn1 = DbHandle("db1", "sqlite:///db1.sql")
    conn1b = DbHandle("db1", "sqlite:///db1.sql")
    conn2 = DbHandle("db1", "sqlite:///db2.sql")

    registry = get_type_registry()
    assert registry.get_hash(conn1) == registry.get_hash(conn1b)
    assert registry.get_hash(conn1) != registry.get_hash(conn2)
