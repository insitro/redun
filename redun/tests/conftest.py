import logging
import os
import sys
import time
from typing import Iterator, Optional, Tuple, cast

import pytest
from _pytest.monkeypatch import MonkeyPatch
from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from redun import Scheduler
from redun.backends.db import RedunBackendDb, RedunSession
from redun.config import Config
from redun.task import get_task_registry
from redun.utils import clear_import_paths

logger = logging.getLogger(__name__)


def connect_with_retries(
    uri: str, interval: float = 0.1, timeout: float = 10.0
) -> Tuple[Engine, Connection]:
    """
    Connect to database with retries.
    """
    logger.info("Connecting to test database ({})...".format(uri))
    credentialed_uri = RedunBackendDb._get_credentialed_uri(uri, {})
    engine = create_engine(credentialed_uri, future=True)
    stop = time.time() + timeout
    while time.time() < stop:
        try:
            conn = engine.connect()
            logger.info("Connected to test database ({})".format(uri))
            return engine, conn
        except OperationalError:
            time.sleep(interval)

    raise RuntimeError("Unable to connect to test database: {}".format(uri))


@pytest.fixture(scope="session")
def monkeysession(request):
    mpatch = MonkeyPatch()
    yield mpatch
    mpatch.undo()


@pytest.fixture(scope="session")
def testdb(monkeysession) -> Iterator[Optional[str]]:
    """
    Create a test database.

    By using scope=session the test database is created only once per testing
    session.
    """
    host = "localhost"
    port = 5432
    database_name = "redun"

    monkeysession.setenv("REDUN_DB_USERNAME", "postgres")
    monkeysession.setenv("REDUN_DB_PASSWORD", "postgres")

    test_db_uri_stub = "postgresql://{host}:{port}/".format(
        host=host,
        port=port,
    )
    test_db_uri = test_db_uri_stub + database_name

    if not os.environ.get("REDUN_TEST_POSTGRES"):
        # Postgres database disabled.
        yield None
        return

    # Wait for database to accept connections.
    engine, conn = connect_with_retries(test_db_uri_stub)

    conn = conn.execution_options(isolation_level="AUTOCOMMIT")

    # Create test database.
    conn.execute(text("drop database if exists {}".format(database_name)))
    conn.execute(text("create database {}".format(database_name)))
    conn.close()
    engine.dispose()

    yield test_db_uri


@pytest.fixture
def scheduler(testdb: Optional[str]) -> Iterator[Scheduler]:
    """
    Returns a Scheduler for testing.
    """
    config: Optional[Config]

    if testdb:
        config = Config(
            {
                "scheduler": {"ignore_warnings": "namespace"},
                "backend": {"db_uri": testdb, "automigrate": "True"},
            }
        )
        scheduler = Scheduler(config=config)
        scheduler.load()
        backend = cast(RedunBackendDb, scheduler.backend)
        assert backend.engine and backend.session

        # We need to create a session bound to connection,
        # so let's get rid of the default one
        # background: https://docs.sqlalchemy.org/en/14/orm/session_transaction.html
        backend.session.close()

        # Run tests inside a transaction, so that we rollback effects.
        # start the session in a SAVEPOINT...
        connection = backend.engine.connect()

        trans = connection.begin()
        session = RedunSession(bind=connection, backend=backend)
        backend.session = session

        nested = connection.begin_nested()

        # then each time that SAVEPOINT ends, reopen it
        @event.listens_for(session, "after_transaction_end")
        def restart_savepoint(session, transaction):
            nonlocal nested
            if not nested.is_active:
                nested = connection.begin_nested()

        yield scheduler

        backend.session.close()
        trans.rollback()

        # Explicitly dispose the db engine so we can force kill the docker
        # process without error messages.
        connection.close()
        backend.engine.dispose()
        del scheduler

    else:
        yield Scheduler(
            config=Config(
                {
                    "scheduler": {"ignore_warnings": "namespace"},
                }
            )
        )


@pytest.fixture
def backend(scheduler: Scheduler) -> RedunBackendDb:
    """
    Returns a RedunBackend cast as RedunBackendDb.
    """
    backend = cast(RedunBackendDb, scheduler.backend)
    return backend


@pytest.fixture
def session(backend: RedunBackendDb) -> Session:
    """
    Returns a sqlalchemy Session for the redun backend.
    """
    assert backend.session
    return backend.session


@pytest.fixture(autouse=True)
def redun_globals(request):
    """
    pytest fixture for resetting redun global state during tests.

    - python modules
    - python sys.path
    - redun tasks
    - redun import paths
    """
    init_module_names = set(sys.modules.keys())
    init_sys_path = list(sys.path)
    task_registry = get_task_registry()

    # Temporarily remove unrelated tests from the task registry to prevent interference
    # (Note: Resetting the task registry to its original state at the end of this function doesn't
    # work because pytest loads all code modules before running any tests which means at this
    # point, all tasks across all unit tests are present in the task registry.)
    tasks_to_hide = {}
    for task_fullname in list(task_registry._tasks):
        if task_fullname.startswith("redun."):
            continue
        if task_fullname.startswith(request.module.__name__):
            continue
        if hasattr(request.module, "redun_namespace") and task_fullname.startswith(
            request.module.redun_namespace
        ):
            continue
        tasks_to_hide[task_fullname] = task_registry.get(task_fullname)
        del task_registry._tasks[task_fullname]

    yield

    # Reset imported modules.
    for module_name in list(sys.modules.keys()):
        if module_name not in init_module_names:
            sys.modules.pop(module_name)

    # Reset sys.path.
    sys.path = init_sys_path

    # Reset redun extra import paths.
    clear_import_paths()

    # Restore temporarily deleted task
    for k, v in tasks_to_hide.items():
        task_registry._tasks[k] = v
