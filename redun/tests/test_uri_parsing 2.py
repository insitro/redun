import json
from unittest.mock import patch

import pytest

from redun.backends.db import RedunBackendDb as RDB
from redun.backends.db import RedunDatabaseError
from redun.config import Section


@pytest.fixture
def mock_env_default_credentials(monkeypatch) -> None:
    monkeypatch.setenv("REDUN_DB_USERNAME", "user")
    monkeypatch.setenv("REDUN_DB_PASSWORD", "secret")


@pytest.fixture
def mock_env_custom_credentials(monkeypatch) -> None:
    monkeypatch.setenv("CUSTOM_DB_USERNAME", "user")
    monkeypatch.setenv("CUSTOM_DB_PASSWORD", "secret")


@pytest.fixture
def mock_env_no_credentials(monkeypatch) -> None:
    monkeypatch.delenv("REDUN_DB_USERNAME", raising=False)
    monkeypatch.delenv("REDUN_DB_PASSWORD", raising=False)


def _check_provided_credentials(conf: Section) -> None:
    sqllite_base = "sqlite:///redun.db"
    assert RDB._get_uri(sqllite_base, conf) == sqllite_base

    postgres_base = "postgresql://localhost:5432/redun"
    postgres_creds = "postgresql://user:secret@localhost:5432/redun"
    assert RDB._get_uri(postgres_base, conf) == postgres_creds


def test_default_credentials(mock_env_default_credentials) -> None:
    _check_provided_credentials({})


def test_custom_credentials(mock_env_custom_credentials) -> None:
    conf = {"db_username_env": "CUSTOM_DB_USERNAME", "db_password_env": "CUSTOM_DB_PASSWORD"}
    _check_provided_credentials(conf)


def test_no_credentials(mock_env_no_credentials) -> None:
    postgres_base = "postgresql://localhost:5432/redun"
    assert RDB._get_uri(postgres_base, {}) == postgres_base


def test_uri_with_credentials() -> None:
    with pytest.raises(RedunDatabaseError):
        RDB._get_uri("postgresql://user:secret@localhost:5432/redun", {})


@patch("redun.executors.aws_utils.get_aws_client")
def test_uri_from_secret(client_mock) -> None:
    engine = "postgres"
    username = "user1"
    password = "secret"
    host = "localhost"
    port = "5555"
    dbname = "mydb"

    client_mock.return_value.get_secret_value.return_value = {
        "SecretString": json.dumps(
            {
                "engine": engine,
                "username": username,
                "password": password,
                "host": host,
                "port": port,
                "dbname": dbname,
            }
        )
    }

    db_uri = RDB._get_uri_from_secret("mysecret")
    assert client_mock.get_secret_value.called_with("mysecret")
    assert db_uri == f"postgresql://{username}:{password}@{host}:{port}/{dbname}"
