import io
import os
from typing import List
from unittest import mock

import boto3
import pytest

from redun import File
from redun.cli import (
    DEFAULT_REDUN_INI,
    REDUN_CONFIG_ENV,
    REDUN_INI_FILE,
    RedunClient,
    RedunClientError,
)
from redun.tests.utils import assert_match_lines, mock_s3, use_tempdir


def make_config(config_path: str) -> None:
    config_file = File(os.path.join(config_path, REDUN_INI_FILE))
    with config_file.open("w") as out:
        out.write(DEFAULT_REDUN_INI.format(db_uri="sqlite:///other.db"))


def run_command(client: RedunClient, argv: List[str]) -> str:
    """
    Run redun cli command and return output as a string.
    """
    client.stdout = io.StringIO()
    client.execute(argv)
    client.stdout.seek(0)
    return client.stdout.read()


def test_cli_repos() -> None:
    """
    redun push/pull should detect nonexistent repos.
    """
    client = RedunClient()
    client.stdout = io.StringIO()

    # Look for a repo that doesn't exist
    with pytest.raises(RedunClientError):
        client.execute(["redun", "push", "--repo", "nonexistent"])

    with pytest.raises(RedunClientError):
        client.execute(["redun", "pull", "--repo", "nonexistent"])


@use_tempdir
@mock.patch.dict(os.environ, {REDUN_CONFIG_ENV: "."})
@mock_s3
def test_add_list_s3_repos() -> None:
    """
    Tests adding and listing repos from S3.
    """
    client = RedunClient()

    s3_client = boto3.client("s3", region_name="us-east-1")
    s3_client.create_bucket(Bucket="example-repo")

    file = File("s3://example-repo/potato/redun.ini")
    file.write(DEFAULT_REDUN_INI.format(db_uri="sqlite:///other.db"))

    with pytest.raises(FileNotFoundError):
        client.execute(["redun", "repo", "add", "carrots", "s3://example-repo/carrots"])

    client.execute(["redun", "repo", "add", "potato", "s3://example-repo/potato"])

    output = run_command(client, ["redun", "repo", "list"]).split("\n")
    assert "  potato: s3://example-repo/potato" in output


@use_tempdir
@mock.patch.dict(os.environ, {REDUN_CONFIG_ENV: "."})
def test_add_list_remove_repo() -> None:
    """
    Tests adding and removing repos from the config file.
    """
    client = RedunClient()
    os.mkdir("the_repo")

    # No redun.ini in directory
    with pytest.raises(FileNotFoundError):
        client.execute(["redun", "repo", "add", "foo", "the_repo"])
    repos = run_command(client, ["redun", "repo", "list"])
    assert "foo" not in repos

    # Create the config, then it should work
    make_config("the_repo")
    client.execute(["redun", "repo", "add", "foo", "the_repo"])
    repos = run_command(client, ["redun", "repo", "list"])
    assert "the_repo" in repos

    # Duplicate additions should not work.
    with pytest.raises(RedunClientError):
        client.execute(["redun", "repo", "add", "foo", "the_repo"])

    # Adding repository under a different name should print a message.
    output = run_command(client, ["redun", "repo", "add", "bar", "the_repo"])
    assert output == f"Existing repository 'foo' also uses config at {os.getcwd()}/the_repo\n"

    # Adding default repository under a different name should print a message.
    output = run_command(client, ["redun", "repo", "add", "oops", "."])
    assert output == f"Existing repository 'default' also uses config at {os.getcwd()}\n"

    # Repositories cannot be named 'default'.
    with pytest.raises(RedunClientError):
        client.execute(["redun", "repo", "add", "default", "the_repo"])

    # Removing nonexistent repo should not work.
    with pytest.raises(RedunClientError):
        client.execute(["redun", "repo", "remove", "nonexistent"])

    # Removing repo isn't fully implemented.
    with pytest.raises(RedunClientError):
        client.execute(["redun", "repo", "remove", "foo"])


@use_tempdir
@mock.patch.dict(os.environ, {REDUN_CONFIG_ENV: "."})
def test_repo_sync() -> None:
    """
    redun push/pull should synchronize the expected number of records.
    """
    file = File("workflow.py")
    file.write(
        """
from redun import task
redun_namespace = 'test'

@task()
def task1(x: int):
    return x + 1

@task()
def carrots(x: int):
    return f"I have {x} carrots"
    """
    )

    # Set up another repo
    os.mkdir("the_repo")
    make_config("the_repo")
    other_client = RedunClient()
    other_client.execute(["redun", "-c", "the_repo", "init"])

    client = RedunClient()
    client.execute(["redun", "repo", "add", "other", "the_repo"])

    # Run a job locally and in other repo.
    client.execute(["redun", "--repo", "other", "run", "workflow.py", "carrots", "--x", "10"])
    client.execute(["redun", "run", "workflow.py", "task1", "--x", "12"])

    # Commands should not work on nonexistent repos.
    with pytest.raises(RedunClientError):
        client.execute(["redun", "--repo", "nonexistent", "log"])
    with pytest.raises(RedunClientError):
        client.execute(["redun", "push", "nonexistent"])
    with pytest.raises(RedunClientError):
        client.execute(["redun", "pull", "nonexistent"])

    # Check logs.
    lines = run_command(client, ["redun", "log"]).split("\n")
    assert_match_lines(
        [
            r"Recent executions:",
            r"",
            r"Exec ........ \[ DONE \] ....-..-.. ..:..:..:  run workflow.py task1 --x 12 \(.*\)",
            r"",
        ],
        lines,
    )
    lines_other = run_command(client, ["redun", "--repo", "other", "log"]).split("\n")
    assert_match_lines(
        [
            r"Recent executions:",
            r"",
            r"Exec ........ \[ DONE \] ....-..-.. ..:..:..:  "
            r"--repo other run workflow.py carrots --x 10 \(.*\)",
            r"",
        ],
        lines_other,
    )

    # Sync repos local -> other
    lines = run_command(client, ["redun", "pull", "other"]).split("\n")
    assert_match_lines(
        [r"Pulled \d+ new record\(s\) from repo 'other'", ""],
        lines,
    )

    # Should now have cached value from sync.
    client.execute(["redun", "run", "workflow.py", "carrots", "--x", "10"])
    lines = run_command(client, ["redun", "log", "--task"]).split("\n")
    assert_match_lines(
        ["Task ........ test.task1 ", "Task ........ test.carrots ", ""],
        lines,
    )

    # Should not be able to sync a repo to itself.
    with pytest.raises(RedunClientError):
        client.execute(["redun", "--repo", "other", "push", "other"])

    # Now push to the remote
    lines = run_command(client, ["redun", "push", "other"]).split("\n")
    assert_match_lines(
        [r"Pushed \d+ record\(s\) to repo 'other'", ""],
        lines,
    )
    lines = run_command(client, ["redun", "--repo", "other", "log", "--exec"]).split("\n")
    assert_match_lines(
        [
            r"Exec ........ \[ DONE \] ....-..-.. ..:..:..:  "
            r"run workflow.py carrots --x 10 \(.*\)",
            r"Exec ........ \[ DONE \] ....-..-.. ..:..:..:  run workflow.py task1 --x 12 \(.*\)",
            r"Exec ........ \[ DONE \] ....-..-.. ..:..:..:  "
            r"--repo other run workflow.py carrots --x 10 \(.*\)",
            "",
        ],
        lines,
    )
    # Subsequent push should sync 0 new records.
    output = run_command(client, ["redun", "push", "other"])
    assert output == "Repo 'other' is up to date.\n"
