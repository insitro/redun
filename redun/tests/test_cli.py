import io
import json
import os
import pickle
import sys
import time
from socket import socket
from typing import Any, List, cast
from unittest.mock import Mock, patch

import boto3
import botocore
import pytest
from freezegun import freeze_time
from moto import mock_logs
from sqlalchemy.orm import Session

import redun
from redun import File, Scheduler, task
from redun.backends.db import CallNode, Execution, Job, RedunBackendDb, Value
from redun.backends.db.query import find_file
from redun.cli import (
    REDUN_CONFIG_ENV,
    CallGraphQuery,
    RedunClient,
    RedunClientError,
    check_version,
    find_config_dir,
    get_config_dir,
    import_script,
    is_port_in_use,
    parse_func_path,
    setup_scheduler,
)
from redun.executors.aws_batch import BATCH_LOG_GROUP
from redun.executors.code_packaging import create_tar
from redun.job_array import AWS_ARRAY_VAR
from redun.scheduler import Traceback
from redun.scheduler_config import get_abs_db_uri
from redun.tags import ANY_VALUE
from redun.tests.utils import assert_match_lines, use_tempdir
from redun.utils import get_import_paths, pickle_dump
from redun.value import get_type_registry


def test_check_version() -> None:
    """
    check_version() should handle all specification formats.
    """
    # Test all comparisons.
    assert check_version("1.0", "1.0")
    assert check_version("1.0", "==1.0")
    assert check_version("1.0", ">=1.0")
    assert check_version("1.0", ">=0.9")
    assert check_version("1.0", ">0.9")
    assert check_version("1.0", "<=1.9")
    assert check_version("1.0", "<1.9")

    # Test all negative comparisons.
    assert not check_version("1.1", "1.0")
    assert not check_version("1.1", "==1.0")
    assert not check_version("0.5", ">=1.0")
    assert not check_version("0.9", ">0.9")
    assert not check_version("2.0", "<=1.9")
    assert not check_version("1.9", "<1.9")

    # Test differences in version parts.
    assert check_version("1", "<2.0")
    assert check_version("1.0", "<2")
    assert check_version("1.2.0", ">1.1")

    # Test double digit versions.
    assert check_version("10", ">1")

    # Test multiple specifications.
    assert check_version("1.0", ">0.9,<2")
    assert not check_version("2.0", ">0.9,<2")

    with pytest.raises(ValueError):
        check_version("boom", "0.9")

    with pytest.raises(ValueError):
        check_version("1.0", "boom")


def test_db_uri() -> None:
    """
    db_uri should be relative to the config_dir.
    """
    cwd = "/home/user/code/a/b"
    config_dir = "../../.redun"

    assert (
        get_abs_db_uri("sqlite:///redun.db", config_dir, cwd)
        == "sqlite:////home/user/code/.redun/redun.db"
    )
    assert get_abs_db_uri("sqlite:////a/b/redun.db", config_dir, cwd) == "sqlite:////a/b/redun.db"
    assert (
        get_abs_db_uri("sqlite://host/a/b/redun.db", config_dir, cwd)
        == "sqlite://host/a/b/redun.db"
    )


@use_tempdir
def test_get_config_dir() -> None:
    """
    get_config_dir() should respect the precedence.
    """
    # Get the default config dir.
    assert get_config_dir() == ".redun"

    # Get the CLI-specified config dir.
    assert get_config_dir("redun-config") == "redun-config"

    # Search for config dir in current directory.
    os.makedirs(".redun")
    expected_config_dir = os.path.join(os.getcwd(), ".redun")
    assert get_config_dir() == expected_config_dir

    # Search up parent directories for config dir.
    os.makedirs("aaa/bbb/ccc")
    os.chdir("aaa/bbb/ccc")
    assert get_config_dir() == expected_config_dir

    # Use environment variable for config dir.
    environ = {REDUN_CONFIG_ENV: "redun-config-env"}

    with patch("os.environ", new_callable=lambda: environ):
        get_config_dir() == "redun-config_env"


@use_tempdir
def test_find_config_dir() -> None:
    """
    Should search up directories to find config_dir.
    """
    os.makedirs(".redun")
    os.makedirs("aaa/bbb/ccc")

    expected_config_dir = os.path.join(os.getcwd(), ".redun")
    cwd = os.getcwd()
    assert find_config_dir(cwd) == expected_config_dir
    assert find_config_dir(os.path.join(cwd, "aaa/bbb/ccc")) == expected_config_dir
    assert find_config_dir(os.path.dirname(cwd)) is None


@use_tempdir
def test_setup_scheduler() -> None:
    """
    Ensure the setup of a scheduler from scratch.
    """
    setup_scheduler()

    # Ensure initial config dir and files are created.
    assert os.path.exists(".redun/redun.ini")
    assert os.path.exists(".redun/redun.db")


@use_tempdir
def test_setup_scheduler_find_config_dir() -> None:
    """
    Ensure the setup of a scheduler from scratch with higher-level config dir.
    """
    os.makedirs(".redun")
    os.makedirs("aaa/bbb/ccc")
    os.chdir("aaa/bbb/ccc")

    setup_scheduler()

    # Ensure initial config dir and files are created.
    assert os.path.exists("../../../.redun/redun.ini")
    assert os.path.exists("../../../.redun/redun.db")


@use_tempdir
def test_setup_scheduler_custom_db_uri() -> None:
    """
    Ensure we can use a custom db URI.
    """
    os.makedirs(".redun")
    with open(".redun/redun.ini", "w") as out:
        out.write(
            """
[backend]
db_uri = sqlite:///redun2.db
"""
        )

    setup_scheduler()

    # Ensure initial config dir and files are created.
    assert os.path.exists(".redun/redun.ini")
    assert os.path.exists(".redun/redun2.db")
    assert not os.path.exists(".redun/redun.db")


@use_tempdir
def test_setup_scheduler_ommit_db_uri() -> None:
    """
    Ensure we use default DB URI if omitted in config.
    """
    os.makedirs(".redun")
    with open(".redun/redun.ini", "w") as out:
        out.write(
            """
[backend]
"""
        )

    setup_scheduler()

    # Ensure initial config dir and files are created.
    assert os.path.exists(".redun/redun.ini")
    assert os.path.exists(".redun/redun.db")


@use_tempdir
def test_setup_scheduler_grandfather_db_uri() -> None:
    """
    Ensure we can use a the grandfather default db URI.
    """
    os.makedirs(".redun")
    with open(".redun/redun.ini", "w") as out:
        out.write(
            """
[backend]
db_uri = sqlite:///.redun/redun.db
"""
        )

    setup_scheduler()

    # Ensure initial config dir and files are created.
    assert os.path.exists(".redun/redun.ini")
    assert os.path.exists(".redun/redun.db")


@use_tempdir
def test_import_script_filename() -> None:
    """
    import_script should be able to import a filename.
    """
    File("workflow.py").write(
        """
value = 10
"""
    )

    module: Any = import_script("workflow.py")
    assert module.value == 10


@use_tempdir
def test_import_script_filename_deep() -> None:
    """
    import_script should be able to import a filename from a deep directory.
    """
    File("mylib/scripts/workflow.py").write(
        """
import utils
value = 10
"""
    )
    File("mylib/scripts/utils.py").write(
        """
value = 20
"""
    )

    module: Any = import_script("mylib/scripts/workflow.py")
    assert module.value == 10

    # imports relative to workflow.py should work.
    assert module.utils.value == 20

    # dirname of script should be added as an import path.
    [path] = get_import_paths()
    assert path.endswith("mylib/scripts")


@use_tempdir
def test_import_script_module() -> None:
    """
    import_script should be able to import a module.
    """
    File("workflow.py").write(
        """
value = 10
"""
    )

    module: Any = import_script("workflow")
    assert module.value == 10


@use_tempdir
def test_import_script_module_deep() -> None:
    """
    import_script should be able to import a deep module.
    """
    File("lib/workflows/workflow.py").write(
        """
import lib.workflows.utils as utils

value = 10
"""
    )
    File("lib/workflows/utils.py").write(
        """
value = 20
"""
    )
    File("lib/__init__.py").touch()
    File("lib/workflows/__init__.py").touch()

    module: Any = import_script("lib.workflows.workflow")
    assert module.value == 10

    # imports within module should work.
    assert module.utils.value == 20


@patch("redun.cli.print")
@use_tempdir
def test_task_help(print_mock) -> None:
    """
    CLI should show help for task arguments.

    Both simple types (e.g. int) and generic types (e.g. List) should work.
    """
    File("workflow.py").write(
        """
from typing import List

from redun import task

@task()
def main(x: int, y: List[int]):
    return x
"""
    )

    client = RedunClient()
    client.stdout = io.StringIO()
    client.execute(["redun", "run", "workflow.py", "main", "help"])

    client.stdout.seek(0)
    output = client.stdout.read()
    assert "--x X" in output
    assert "--y Y" in output


@patch("redun.cli.print")
@use_tempdir
def test_task_help_file_annotation(print_mock) -> None:
    """
    CLI should not break when one of the task's inputs has redun.File type.
    """
    File("workflow.py").write(
        """
from typing import List

from redun import File
from redun import task

@task()
def main(x: File, y: List[File]):
    return x
"""
    )

    client = RedunClient()
    client.stdout = io.StringIO()
    client.execute(["redun", "run", "workflow.py", "main", "help"])

    client.stdout.seek(0)
    output = client.stdout.read()
    assert "--x X" in output
    assert "--y Y" in output


@use_tempdir
@patch.dict(os.environ, {AWS_ARRAY_VAR: "1"}, clear=True)
def test_oneshot_existing_output() -> None:
    """
    Ensure we can reunite with an existing output file, in array mode
    """
    File("workflow.py").write(
        """
from redun import task
@task()
def task1(x: int):
    return x + 10
"""
    )

    # Set up output file list
    output_file = File("output_files")
    with output_file.open("w") as ofile:
        json.dump(["output_file1", "output_file2"], ofile)

    # Simulate pre-existing job output.
    actual_output = File("output_file1")
    with actual_output.open("wb") as afile:
        pickle_dump("pre computed answer", afile)

    # Run oneshot job.
    client = RedunClient()
    client.execute(
        [
            "redun",
            "oneshot",
            "--array-job",
            "--output",
            output_file.path,
            "--error",
            output_file.path,
            "workflow.py",
            "task1",
            "--x",
            "10",
        ]
    )

    # Check that no compute was actually run
    assert pickle.loads(cast(bytes, actual_output.read("rb"))) == "pre computed answer"


@use_tempdir
def test_oneshot_existing_output_nocache() -> None:
    """
    Ensure we do not reunite with an existing output file if caching is off.
    """
    File("workflow.py").write(
        """
from redun import task
@task()
def task1(x: int):
    return x + 10
"""
    )

    # Simulate pre-existing job output.
    output_file = File("existing.output")
    with output_file.open("wb") as ofile:
        pickle_dump("pre computed answer", ofile)

    # Run oneshot job.
    client = RedunClient()
    client.execute(
        [
            "redun",
            "oneshot",
            "--output",
            output_file.path,
            "--no-cache",
            "workflow.py",
            "task1",
            "--x",
            "10",
        ]
    )

    # Check that task was run
    assert pickle.loads(cast(bytes, output_file.read("rb"))) == 20


@patch("argparse.ArgumentParser.exit")
@patch("redun.cli.print")
@use_tempdir
def test_task_parse_arg(print_mock, arg_exit_mock) -> None:
    """
    CLI should be able to parse task arguments.
    """
    File("workflow.py").write(
        """
from enum import Enum
from typing import Callable, List, Optional
from redun import task

@task()
def task1(x: str, y):
    return (x, y)

@task()
def main(x: int = 10, y: bool = True):
    return (x, y)

class Color(Enum):
    red = 1
    green = 2
    blue = 3

@task()
def multiply(numbers: List[int], m: int) -> List[int]:
    return [n * m for n in numbers]

@task()
def apply_not(values: List[bool]) -> List[bool]:
    return [not v for v in values]

@task()
def color_to_string(colors: List[Color]) -> List[str]:
    return [str(c) for c in colors]


@task()
def optional_arg(x: Optional[int] = None) -> int:
    if not x:
        return -1
    return 10 * x

def hello(name: str) -> str:
    return f"Hello, {name}!"

@task()
def call_hello(func: Callable[[str], str]) -> str:
    # We should be able to pass functions for a Callable that specifies its
    # argument and result types.
    return func("Alice")

@task()
def call_hello2(func: Callable) -> str:
    # We should be able to pass functions for a Callable that does not specify its
    # argument or result types.
    return func("Alice")
"""
    )

    def arg_exit(status=0, message=None):
        raise ValueError(message)

    arg_exit_mock.side_effect = arg_exit
    client = RedunClient()

    # Use default argument.
    assert client.execute(["redun", "run", "workflow.py", "main"]) == (10, True)

    # Parse argument to int.
    assert client.execute(["redun", "run", "workflow.py", "main", "--x", "20"]) == (20, True)

    # Parse argument to int and bool.
    assert client.execute(
        ["redun", "run", "workflow.py", "main", "--x", "20", "--y", "false"]
    ) == (20, False)

    # Catch invalid arguments.
    with pytest.raises(ValueError, match="argument --x: invalid int value: 'boom'"):
        assert client.execute(["redun", "run", "workflow.py", "main", "--x", "boom"])

    # Parse argument to int and bool.
    assert client.execute(
        ["redun", "run", "workflow.py", "task1", "--x", "hello", "--y", "there"]
    ) == ("hello", "there")

    # Parse argument as list of ints.
    assert client.execute(
        ["redun", "run", "workflow.py", "multiply", "--numbers", "1", "2", "3", "--m", "30"]
    ) == [30, 60, 90]

    # Parse argument as list of bools.
    assert client.execute(
        ["redun", "run", "workflow.py", "apply_not", "--values", "true", "False", "TRUE"]
    ) == [False, True, False]

    # Parse argument as list of enums.
    assert client.execute(
        ["redun", "run", "workflow.py", "color_to_string", "--colors", "Color.red", "blue"]
    ) == ["Color.red", "Color.blue"]

    # Confirm list args are validated against their type.
    with pytest.raises(ValueError, match="argument --colors: invalid Color value"):
        # black is not a color in the enum, should raise an error.
        assert client.execute(
            ["redun", "run", "workflow.py", "color_to_string", "--colors", "Color.red", "black"]
        )

    # Confirm optional args are not required by the CLI
    assert client.execute(["redun", "run", "workflow.py", "optional_arg"]) == -1

    # Confirm optional args are typed correctly when supplied in the CLI
    assert client.execute(["redun", "run", "workflow.py", "optional_arg", "--x", "2"]) == 20

    # Parse argument as a function.
    assert (
        client.execute(["redun", "run", "workflow.py", "call_hello", "--func", "workflow.hello"])
        == "Hello, Alice!"
    )
    assert (
        client.execute(["redun", "run", "workflow.py", "call_hello2", "--func", "workflow.hello"])
        == "Hello, Alice!"
    )

    # Detect errors when parsing a function name.
    with pytest.raises(
        ValueError, match="argument --func: invalid function value: 'bad_workflow.hello'"
    ):
        client.execute(
            ["redun", "run", "workflow.py", "call_hello2", "--func", "bad_workflow.hello"]
        )

    with pytest.raises(
        ValueError, match="argument --func: invalid function value: 'workflow.bad_func'"
    ):
        client.execute(
            ["redun", "run", "workflow.py", "call_hello2", "--func", "workflow.bad_func"]
        )

    with pytest.raises(ValueError, match="argument --func: invalid function value: 'hello'"):
        # Module ('workflow') must be included for the argument `--func`.
        client.execute(["redun", "run", "workflow.py", "call_hello2", "--func", "hello"])


@use_tempdir
def test_invalid_config_args_raises() -> None:
    """
    Tasks with config_args not matching args/kwargs should raise an error.
    """
    File("workflow.py").write(
        """
from redun.task import task
@task(config_args=["this_is_not_valid"])
def add(n1: int, n2: int):
    return n1 + n2
"""
    )

    client = RedunClient()
    with pytest.raises(ValueError):
        client.execute(["redun", "run", "workflow.py", "add", "--n1", "1", "--n2", "10"])


@patch("argparse.ArgumentParser.exit")
@patch("redun.cli.print")
@use_tempdir
def test_task_parse_config_arg(print_mock, arg_exit_mock) -> None:
    """
    CLI should be able to parse task arguments.
    """
    File("workflow.py").write(
        """
from redun import task
@task(config_args=["memory"])
def main(x: int = 10, memory: int = 4):
    return (x, memory)
"""
    )

    def arg_exit(status=0, message=None):
        raise ValueError(message)

    arg_exit_mock.side_effect = arg_exit
    client = RedunClient()

    # Use default config arg.
    assert client.execute(["redun", "run", "workflow.py", "main"]) == (10, 4)

    # Parse config arg to int.
    assert client.execute(
        ["redun", "run", "workflow.py", "main", "--x", "20", "--memory", "5"]
    ) == (20, 5)

    # Different config arg does not force re-execution.
    assert client.execute(
        ["redun", "run", "workflow.py", "main", "--x", "20", "--memory", "10"]
    ) == (20, 5)

    # Catch invalid config arg.
    with pytest.raises(ValueError, match="argument --memory: invalid int value: 'boom'"):
        assert client.execute(
            ["redun", "run", "workflow.py", "main", "--x", "10", "--memory", "boom"]
        )


def test_parse_func_path() -> None:
    """
    `parse_func_path()` should be able to parse the pattern 'file_or_module::function_name'.
    """
    assert parse_func_path("workflow.py::setup_scheduler") == ("workflow.py", "setup_scheduler")
    assert parse_func_path("workflow::setup_scheduler") == ("workflow", "setup_scheduler")


@use_tempdir
@patch("argparse.ArgumentParser.exit")
def test_setup_func(arg_exit_mock: Mock) -> None:
    """
    RedunClient should use custom scheduler setup functions.
    """
    File("workflow.py").write(
        """\
from redun import task, Scheduler
from redun.executors.local import LocalExecutor

redun_namespace = 'acme'

@task()
def main(x: int = 10):
    return x + 1

def setup_scheduler(config, name: str = 'exec2', extra: int = 1):
    scheduler = Scheduler(config=config)
    scheduler.extra = extra
    scheduler.add_executor(LocalExecutor(name))
    return scheduler
"""
    )

    File(".redun/redun.ini").write(
        """\
[scheduler]
setup_scheduler = workflow::setup_scheduler
"""
    )

    def arg_exit(*args, **kwargs):
        raise ValueError()

    arg_exit_mock.side_effect = arg_exit
    client = RedunClient()

    # Ensure workflow can run.
    assert client.execute(["redun", "run", "workflow.py", "main"]) == 11

    # An additional executor should have been configured.
    assert client.scheduler
    assert "exec2" in client.scheduler.executors

    # Ensure pass arguments to setip func.
    client = RedunClient()
    assert (
        client.execute(
            [
                "redun",
                "--setup",
                "name=exec3",
                "--setup",
                "extra=100",
                "run",
                "workflow.py",
                "main",
            ]
        )
        == 11
    )

    # An additional executor should have been configured.
    assert client.scheduler
    assert "exec3" in client.scheduler.executors
    assert client.scheduler.extra == 100  # type: ignore

    # Ensure pass arguments to setip func.
    with pytest.raises(ValueError):
        client = RedunClient()
        client.execute(["redun", "--setup", "bad=boom", "run", "workflow.py", "main"])


@use_tempdir
def test_find_file(scheduler: Scheduler, session: Session) -> None:
    @task()
    def task1():
        file = File("out.txt")
        file.write("hello")
        return file

    scheduler.run(task1())

    row = find_file(session, "out.txt")
    assert row
    file, job, kind = row
    assert file.path == "out.txt"
    assert kind == "result"


@use_tempdir
def test_find_file_as_subvalue(scheduler: Scheduler, session: Session) -> None:
    """
    We should be able to find Files that are subvalues.
    """

    @task()
    def task1():
        file = File("out.txt")
        file.write("hello")
        # Return file as a subvalue of a list.
        return [file, 10]

    scheduler.run(task1())

    result = find_file(session, "out.txt")
    assert result
    file, job, kind = result
    assert file.path == "out.txt"
    assert kind == "result"


@use_tempdir
def test_find_file_arg(scheduler: Scheduler, session: Session) -> None:
    """
    Find a File given as an Argument to a Task.
    """

    @task()
    def task1(file):
        return 10

    @task()
    def main():
        file = File("out.txt")
        file.write("hello")
        return task1(file)

    scheduler.run(main())

    result = find_file(session, "out.txt")
    assert result
    file, job, kind = result
    assert file.path == "out.txt"
    assert kind == "arg"


@use_tempdir
def test_find_file_arg_subvalue(scheduler: Scheduler, session: Session) -> None:
    """
    Find a File given as a Subvalue in an Argument to a Task.
    """

    @task()
    def task1(file_and_int):
        return 10

    @task()
    def main():
        file = File("out.txt")
        file.write("hello")
        return task1([file, 10])

    scheduler.run(main())

    result = find_file(session, "out.txt")
    assert result
    file, job, kind = result
    assert file.path == "out.txt"
    assert kind == "arg"


@use_tempdir
def test_oneshot_code() -> None:
    """
    Ensure we can run a task from a code package.
    """
    file = File("workflow.py")
    file.write(
        """
from redun import task

@task()
def task1(x: int):
    return x + 1
"""
    )

    create_tar("code.tar.gz", ["workflow.py"])

    # Remove the original file to force the use of the code package.
    file.remove()

    client = RedunClient()
    client.execute(
        [
            "redun",
            "oneshot",
            "--code",
            "code.tar.gz",
            "--output",
            "out",
            "workflow.py",
            "task1",
            "--x",
            "10",
        ]
    )

    out = File("out")
    assert pickle.loads(cast(bytes, out.read("rb"))) == 11


@use_tempdir
def test_oneshot_shortcircuit() -> None:
    """
    Ensures oneshot can use existing output.
    """
    file = File("workflow.py")
    file.write(
        """
from redun import task

@task()
def task_hi(x: int):
    return "Hello" * x
"""
    )

    output = File("output")
    with output.open("wb") as ofile:
        pickle_dump("Hello" * 3, ofile)

    # Should return saved output.
    client = RedunClient()
    client.execute(
        ["redun", "oneshot", "--output", "output", "workflow.py", "task_hi", "--x", "2"]
    )

    assert pickle.loads(cast(bytes, output.read("rb"))) == "Hello" * 3

    # Should recalculate with no-cache.
    client = RedunClient()
    client.execute(
        [
            "redun",
            "oneshot",
            "--no-cache",
            "--output",
            "output",
            "workflow.py",
            "task_hi",
            "--x",
            "2",
        ]
    )

    assert pickle.loads(cast(bytes, output.read("rb"))) == "Hello" * 2


@use_tempdir
def test_oneshot_import() -> None:
    """
    Ensure we can oneshot a task using import path.
    """
    file = File("path/to/workflow.py")
    file.write(
        """
from redun import task

@task()
def task1(x: int):
    return x + 1
"""
    )

    client = RedunClient()
    client.stdout = io.StringIO()
    client.execute(
        [
            "redun",
            "oneshot",
            "--import-path",
            "path/to",
            "--output",
            "out",
            "workflow",
            "task1",
            "--x",
            "10",
        ]
    )

    out = File("out")
    assert pickle.loads(cast(bytes, out.read("rb"))) == 11


@use_tempdir
@patch("redun.cli.get_aws_user", side_effect=botocore.exceptions.NoCredentialsError())
@patch("redun.cli.get_simple_aws_user", side_effect=botocore.exceptions.NoCredentialsError())
@patch("redun.cli.get_gcp_user", lambda: None)
def test_run_tags(get_simple_aws_user_mock: Mock, get_aws_user_mock: Mock) -> None:
    """
    Run tags should be recorded on the execution.
    """
    os.makedirs("acme")
    os.chdir("acme")

    file = File("workflow.py")
    file.write(
        """
from redun import task

redun_namespace = "acme"

@task()
def task1(x: int):
    return x + 1
"""
    )

    client = RedunClient()
    client.execute(
        [
            "redun",
            "run",
            "--user",
            "alice",
            "--tag",
            "cost=10",
            "--tag",
            'env=["dev", "stg"]',
            "workflow.py",
            "task1",
            "--x",
            "10",
        ]
    )

    scheduler = setup_scheduler()
    assert scheduler.backend
    backend = cast(RedunBackendDb, scheduler.backend)
    assert backend.session

    execution = backend.session.query(Execution).one()
    tags = backend.get_tags([execution.id])[execution.id]
    assert tags == {
        "env": [["dev", "stg"]],
        "user": ["alice"],
        "project": ["acme"],
        "cost": [10],
        "redun.version": [redun.__version__],
    }


@use_tempdir
def test_oneshot_error() -> None:
    """
    oneshot should produce an error file when a task raises an exception.
    """
    file = File("workflow_error.py")
    file.write(
        """
from redun import task

@task()
def task_error(x: int):
    raise ValueError("boom")
"""
    )

    create_tar("code.tar.gz", ["workflow_error.py"])

    # Remove the original file to force the use of the code package.
    file.remove()

    client = RedunClient()
    with pytest.raises(ValueError):
        client.execute(
            [
                "redun",
                "oneshot",
                "--code",
                "code.tar.gz",
                "--output",
                "out",
                "--error",
                "error",
                "workflow_error.py",
                "task_error",
                "--x",
                "10",
            ]
        )

    error_file = File("error")
    error, error_traceback = pickle.loads(cast(bytes, error_file.read("rb")))

    assert isinstance(error, ValueError)
    assert isinstance(error_traceback, Traceback)


@use_tempdir
def test_oneshot_error_bad_input() -> None:
    """
    oneshot should produce an error file when input file is bad.
    """
    file = File("workflow_error.py")
    file.write(
        """
from redun import task

@task()
def task1(x: int):
    return x
"""
    )

    create_tar("code.tar.gz", ["workflow_error.py"])

    # Remove the original file to force the use of the code package.
    file.remove()

    client = RedunClient()
    with pytest.raises(FileNotFoundError):
        client.execute(
            [
                "redun",
                "oneshot",
                "--code",
                "code.tar.gz",
                "--input",
                "does_not_exist",
                "--output",
                "out",
                "--error",
                "error",
                "workflow_error.py",
                "task1",
            ]
        )

    error_file = File("error")
    error, error_traceback = pickle.loads(cast(bytes, error_file.read("rb")))

    assert isinstance(error, FileNotFoundError)
    assert isinstance(error_traceback, Traceback)


@mock_logs
@freeze_time("2020-01-01 00:00:00", tz_offset=-7)
@patch("redun.executors.aws_batch.aws_describe_jobs")
@patch("redun.cli.aws_describe_jobs")
@use_tempdir
def test_aws_logs(aws_describe_jobs_mock1, aws_describe_jobs_mock2) -> None:

    timestamp = int(time.time() * 1000)
    stream_name = "redun_aws_batch_example-redun-jd/default/6c939514f4054fdfb5ee65acc8aa4b07"
    job_descriptions = [
        {
            "jobId": "123",
            "container": {"logStreamName": stream_name},
            "startedAt": timestamp,
            "createdAt": timestamp,
        }
    ]
    aws_describe_jobs_mock1.return_value = iter(job_descriptions)
    aws_describe_jobs_mock2.return_value = iter(job_descriptions)

    # Setup logs mocks.
    logs_client = boto3.client("logs", region_name="us-west-2")
    logs_client.create_log_group(logGroupName=BATCH_LOG_GROUP)
    logs_client.create_log_stream(logGroupName=BATCH_LOG_GROUP, logStreamName=stream_name)
    resp = logs_client.put_log_events(
        logGroupName=BATCH_LOG_GROUP,
        logStreamName=stream_name,
        logEvents=[
            {"timestamp": timestamp, "message": "A message 1."},
            {"timestamp": timestamp + 1, "message": "A message 2."},
        ],
    )
    resp = logs_client.put_log_events(
        logGroupName=BATCH_LOG_GROUP,
        logStreamName=stream_name,
        logEvents=[
            {"timestamp": timestamp + 2, "message": "A message 3."},
            {"timestamp": timestamp + 3, "message": "A message 4."},
        ],
        sequenceToken=resp["nextSequenceToken"],
    )

    File(".redun/redun.ini").write(
        """
[executors.batch]
type = aws_batch
image = my-image
queue = my-queue
s3_scratch = s3://bucket/scratch/
"""
    )

    client = RedunClient()
    client.execute(["redun", "init"])
    client.stdout = io.StringIO()
    client.execute(
        [
            "redun",
            "aws",
            "logs",
            "--batch-job",
            "123",
        ]
    )

    client.stdout.seek(0)

    lines = client.stdout.readlines()
    assert lines == [
        "# AWS Batch job 123\n",
        "# AWS Log Stream redun_aws_batch_example-redun-jd/default/6c939514f4054fdfb5ee65acc8aa4b07\n",  # noqa:E501
        "2019-12-31 17:00:00  A message 1.\n",
        "2019-12-31 17:00:00.001000  A message 2.\n",
        "2019-12-31 17:00:00.002000  A message 3.\n",
        "2019-12-31 17:00:00.003000  A message 4.\n",
        "\n",
    ]


@use_tempdir
def test_project_tag() -> None:
    """
    Run tags should be recorded on the execution.
    """
    os.makedirs("acme")
    os.chdir("acme")

    file = File("workflow.py")
    file.write(
        """
from redun import task

@task()
def task1(x: int):
    return x + 1
"""
    )

    client = RedunClient()
    client.execute(
        ["redun", "run", "--project", "my-project", "workflow.py", "task1", "--x", "10"]
    )

    scheduler = setup_scheduler()
    assert scheduler.backend
    backend = cast(RedunBackendDb, scheduler.backend)
    assert backend.session

    execution = backend.session.query(Execution).one()
    tags = backend.get_tags([execution.id])[execution.id]
    assert tags["project"] == ["my-project"]


@use_tempdir
def test_cloud_user_tags() -> None:
    """
    Run cloud (AWS, GCP) user tags should be recorded on the execution.
    """
    file = File("workflow.py")
    file.write(
        """
from redun import task

@task()
def task1(x: int):
    return x + 1
"""
    )

    with patch("redun.cli.get_aws_user", lambda _: "arn:::::alice"), patch(
        "redun.cli.get_simple_aws_user", lambda _: "alice"
    ), patch("redun.cli.get_gcp_user", lambda: "alice@gmail.com"):
        client = RedunClient()
        client.execute(["redun", "run", "workflow.py", "task1", "--x", "10"])

    scheduler = setup_scheduler()
    assert scheduler.backend
    backend = cast(RedunBackendDb, scheduler.backend)
    assert backend.session

    execution = backend.session.query(Execution).one()
    tags = backend.get_tags([execution.id])[execution.id]
    assert tags["user_aws"] == ["alice"]
    assert tags["user_aws_arn"] == ["arn:::::alice"]
    assert tags["user_gcp"] == ["alice@gmail.com"]


def test_query(scheduler: Scheduler, backend: RedunBackendDb, session: Session) -> None:
    """
    CallGraphQuery should query records correctly.
    """

    @task(tags=[("step", "calc")])
    def task1(x):
        return x + 1

    assert scheduler.run(task1(10), tags=[("project", "acme")]) == 11

    # All entities should be part of query.
    query = CallGraphQuery(session)
    assert len(list(query.all())) == 7
    assert len(list(query.order_by("time").all())) == 7
    assert list(query.count()) == [
        ("Execution", 1),
        ("Job", 1),
        ("CallNode", 1),
        ("Task", 1),
        ("Value", 3),
    ]

    # Select ids.
    ids = list(query.select("id", flat=True))
    assert task1.hash in ids

    # Select type and ids.
    type_ids = list(query.select("type", "id"))
    assert ("Task", task1.hash) in type_ids

    # Filter by type.
    execution = query.filter_types(["Execution"]).one()
    assert isinstance(execution, Execution)
    # ensure the correct type is returned when ordering query is added
    execution_ordered = query.order_by("time").filter_types(["Execution"]).one()
    assert isinstance(execution_ordered, Execution)

    call_node = query.filter_types(["CallNode"]).one()
    assert isinstance(call_node, CallNode)

    # Filter by value type.
    assert dict(query.filter_value_types(["builtins.int"]).count())["Value"] == 2

    # Filter by ids.
    assert set(
        query.filter_ids([execution.id, call_node.call_hash, task1.hash]).select("type", "id")
    ) == {
        ("Execution", execution.id),
        ("CallNode", call_node.call_hash),
        ("Task", task1.hash),
        ("Value", task1.hash),
    }

    # Filter by id prefix.
    assert query.like_id(task1.hash[:8]).filter_types(["Task"]).one().hash == task1.hash

    # Generate an extra execution.
    assert scheduler.run(task1(20)) == 21

    # Filter execution id.
    exec_id = cast(Execution, query.filter_types({"Execution"}).first()).id
    assert list(query.count()) == [
        ("Execution", 2),
        ("Job", 2),
        ("CallNode", 2),
        ("Task", 1),
        ("Value", 5),
    ]
    assert list(query.filter_execution_ids([exec_id]).count()) == [
        ("Execution", 1),
        ("Job", 1),
        ("CallNode", 1),
        ("Task", 1),
        ("Value", 2),
    ]

    # Filter by execution status.
    assert len(list(query.filter_execution_statuses(["DONE"]).all()))

    # Filter by jobs status.
    assert len(list(query.filter_job_statuses(["DONE"]).all())) == 2
    assert len(list(query.filter_job_statuses(["CACHED"]).all())) == 0
    assert len(list(query.filter_job_statuses(["FAILED"]).all())) == 0

    # Filter by tags.
    assert dict(query.filter_tags([("step", "calc")]).count()) == {
        "CallNode": 0,
        "Execution": 0,
        "Job": 2,
        "Task": 1,
        "Value": 1,
    }
    assert dict(query.filter_tags([("project", "acme")]).count()) == {
        "CallNode": 0,
        "Execution": 1,
        "Job": 0,
        "Task": 0,
        "Value": 0,
    }

    # Filter by execution tags.
    assert dict(query.filter_execution_tags([("project", "acme")]).count()) == {
        "CallNode": 1,
        "Execution": 1,
        "Job": 1,
        "Task": 1,
        "Value": 2,
    }

    # Empty query should return nothing.
    assert list(query.empty().all()) == []


def test_filter_task_names(
    scheduler: Scheduler, backend: RedunBackendDb, session: Session
) -> None:
    """
    Should be able to filter by task name.
    """

    @task(namespace="ns")
    def task1(x):
        return x + 1

    @task(namespace="ns")
    def main():
        return task1(10)

    assert scheduler.run(main()) == 11

    # All entities should be part of query.
    query = CallGraphQuery(session)

    assert len(list(query.filter_types(["Job"]).filter_task_names(["task1"]).all())) == 1
    assert len(list(query.filter_types(["Job"]).filter_task_names(["ns.task1"]).all())) == 1
    assert len(list(query.filter_types(["Task"]).filter_task_names(["task1"]).all())) == 1
    assert len(list(query.filter_types(["Job"]).filter_task_names(["task1", "main"]).all())) == 2
    assert len(list(query.filter_types(["Task"]).filter_task_names(["bad_task"]).all())) == 0


def test_value_subqueries(scheduler: Scheduler, backend: RedunBackendDb, session: Session) -> None:
    """
    CallGraphQuery should be able to query Values by Execution.
    """

    @task()
    def task1(x: Any) -> int:
        return 10

    @task()
    def task2(x: Any) -> List[File]:
        return [File("foo"), File("bar")]

    # Single Value argument and result.
    scheduler.run(task1(1))

    # List Value argument.
    scheduler.run(task1([File("alice")]))

    # List Value result.
    scheduler.run(task2(2))

    query = CallGraphQuery(session)

    # Get the three executions.
    exec3, exec2, exec1 = list(query.filter_types(["Execution"]).order_by("time").all())
    assert exec3.call_node.timestamp > exec2.call_node.timestamp > exec1.call_node.timestamp

    # We should be able to fetch the other record types from the execution.
    assert (
        query.filter_types(["Job"]).filter_execution_ids([exec1.id]).one().execution_id == exec1.id
    )
    assert query.filter_types(["Task"]).filter_execution_ids([exec1.id]).one().name == "task1"
    assert (
        query.filter_types(["CallNode"])
        .filter_execution_ids([exec1.id])
        .one()
        .jobs[0]
        .execution_id
        == exec1.id
    )

    # We should be able to fetch the Values from an Execution.
    values = query.filter_types(["Value"]).filter_execution_ids([exec1.id]).all()
    assert {value.value_parsed for value in values} == {1, 10}

    type_registry = get_type_registry()

    def assert_hashes(values1, raw_values2):
        hashes1 = {type_registry.get_hash(value.value_parsed) for value in values1}
        hashes2 = {type_registry.get_hash(value) for value in raw_values2}
        assert hashes1 == hashes2

    # We should get both the list and subvalues.
    values2 = list(query.filter_types(["Value"]).filter_execution_ids([exec2.id]).all())
    assert_hashes(values2, [10, File("alice"), [File("alice")]])

    values3 = list(query.filter_types(["Value"]).filter_execution_ids([exec3.id]).all())
    assert_hashes(values3, [2, File("foo"), File("bar"), [File("foo"), File("bar")]])


def test_query_tags(scheduler: Scheduler, backend: RedunBackendDb, session: Session) -> None:
    """
    CallGraphQuery should query tags correctly.
    """

    @task()
    def task1(x):
        return x + 1

    assert (
        scheduler.run(task1(10), tags=[("project", "acme"), ("user", "alice"), ("env", "prod")])
        == 11
    )
    assert (
        scheduler.run(task1(100), tags=[("project", "skunk"), ("user", "bob"), ("env", "prod")])
        == 101
    )

    # Get Executions for testing.
    query = CallGraphQuery(session)
    execs = (
        session.query(Execution)
        .join(Job, Job.id == Execution.job_id)
        .order_by(Job.start_time)
        .all()
    )
    exec1, exec2 = execs

    # Query by key and value.
    assert set(query.filter_tags([("project", "acme")]).all()) == {exec1}
    assert set(query.filter_tags([("env", "prod")]).all()) == {exec1, exec2}

    # Query by only key.
    assert set(query.filter_tags([("project", ANY_VALUE)]).all()) == {exec1, exec2}

    # Query by wrong key.
    assert set(query.filter_tags([("unknown", ANY_VALUE)]).all()) == set()

    # Query by two tags.
    assert set(query.filter_tags([("project", "acme"), ("user", "alice")]).all()) == {exec1}

    # Query values by exec tag.
    values = query.filter_types(["Value"]).filter_execution_tags([("project", "acme")]).all()
    assert {value.value_parsed for value in values} == {10, 11}

    values = query.filter_types(["Value"]).filter_execution_tags([("env", "prod")]).all()
    assert {value.value_parsed for value in values} == {10, 11, 100, 101}

    # Query by two exec tags.
    values = (
        query.filter_types(["Value"])
        .filter_execution_tags([("project", "acme"), ("user", "alice")])
        .all()
    )
    assert {value.value_parsed for value in values} == {10, 11}


def run_command(client: RedunClient, argv: List[str]) -> str:
    """
    Run redun cli command and return output as a string.
    """
    client.stdout = io.StringIO()
    client.execute(argv)
    client.stdout.seek(0)
    return client.stdout.read()


@use_tempdir
def test_cli_log() -> None:
    """
    cli should allow querying the CallGraph.
    """
    file = File("workflow.py")
    file.write(
        """
import time

from redun import task

redun_namespace = 'test'

@task()
def task1(x: int):
    return x + 1

@task()
def main(x: int):
    return task1(x)
"""
    )

    # Run example workflow.
    client = RedunClient()
    client.execute(["redun", "run", "workflow.py", "main", "--x", "10"])
    client.execute(["redun", "run", "workflow.py", "main", "--x", "11"])
    client.execute(["redun", "run", "workflow.py", "main", "--x", "10"])

    # Show just executions.
    lines = run_command(client, ["redun", "log"]).split("\n")
    assert_match_lines(
        [
            r"Recent executions:",
            r"",
            r"Exec ........ \[ DONE \] ....-..-.. ..:..:..:  run workflow.py main --x 10 \(.*\)",
            r"Exec ........ \[ DONE \] ....-..-.. ..:..:..:  run workflow.py main --x 11 \(.*\)",
            r"Exec ........ \[ DONE \] ....-..-.. ..:..:..:  run workflow.py main --x 10 \(.*\)",
            r"",
        ],
        lines,
    )

    # Filter by record type.
    lines = run_command(client, ["redun", "log", "--job"]).split("\n")
    assert_match_lines(
        [
            r"Job ........ \[CACHED\] ....-..-.. ..:..:..:  test.task1\(10\) ",
            r"Job ........ \[CACHED\] ....-..-.. ..:..:..:  test.main\(x=10\) ",
            r"Job ........ \[ DONE \] ....-..-.. ..:..:..:  test.task1\(11\) ",
            r"Job ........ \[ DONE \] ....-..-.. ..:..:..:  test.main\(x=11\) ",
            r"Job ........ \[ DONE \] ....-..-.. ..:..:..:  test.task1\(10\) ",
            r"Job ........ \[ DONE \] ....-..-.. ..:..:..:  test.main\(x=10\) ",
            "",
        ],
        lines,
    )

    lines = run_command(client, ["redun", "log", "--task"]).split("\n")
    assert_match_lines(
        ["Task ........ test.main ", "Task ........ test.task1 ", ""],
        lines,
    )

    lines = run_command(client, ["redun", "log", "--exec"]).split("\n")
    assert_match_lines(
        [
            r"Exec ........ \[ DONE \] ....-..-.. ..:..:..:  run workflow.py main --x 10 \(.*\)",
            r"Exec ........ \[ DONE \] ....-..-.. ..:..:..:  run workflow.py main --x 11 \(.*\)",
            r"Exec ........ \[ DONE \] ....-..-.. ..:..:..:  run workflow.py main --x 10 \(.*\)",
            "",
        ],
        lines,
    )

    # Filter by exec id.
    lines = run_command(client, ["redun", "log", "--exec"]).split("\n")
    exec_id = lines[0].split()[1]
    lines = run_command(client, ["redun", "log", exec_id]).split("\n")
    assert_match_lines(
        [
            r"Exec .* \[ DONE \] ....-..-.. ..:..:..:  run workflow.py main --x 10 \(.*\)",
            r"Duration: 0:00:..\...",
            r"",
            r"\| JOB STATUS ..../../.. ..:..:..",
            r"\| TASK       PENDING RUNNING  FAILED  CACHED    DONE   TOTAL",
            r"\| ",
            r"\| ALL              0       0       0       2       0       2",
            r"\| test\.main        0       0       0       1       0       1",
            r"\| test\.task1       0       0       0       1       0       1",
            r"",
            r"Jobs: 2 \(DONE: 0, CACHED: 2, FAILED: 0\)",
            r"--------------------------------------------------------------------------------",
            r"Job ........ \[CACHED\] ....-..-.. ..:..:..:  test.main\(x=10\) ",
            r"  Job ........ \[CACHED\] ....-..-.. ..:..:..:  test.task1\(10\) ",
            r"",
        ],
        lines,
    )

    # Filter by most recent execution id.
    lines = run_command(client, ["redun", "log", "-"]).split("\n")
    assert_match_lines(
        [
            r"Exec .* \[ DONE \] ....-..-.. ..:..:..:  run workflow.py main --x 10 \(.*\)",
            r"Duration: 0:00:..\...",
            r"",
            r"\| JOB STATUS ..../../.. ..:..:..",
            r"\| TASK       PENDING RUNNING  FAILED  CACHED    DONE   TOTAL",
            r"\| ",
            r"\| ALL              0       0       0       2       0       2",
            r"\| test\.main        0       0       0       1       0       1",
            r"\| test\.task1       0       0       0       1       0       1",
            r"",
            r"Jobs: 2 \(DONE: 0, CACHED: 2, FAILED: 0\)",
            r"--------------------------------------------------------------------------------",
            r"Job ........ \[CACHED\] ....-..-.. ..:..:..:  test.main\(x=10\) ",
            r"  Job ........ \[CACHED\] ....-..-.. ..:..:..:  test.task1\(10\) ",
            r"",
        ],
        lines,
    )

    # Filter jobs by execution id.
    lines = run_command(client, ["redun", "log", "--job", "--exec-id", exec_id]).split("\n")
    assert_match_lines(
        [
            r"Job ........ \[CACHED\] ....-..-.. ..:..:..:  test.task1\(10\) ",
            r"Job ........ \[CACHED\] ....-..-.. ..:..:..:  test.main\(x=10\) ",
            "",
        ],
        lines,
    )

    # Job detail.
    lines = run_command(client, ["redun", "log", "--job"]).split("\n")
    job_id = lines[0].split()[1]
    lines = run_command(client, ["redun", "log", job_id]).split("\n")
    assert_match_lines(
        [
            r"Job .* \[CACHED\] ....-..-.. ..:..:..:  test.task1\(10\) ",
            r"Traceback: Exec ........ > Job ........ main > Job ........ task1",
            r"",
            r"  CallNode .* test.task1 ",
            r"    Args:   10",
            r"    Result: 11",
            r"",
            r"  Task .* test.task1 ",
            r"",
            r"    def task1\(x: int\):",
            r"        return x \+ 1",
            r"",
            r"",
            r"  Upstream dataflow:",
            r"",
            r"    result = 11",
            r"",
            r"    result <-- <........> task1\(x\)",
            r"      x = <........> 10",
            r"",
            r"    x <-- argument of <........> main\(x\)",
            r"      <-- origin",
            r"",
            r"",
        ],
        lines,
    )


@use_tempdir
@patch("redun.cli.get_aws_user", side_effect=botocore.exceptions.NoCredentialsError())
@patch("redun.cli.get_simple_aws_user", side_effect=botocore.exceptions.NoCredentialsError())
@patch("redun.cli.get_gcp_user", lambda: None)
def test_cli_log_tags(get_simple_aws_user_mock: Mock, get_aws_user_mock: Mock) -> None:
    """
    cli should allow querying the CallGraph for tags.
    """
    file = File("workflow.py")
    file.write(
        """
import time

from redun import task

redun_namespace = 'test'

@task(tags=[("step", "calc")])
def task1(x: int) -> int:
    return x + 1

@task()
def main(x: int) -> int:
    return task1(x)
"""
    )

    # Run example workflow.
    client = RedunClient()
    client.execute(
        [
            "redun",
            "run",
            "--user",
            "alice",
            "workflow.py",
            "task1",
            "--x",
            "10",
        ]
    )

    scheduler = setup_scheduler()
    assert scheduler.backend
    backend = cast(RedunBackendDb, scheduler.backend)
    assert backend.session

    execution = backend.session.query(Execution).one()

    # Query all tags.
    output = run_command(client, ["redun", "tag", "list"])
    assert (
        output == "Tags:\n  project    (1 Execution)\n"
        "  redun.version (1 Execution)\n"
        "  step       (1 Job, 1 Task)\n"
        "  user       (1 Execution)\n"
    )

    # Query specific tag key.
    output = run_command(client, ["redun", "tag", "list", "step"])
    assert "test.task1 (step=calc)" in output

    # Query tag key=value.
    output = run_command(client, ["redun", "tag", "list", "step=calc"])
    assert "test.task1 (step=calc)" in output

    output = run_command(client, ["redun", "tag", "list", "step=unknown"])
    assert "test.task1" not in output

    # Add a tag.
    output = run_command(client, ["redun", "tag", "add", execution.id, "env=prod"])
    output = run_command(client, ["redun", "tag", "list", "env=prod"])
    assert "Exec " in output

    # Remove a tag.
    output = run_command(client, ["redun", "tag", "rm", execution.id, "env=prod"])
    output = run_command(client, ["redun", "tag", "list", "env=prod"])
    assert "Exec " not in output

    # Add a tag back.
    output = run_command(client, ["redun", "tag", "add", execution.id, "env=prod"])
    output = run_command(client, ["redun", "tag", "list", "env=prod"])
    assert "Exec " in output

    # Update a tag.
    output = run_command(client, ["redun", "tag", "update", execution.id, "env=prod2"])
    output = run_command(client, ["redun", "tag", "list", "env=prod2"])
    assert "Exec " in output


@use_tempdir
def test_cli_log_tags_order() -> None:
    """
    redun log entries should have tags sorted in a stable way, even if their values are not
    comparable directly.
    """
    file = File("workflow.py")
    file.write(
        """
from redun import task
from redun.scheduler import apply_tags

redun_namespace = 'test'

@task()
def example1():
    return apply_tags(
        42,
        execution_tags=[("key", {"b": 1}), ("key", {"a": 2})]
    )

@task()
def example2():
    return apply_tags(
        42,
        execution_tags=[("key", {"c": 2}), ("key", {"c": 1})]
    )

"""
    )

    # Run example workflow.
    client = RedunClient()
    client.execute(["redun", "run", "workflow.py", "example1"])
    client.execute(["redun", "run", "workflow.py", "example2"])

    lines = run_command(client, ["redun", "log"]).split("\n")

    assert_match_lines(
        [
            r"Recent executions:",
            r"",
            r"Exec.*key={\"c\": 1}, key={\"c\": 2}.*",
            r"Exec.*key={\"a\": 2}, key={\"b\": 1}.*",
            r"",
        ],
        lines,
    )


@use_tempdir
def test_cli_log_files() -> None:
    """
    cli should support querying for files.
    """
    file = File("workflow.py")
    file.write(
        """
from redun import task, File

@task()
def task1(filename: str):
    file = File(filename)
    file.write('hello')
    return file

@task()
def main(filename: str):
    return task1(filename)
"""
    )

    # Run example workflow.
    client = RedunClient()
    client.execute(["redun", "run", "workflow.py", "main", "--filename", "out"])
    client.execute(["redun", "run", "workflow.py", "main", "--filename", "out2"])
    client.execute(["redun", "run", "workflow.py", "main", "--filename", "out"])

    # Query File type.
    lines = run_command(client, ["redun", "log", "--file"]).split("\n")
    assert_match_lines(
        [
            "File ........ out ",
            "File ........ out2 ",
            "",
        ],
        lines,
    )

    # Query Files by path.
    lines = run_command(client, ["redun", "log", "--file-path", "out"]).split("\n")
    assert_match_lines(
        [r"Value ........ redun.File File\(path=out, hash=........\) ", ""],
        lines,
    )

    # Query file provenance.
    lines = run_command(client, ["redun", "log", "out"]).split("\n")
    assert_match_lines(
        [
            r"File ........ out ",
            r"Produced by Job ........",
            r"",
            r"  Job .* \[ DONE \] ....-..-.. ..:..:..:  task1\('out'\) ",
            r"  Traceback: Exec ........ > Job ........ main > Job ........ task1",
            r"  Duration: 0:00:..\...",
            r"",
            r"    CallNode .* task1 ",
            r"      Args:   'out'",
            r"      Result: File\(path=out, hash=........\)",
            r"",
            r"    Task 8106568b9069e7b743d827a7167bec59bb86d78a task1 ",
            r"",
            r"      def task1\(filename: str\):",
            r"          file = File\(filename\)",
            r"          file.write\('hello'\)",
            r"          return file",
            r"",
            r"",
            r"    Upstream dataflow:",
            r"",
            r"      result = File\(path=out, hash=........\)",
            r"",
            r"      result <-- <........> task1\(filename\)",
            r"        filename = <........> 'out'",
            r"",
            r"      filename <-- argument of <........> main\(filename\)",
            r"               <-- origin",
            r"",
            r"",
        ],
        lines,
    )


def test_port_usage_check() -> None:
    taken_port = 65500
    open_port = 65501

    # Ensure that bound ports are detected as in use
    with socket() as sock1:
        sock1.bind(("localhost", taken_port))
        assert is_port_in_use("localhost", taken_port)
        assert is_port_in_use("localhost", str(taken_port))

    # Ensure that open ports are detected
    assert not is_port_in_use("localhost", open_port)
    # Ensure that the detection process itself doesn't bind the port
    assert not is_port_in_use("localhost", open_port)

    with pytest.raises(ValueError):
        is_port_in_use("localhost", "bla")


@use_tempdir
def test_cli_migrate() -> None:
    """
    cli should work with database migrations.
    """
    file = File("workflow.py")
    file.write(
        """
from redun import task

@task()
def main() -> int:
    return 10
"""
    )

    # Run example workflow.
    client = RedunClient()
    assert client.execute(["redun", "run", "workflow.py", "main"]) == 10
    backend = RedunBackendDb(db_uri="sqlite:///.redun/redun.db")
    backend.create_engine()

    # New database should be current.
    assert backend.is_db_compatible()

    # Should be able to get db info.
    lines = run_command(client, ["redun", "db", "info"]).split("\n")
    assert_match_lines(
        [
            r"redun :: version .*",
            r"config dir: .*",
            r"",
            r"db version: \d+(\.\d+)? '.*'",
            r"CLI requires db versions: >=[0-9.]+,<\d+",
            r"CLI compatible with db: True",
            r"",
        ],
        lines,
    )

    # Should be able to downgrade the database.
    client = RedunClient()
    client.execute(["redun", "db", "downgrade", "1"])

    # Database should now be incompatible.
    client = RedunClient()
    client.execute(["redun", "db", "info"])
    backend = RedunBackendDb(db_uri="sqlite:///.redun/redun.db")
    backend.create_engine()
    assert not backend.is_db_compatible()

    # redun log should be prevented.
    with pytest.raises(RedunClientError):
        client.execute(["redun", "log"])

    # By default redun run automigrates.
    client = RedunClient()
    assert client.execute(["redun", "run", "workflow.py", "main"]) == 10
    backend = RedunBackendDb(db_uri="sqlite:///.redun/redun.db")
    backend.create_engine()

    # database should have automigrated.
    assert backend.is_db_compatible()


@use_tempdir
def test_cli_automigrate_postgres() -> None:
    """
    Postgres databases should not automigrate.
    """
    File(".redun2/redun.ini").write(
        """
[backend]
db_uri = postgresql://host/db
"""
    )

    client = RedunClient()
    parser = client.get_command_parser()
    args = parser.parse_args(["-c", ".redun2", "run", "workflow.py", "main"])

    with patch.object(Scheduler, "load") as load_mock:
        client.get_scheduler(args, migrate_if_local=True)

    load_mock.assert_called_with(migrate=False)


@use_tempdir
def test_rerun() -> None:
    """
    cli should allow re-running of subgraphs
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
def main(x: int, y: int):
    return task1(x) + 1 + y
"""
    )

    client = RedunClient()

    result = client.execute(["redun", "run", "workflow.py", "main", "--x", "10", "--y", "5"])
    assert result == 17

    assert client.scheduler
    scheduler = client.scheduler
    assert scheduler.backend
    backend = cast(RedunBackendDb, scheduler.backend)
    assert backend.session

    # Ensure that we can rerun from multiple jobs in the call graph.
    jobs = backend.session.query(Job).all()
    assert len(jobs) == 2
    for job in jobs:
        result = client.execute(["redun", "run", "--rerun", "workflow.py", job.id])
        if job.parent_id:
            assert result == 11
        else:
            assert result == 17

    # Ensure that we can specify execution ids as well as job ids
    execution = backend.session.query(Execution).first()
    result = client.execute(["redun", "run", "--rerun", "workflow.py", execution.id])
    assert result == 17

    result = client.execute(["redun", "run", "--rerun", "workflow.py", execution.id, "--y", "10"])
    assert result == 22


@use_tempdir
def test_run_packed_inputs() -> None:
    """
    Test that `run` accepts binary inputs w/ the --input flag.
    """
    file = File("workflow.py")
    file.write(
        """
from redun import task

redun_namespace = 'test'

@task()
def main(x: int, y: int = 0):
    # Note: the default value y=0 is important to the test, this is a corner case of the CLI.
    return x + y
"""
    )

    client = RedunClient()

    with File("inputs").open("wb") as out:
        pickle_dump([(5,), {"y": 7}], out)

    result = client.execute(["redun", "run", "workflow.py", "main", "--input", "inputs"])
    assert result == 12

    # We should still allow overrides from the cmdline.
    result = client.execute(
        ["redun", "run", "workflow.py", "main", "--input", "inputs", "--y", "10"]
    )
    assert result == 15


@pytest.mark.parametrize("executor", ["proc", "thread"])
@use_tempdir
def test_launch(executor: str) -> None:
    """
    Launch subcommand should run a workflow within an executor.
    """
    config_string = """
[executors.proc]
type = local
mode = process

[executors.thread]
type = local
mode = thread
"""
    workflow = """
import os
import redun

@redun.task
def main() -> int:
    return os.getpid()
"""
    File(".redun/redun.ini").write(config_string)
    File("workflow.py").write(workflow)
    this_pid = os.getpid()
    client = RedunClient()
    client.execute(
        ["redun", "launch", "--executor", executor, "redun", "run", "workflow.py", "main"]
    )

    if (sys.version_info.major, sys.version_info.minor) == (3, 8) and executor == "proc":
        # Process executors are not shut down in py3.8, so it may still be running.
        # This is expected but we skip the test rather than try to wait it out
        return

    parser = client.get_command_parser()
    args, extra_args = parser.parse_known_args([])
    value = client.get_session(args=args).query(Value).filter(Value.type != "redun.Task").one()
    assert value.value_parsed
    assert value.value_parsed != this_pid


@pytest.mark.parametrize("executor", ["proc", "thread"])
@use_tempdir
def test_wait_launch(executor: str) -> None:
    """
    Launch with --wait which prints the value
    """
    config_string = """
[executors.proc]
type = local
mode = process

[executors.thread]
type = local
mode = thread
"""
    workflow = """
import os
import redun

@redun.task
def main() -> str:
    return "hi"
"""
    File(".redun/redun.ini").write(config_string)
    File("workflow.py").write(workflow)
    client = RedunClient()
    waited_display = run_command(
        client,
        [
            "redun",
            "launch",
            "--wait",
            "--executor",
            executor,
            "redun",
            "run",
            "workflow.py",
            "main",
        ],
    )
    lines = waited_display.split("\n")
    assert lines[-2] == "b\"'hi'\\n\""
