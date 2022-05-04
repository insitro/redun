import os
import subprocess
from configparser import ConfigParser

import boto3
import pytest
from moto import mock_s3

from redun import File, Scheduler, task
from redun.executors.aws_batch import AWSBatchExecutor
from redun.expression import TaskExpression
from redun.file import Dir
from redun.scripting import (
    ScriptError,
    exec_script,
    get_command_eof,
    get_wrapped_command,
    prepare_command,
    script,
)
from redun.tests.utils import use_tempdir


@use_tempdir
def test_redirect() -> None:
    """
    Shell redirection should use tee to create files for stderr and stdout.

    This test documents the technique works.
    """
    proc = subprocess.run(
        [
            "bash",
            "-c",
            """
            (echo hello) 2> >(tee >(cat > stderr) >&2) | tee >(cat > stdout)
            """,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert File("stdout").read() == "hello\n"
    assert File("stderr").read() == ""
    assert proc.stdout == b"hello\n"
    assert proc.stderr == b""
    assert proc.returncode == 0

    proc = subprocess.run(
        [
            "bash",
            "-c",
            "-o",
            "pipefail",
            "(bad_command) 2> >(tee >(cat > stderr) >&2) | "
            "tee >(cat > stdout) || (echo fail; exit 1)",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert File("stdout").read() == ""
    assert "command not found" in File("stderr").read()
    assert proc.stdout == b"fail\n"
    assert b"command not found" in proc.stderr
    assert proc.returncode != 0


def test_prepare_command() -> None:
    """
    Commands should be dedented.
    """
    assert (
        prepare_command(
            """
        #!/bin/bash
        echo hello
        """
        )
        == """\
#!/bin/bash
echo hello"""
    )


def test_prepare_command_default_interpreter() -> None:
    """
    Commands should be dedented.
    """
    assert (
        prepare_command(
            """
        echo hello
        """
        )
        == """\
#!/usr/bin/env bash
set -exo pipefail
echo hello"""
    )


def test_exec_script() -> None:
    """
    Execute a python script.
    """
    assert (
        exec_script(
            """\
#!/usr/bin/env python
print('hello')
"""
        )
        == b"hello\n"
    )


def test_script_task(scheduler: Scheduler) -> None:
    """
    Tasks should be definable as shell scripts.
    """

    @task(script=True)
    def task1(message):
        return """echo Hello, {message}!""".format(message=message)

    assert scheduler.run(task1("World")) == b"Hello, World!\n"


def test_script_task_error(scheduler: Scheduler) -> None:
    """
    Script tasks should raise errors from shell script.
    """

    @task(script=True)
    def task1():
        return "bad_command"

    @task(script=True)
    def task2():
        # Multi-line script should exit on first error by default.
        return """
bad_command
echo hello
        """

    with pytest.raises(ScriptError):
        scheduler.run(task1())

    with pytest.raises(ScriptError):
        scheduler.run(task2())


def test_python_script_task(scheduler: Scheduler) -> None:
    """
    Script tasks should be able to use custom interpreters.
    """

    @task(script=True)
    def task1(message):
        return """
        #!/usr/bin/env python
        print('Hello, {message}!')
        """.format(
            message=message
        )

    assert scheduler.run(task1("World")) == b"Hello, World!\n"


def test_python_script_task2(scheduler: Scheduler) -> None:
    """
    script() should use custom interpreters.
    """
    result = script(
        """
        #!/usr/bin/env python
        print('Hello, World!')
        """,
        executor="default",
    )
    assert isinstance(result, TaskExpression)
    assert scheduler.run(result) == b"Hello, World!\n"


def test_default_shell(scheduler: Scheduler) -> None:
    """
    script() should use bash as default interpreter.
    """
    result = script(
        """
        # Use a bash only syntax.
        cat <(echo ok)
        """
    )
    assert scheduler.run(result) == b"ok\n"


def test_script_error(scheduler: Scheduler) -> None:
    """
    Scripts should propagate their errors.
    """

    @task()
    def task1():
        return script(
            """
            echo message > /dev/stderr
            bad_prog 1 2 3
            """
        )

    with pytest.raises(ScriptError) as error:
        scheduler.run(task1())

    assert "message" in error.value.message
    assert "bad_prog: command not found" in error.value.message


def test_script_outputs(scheduler: Scheduler) -> None:
    """
    script() should be able to define an output structure.
    """
    result = script(
        """
        #!/bin/sh
        echo 'Hello, World!'
        """,
        executor="default",
        outputs={"my_output": 10, "stdout": File("-")},
    )
    assert isinstance(result, TaskExpression)
    assert scheduler.run(result) == {
        "my_output": 10,
        "stdout": b"Hello, World!\n",
    }


@use_tempdir
def test_script_file(scheduler: Scheduler) -> None:
    result = script(
        """
        #!/bin/sh
        echo 'hello' > hello.txt
        echo 'good bye' > bye.txt
        """,
        outputs={"hello": File("hello.txt"), "bye": File("bye.txt")},
    )

    result = scheduler.run(result)
    assert result["hello"].read() == "hello\n"
    assert result["bye"].read() == "good bye\n"


def test_command_eof() -> None:
    command = """
run-prog --x 10
ls my-dir
"""
    assert get_command_eof(command) == "EOF"

    command = """
run-prog --x 10 <<"EOF"
    ls my-dir
EOF
"""
    assert get_command_eof(command) == "EOF1"

    command = """
run-prog1 <<"EOF"
run-prog2 --x 10 <<"EOF1"
    ls my-dir
EOF1
EOF
"""
    assert get_command_eof(command) == "EOF2"


def test_wrapped_command() -> None:
    command = """\
#!/bin/bash
echo hello
"""
    assert "EOF" in get_wrapped_command(command)


def test_exec_wrapped_command() -> None:
    command = """\
#!/bin/bash
echo hello
"""
    wrapped_command = get_wrapped_command(command)
    assert subprocess.check_output(wrapped_command, shell=True) == b"hello\n"


def test_script_tempdir(scheduler: Scheduler) -> None:
    result = script(
        """
        #!/bin/sh
        echo 'hello' > hello.txt
        echo 'good bye' > bye.txt
        """,
        tempdir=True,
        outputs={"hello": File("hello.txt"), "bye": File("bye.txt")},
    )

    result = scheduler.run(result)

    # tempdir has been cleaned up.
    assert not result["hello"].exists()
    assert not result["bye"].exists()


@use_tempdir
def test_script_staging(scheduler: Scheduler) -> None:
    basedir = os.getcwd()

    hello_path = os.path.join(basedir, "remote_hello.txt")
    bye_path = os.path.join(basedir, "remote_bye.txt")

    result = script(
        """
        #!/bin/sh
        echo 'hello' > hello.txt
        echo 'good bye' > bye.txt
        """,
        tempdir=True,
        outputs={
            "hello": File(hello_path).stage("hello.txt"),
            "bye": File(bye_path).stage("bye.txt"),
        },
    )

    result = scheduler.run(result)

    # Remote files should still exist.
    assert result["hello"].exists()
    assert result["bye"].exists()
    assert result["hello"].path == hello_path
    assert result["bye"].path == bye_path
    assert result["hello"].read() == "hello\n"
    assert result["bye"].read() == "good bye\n"


@use_tempdir
def test_script_staging_dir(scheduler: Scheduler) -> None:
    basedir = os.getcwd()

    remote_dir = os.path.join(basedir, "remote")

    File(remote_dir + "/in/a.txt").write("a")
    File(remote_dir + "/in/b.txt").write("b")
    File(remote_dir + "/in/c/d.txt").write("d")

    result = script(
        """
        #!/bin/sh
        mkdir -p out
        cat in/a.txt in/b.txt in/c/d.txt > out/z
        echo 'hello' > out/y
        """,
        tempdir=True,
        inputs={Dir(remote_dir + "/in").stage("in")},
        outputs={"out": Dir(remote_dir + "/out").stage("out")},
    )

    result = scheduler.run(result)

    # Remote files should still exist.
    assert result["out"].exists()
    assert result["out"].file("z").read() == "abd"
    assert result["out"].file("y").read() == "hello\n"


@use_tempdir
def test_script_invalid(scheduler: Scheduler) -> None:
    """
    script() should be reactive to invalidated output.
    """

    expr = script(
        """
        echo hi > local
        """,
        outputs=[File("remote").stage("local")],
    )

    # Run the workflow once.
    [out_file] = scheduler.run(expr)
    assert out_file.read() == "hi\n"

    # Invalidate the output file by overwriting it.
    File("remote").write("bye")

    # Rerunning the workflow should reproduce the same output.
    [out_file] = scheduler.run(expr)
    assert out_file.read() == "hi\n"

    # Invalidate the output file by deleting.
    File("remote").remove()

    # Rerunning the workflow should reproduce the same output.
    scheduler.run(expr)
    assert File("remote").read() == "hi\n"


@use_tempdir
def test_script_staging_input_change(scheduler: Scheduler) -> None:
    """
    script() should be reactive to changing inputs.
    """

    File("input_remote").write("hello")

    expr = script(
        """
        cat input_local > output_local
        """,
        inputs=[File("input_remote").stage("input_local")],
        outputs=File("output_remote").stage("output_local"),
    )
    assert scheduler.run(expr).read() == "hello"

    # Change input.
    File("input_remote").write("hello2")

    expr = script(
        """
        cat input_local > output_local
        """,
        inputs=[File("input_remote").stage("input_local")],
        outputs=File("output_remote").stage("output_local"),
    )
    assert scheduler.run(expr).read() == "hello2"


@mock_s3
def _test_script_staging_s3(scheduler: Scheduler) -> None:
    s3_client = boto3.client("s3", region_name="us-east-1")
    s3_client.create_bucket(Bucket="example-bucket")

    hello_path = "s3://example-bucket/_hello.txt"
    bye_path = "s3://example-bucket/bye.txt"

    result = script(
        """
        #!/bin/sh
        echo 'hello' > hello.txt
        echo 'good bye' > bye.txt
        """,
        tempdir=True,
        outputs={
            "hello": File(hello_path).stage("hello.txt"),
            "bye": File(bye_path).stage("bye.txt"),
        },
    )

    result = scheduler.run(result)

    # Remote files should still exist.
    assert result["hello"].exists()
    assert result["bye"].exists()
    assert result["hello"].path == hello_path
    assert result["bye"].path == bye_path
    assert result["hello"].read() == "hello\n"
    assert result["bye"].read() == "good bye\n"


@mock_s3
def _test_script_task_aws_batch():
    @task(executor="batch", script=True)
    def task1(message):
        return """echo Hello, {message}!""".format(message=message)

    s3_client = boto3.client("s3", region_name="us-east-1")
    s3_client.create_bucket(Bucket="example-bucket")

    config = ConfigParser()
    config.read_dict(
        {
            "batch": {
                "image": "ubuntu",
                "queue": "queue",
                "s3_scratch": "s3://example-bucket/redun",
                "debug": True,
            }
        }
    )

    executor = AWSBatchExecutor("batch", None, config=config["batch"])
    scheduler = Scheduler()
    scheduler.executors["batch"] = executor
    executor.scheduler = scheduler
    assert scheduler.run(task1, ["World"]) == b"Hello, World!\n"
