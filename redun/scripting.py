import os
import shutil
import subprocess
import tempfile
from tempfile import mkdtemp
from textwrap import dedent
from typing import Any, Optional, Tuple, Union

from redun.file import File, Staging
from redun.task import Task, task
from redun.utils import iter_nested_value, map_nested_value

NULL = object()
# By default, use bash shell with immediate exit on first error.
DEFAULT_SHELL = "#!/usr/bin/env bash\nset -exo pipefail"


class ScriptError(Exception):
    """
    Error raised when user script returns failure (non-zero exit code).
    """

    def __init__(self, stderr: bytes):
        self.message: Union[bytes, str]

        try:
            self.message = stderr.decode("utf8")
        except UnicodeDecodeError:
            # Error might not be utf8. Keep as is.
            self.message = stderr

    def __str__(self) -> str:
        if isinstance(self.message, str):
            lines = self.message.rstrip("\n").rsplit("\n")
            return "Last line: " + lines[-1]
        else:
            return ""

    def __repr__(self) -> str:
        return f"ScriptError('{str(self)}')"


class ScriptCommand:
    """
    Class to store pre- and postprocessing information for a script command.
    """

    def __init__(self, command: str, inputs: Any, outputs: Any, temp_path: Optional[str]):
        self.command = command
        self.inputs = inputs
        self.outputs = outputs
        self.temp_path = temp_path


def prepare_command(command: str, default_shell=DEFAULT_SHELL) -> str:
    """
    Prepare a command string execution by removing surrounding blank lines and dedent.

    Also if an interpreter is not specified, add the default shell as interpreter.
    """
    command = dedent(command).strip()
    if not command.startswith("#!"):
        command = default_shell.rstrip("\n") + "\n" + command
    return command


def get_task_command(task: Task, args: Tuple, kwargs: dict, mount_staging: bool = False) -> str:
    """
    Get command from a script task.
    """
    command = task.func(*args, **kwargs)

    if isinstance(command, ScriptCommand):
        # Render a ScriptCommand into a full command string.
        command_parts = []

        # Prepare tempdir if requested.
        if command.temp_path:
            command_parts.append('cd "{}"'.format(command.temp_path))

        # Stage inputs.
        command_parts.extend(
            input.render_stage(as_mount=mount_staging)
            for input in iter_nested_value(command.inputs)
        )

        # User command.
        command_parts.append(get_wrapped_command(prepare_command(command.command)))

        # Unstage outputs.
        file_stages = [
            value for value in iter_nested_value(command.outputs) if isinstance(value, Staging)
        ]
        command_parts.extend(
            file_stage.render_unstage(as_mount=mount_staging) for file_stage in file_stages
        )

        full_command = "\n".join(command_parts)

        return prepare_command(full_command)

    elif isinstance(command, str):
        # Comand is already a string.
        return prepare_command(command)

    else:
        raise ValueError(f"Unexpected return value from script task: {command}")


def exec_script(command: str) -> bytes:
    """
    Run a script as a subprocess.
    """
    fd, command_file = tempfile.mkstemp()
    try:
        os.write(fd, command.encode("utf8"))
        os.close(fd)

        command2 = """\
chmod +x {command_file}
{command_file}
""".format(
            command_file=command_file
        )
        proc = subprocess.run(
            command2, check=False, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        result, error = proc.stdout, proc.stderr
    finally:
        os.remove(command_file)

    if proc.returncode != 0:
        # Raise error if command had error.
        raise ScriptError(error)

    return result


def get_command_eof(command: str, eof_prefix: str = "EOF") -> str:
    """
    Determine a safe end-of-file keyword to use for a given command to wrap.
    """
    index = 0
    eof = eof_prefix
    lines = command.split("\n")

    while True:
        if eof in lines:
            index += 1
            eof = eof_prefix + str(index)
        else:
            return eof


def get_wrapped_command(command: str, eof_prefix: str = "EOF") -> str:
    """
    Returns a shell script for executing a script written in any language.

    Consider `command` written in python:

    .. code-block:: python

        '''
        #!/usr/bin/env python

        print('Hello, World')
        '''

    In order to turn this into a regular sh shell script, we need to write
    this command to a temporary file, mark the file as executable,
    execute the file, and remove the temporary file.
    """
    wrapped_command = """\
(
# Save command to temp file.
COMMAND_FILE="$(mktemp)"
cat > "$COMMAND_FILE" <<"{eof}"
{command}
{eof}

# Execute temp file.
chmod +x "$COMMAND_FILE"
"$COMMAND_FILE"
RETCODE=$?

# Remove temp file.
rm "$COMMAND_FILE"

exit $RETCODE
)
""".format(
        command=command, eof=get_command_eof(command, eof_prefix=eof_prefix)
    )
    return wrapped_command


@task(name="script_task", namespace="redun", version="1", script=True)
def script_task(
    command: str, inputs: Any = [], outputs: Any = None, temp_path: Optional[str] = None
) -> ScriptCommand:
    """
    Execute a shell script as redun Task.
    """
    return ScriptCommand(command, inputs, outputs, temp_path)


@task(
    name="script",
    namespace="redun",
    version="1",
    check_valid="shallow",
    config_args=["input_staging"],
)
def _script(
    command: str,
    inputs: Any,
    outputs: Any,
    task_options: dict = {},
    temp_path: Optional[str] = None,
    core_command: Optional[str] = None,
    input_staging: Any = None,
) -> Any:
    """
    Internal task for executing a script.

    This task correctly implements reactivity to changing inputs and outputs.
    `script_task()` alone is unable to implement such reactivity because its
    only argument is a shell script string and its output is the stdout.
    Thus, the ultimate input and output files of the script are accessed
    outside the usual redun detection mechanisms (task arguments
    and return values).

    To achieve the correct reactivity, `script_task()` is special-cased in the Scheduler
    to not use caching, in order to force it to always execute when called.
    Additionally, `_script()` is configured with `check_valid="shallow"` to
    skip execution of its child tasks, `script_task()` and `postprocess_script()`,
    if its previous outputs are still valid (i.e. not altered or deleted).
    """
    # Note: inputs are an argument just for reactivity sake.
    # They have already been incorporated into the command.

    # Run the script.
    if core_command:
        result = script_task.options(**task_options)(
            core_command, input_staging, outputs, temp_path
        )
    else:
        # Backward-compatible behavior for cached expressions.
        result = script_task.options(**task_options)(command)

    # Post-process script results.
    return postprocess_script(result, outputs, temp_path=temp_path)


@task(name="postprocess_script", namespace="redun", version="1")
def postprocess_script(result: Any, outputs: Any, temp_path: Optional[str] = None) -> Any:
    """
    Postprocess the results of a script task.
    """

    def get_file(value: Any) -> Any:
        if isinstance(value, File) and value.path == "-":
            # File for script stdout.
            return result
        elif isinstance(value, Staging):
            # Staging files and dir turn into their remote versions.
            cls = type(value.remote)
            return cls(value.remote.path)
        else:
            return value

    if temp_path:
        shutil.rmtree(temp_path)

    return map_nested_value(get_file, outputs)


def build_backcompat_full_command(
    command: str, inputs: Any = [], outputs: Any = None, temp_path: Optional[str] = None
) -> str:
    """
    Build a full command with staging and unstaging.

    Note: This is only used for achieving caching backwards compatibility.
    """
    command_parts = []

    # Prepare tempdir if requested.
    if temp_path:
        command_parts.append('cd "{}"'.format(temp_path))

    # Stage inputs.
    command_parts.extend(input.render_stage() for input in iter_nested_value(inputs))

    # User command.
    command_parts.append(get_wrapped_command(prepare_command(command)))

    # Unstage outputs.
    file_stages = [value for value in iter_nested_value(outputs) if isinstance(value, Staging)]
    command_parts.extend(file_stage.render_unstage() for file_stage in file_stages)

    full_command = "\n".join(command_parts)
    return full_command


def script(
    command: str,
    inputs: Any = [],
    outputs: Any = NULL,
    tempdir: bool = False,
    **task_options: Any,
) -> Any:
    """
    Execute a shell script as a redun task with file staging.
    """
    if outputs == NULL:
        outputs = File("-")

    # Prepare tempdir if requested.
    temp_path = mkdtemp(suffix=".tempdir") if tempdir else None
    full_command = build_backcompat_full_command(command, inputs, outputs, temp_path=temp_path)

    # Get input files for reactivity.
    def get_file(value: Any) -> Any:
        if isinstance(value, Staging):
            # Staging files and dir turn into their remote versions.
            cls = type(value.remote)
            return cls(value.remote.path)
        else:
            return value

    input_args = map_nested_value(get_file, inputs)
    return _script(
        full_command,
        input_args,
        outputs,
        task_options=task_options,
        temp_path=temp_path,
        core_command=command,
        input_staging=inputs,
    )
