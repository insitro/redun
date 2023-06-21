import argparse
import datetime
import enum
import getpass
import importlib
import inspect
import json
import os
import pdb
import pickle
import re
import subprocess
import sys
import textwrap
from argparse import Namespace
from collections import Counter, defaultdict
from collections.abc import Callable as AbstractCallable
from contextlib import contextmanager
from itertools import chain, islice
from pprint import pprint
from shlex import quote
from socket import gethostname, socket
from textwrap import dedent
from types import ModuleType
from typing import (
    IO,
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Sequence,
    TextIO,
    Tuple,
    Union,
    cast,
)
from urllib.parse import ParseResult, urlparse

import botocore
import sqlalchemy as sa
from sqlalchemy.orm import Session
from sqlalchemy.sql.expression import cast as sa_cast

import redun
from redun.backends.base import TagEntity
from redun.backends.db import (
    JSON,
    Argument,
    Base,
    CallNode,
    Execution,
    File,
    Job,
    RedunBackendDb,
    RedunVersionError,
    Subvalue,
    Tag,
    Task,
    Value,
    parse_db_version,
)
from redun.backends.db.dataflow import display_dataflow, make_dataflow_dom, walk_dataflow
from redun.backends.db.query import (
    CallGraphQuery,
    infer_id,
    infer_specialty_id,
    parse_callgraph_query,
    setup_query_parser,
)
from redun.backends.db.serializers import RecordSerializer
from redun.config import Config, create_config_section
from redun.executors.aws_batch import (
    BATCH_LOG_GROUP,
    AWSBatchExecutor,
    aws_describe_jobs,
    format_log_stream_event,
)
from redun.executors.aws_utils import (
    get_aws_user,
    get_default_region,
    get_simple_aws_user,
    iter_log_stream,
)
from redun.executors.code_packaging import extract_tar
from redun.executors.launch import launch_script
from redun.expression import TaskExpression
from redun.file import File as BaseFile
from redun.file import copy_file, list_filesystems
from redun.job_array import get_job_array_index
from redun.logging import log_levels, logger
from redun.scheduler import (
    DryRunResult,
    ErrorValue,
    Scheduler,
    Traceback,
    format_job_statuses,
    get_task_registry,
)
from redun.scheduler_config import (
    DEFAULT_DB_URI,
    DEFAULT_POSTGRESQL_PORT,
    DEFAULT_REDUN_INI,
    DEFAULT_REPO_NAME,
    REDUN_CONFIG_DIR,
    REDUN_CONFIG_ENV,
    REDUN_INI_FILE,
    REDUN_USER_ENV,
    postprocess_config,
)
from redun.scripting import script_task
from redun.tags import (
    ANY_VALUE,
    DOC_KEY,
    PROJECT_KEY,
    USER_KEY,
    VERSION_KEY,
    format_tag_key_value,
    parse_tag_key_value,
)
from redun.task import Task as BaseTask
from redun.utils import add_import_path, format_table, pickle_dump, trim_string
from redun.value import NoneType, function_type, get_type_registry

# pygraphviz may not be installed. If not, disable
# the visualization functionality.
try:
    from redun.visualize import viz_record

    viz_is_enabled = True
except ModuleNotFoundError:
    viz_is_enabled = False

# Constants.
REDUN_DESCRIPTION = """\
redun :: version {version}

The redundant workflow engine.
"""


PAGER = "less"


class RedunClientError(Exception):
    pass


class ArgFormatter(argparse.ArgumentDefaultsHelpFormatter, argparse.RawDescriptionHelpFormatter):
    """
    An argument formatter that shows default values and does not reflow description strings.
    """

    pass


def format_timedelta(duration: datetime.timedelta) -> str:
    """
    Format timedelta as a string.
    """
    hours, remainder_seconds = divmod(duration.seconds, 3600)
    minutes, seconds = divmod(remainder_seconds, 60)
    centiseconds = int(duration.microseconds / 10000)
    return "{}:{:02}:{:02}.{:02}".format(
        hours,
        minutes,
        seconds,
        centiseconds,
    )


def format_id(id: str, detail: bool = False, prefix=8) -> str:
    """
    Display a record id.
    """
    if detail:
        return id
    else:
        return id[:prefix]


def format_arguments(args: List[Argument]) -> str:
    """
    Display CallNode arguments.

    For example, if `args` has 2 positional and 1 keyword argument, we would
    display that as:

        'prog', 10, extra_file=File(path=prog.c, hash=763bc10f)
    """
    pos_args = sorted(
        [arg for arg in args if arg.arg_position is not None], key=lambda arg: arg.arg_position
    )
    kw_args = sorted([arg for arg in args if arg.arg_key is not None], key=lambda arg: arg.arg_key)

    text = ", ".join(
        chain(
            (trim_string(repr(arg.value.preview)) for arg in pos_args),
            ("{}={}".format(arg.arg_key, trim_string(repr(arg.value.preview))) for arg in kw_args),
        )
    )
    return text


def parse_version(version: str) -> Tuple:
    """
    Parse a version number into a tuple of ints.
    """
    try:
        return tuple(map(int, version.split(".")))
    except ValueError:
        raise ValueError("Invalid version '{}'".format(version))


def check_version(version: str, version_spec: str) -> bool:
    """
    Returns True if version satisfies version specification.
    """
    if "," in version_spec:
        # Multiple version specifications.
        return all(
            check_version(version, version_part) for version_part in version_spec.split(",")
        )

    # Parse version and specification.
    match = re.match(r"^(?P<cmp>(==|>=|<=|>|<|))(?P<version>.*)$", version_spec)
    if not match:
        raise ValueError("Invalid version specification '{}'".format(version_spec))

    version_tuple = parse_version(version)
    spec_tuple = parse_version(match["version"])

    # Perform comparison.
    if match["cmp"] in ("", "=="):
        return version_tuple == spec_tuple
    elif match["cmp"] == ">":
        return version_tuple > spec_tuple
    elif match["cmp"] == ">=":
        return version_tuple >= spec_tuple
    elif match["cmp"] == "<=":
        return version_tuple <= spec_tuple
    elif match["cmp"] == "<":
        return version_tuple < spec_tuple
    else:
        raise NotImplementedError(version_spec)


def get_abs_path(path: str) -> str:
    """
    Returns absolute path of input string, which can be an s3 path.
    """
    if re.match(r"^.+://.*", path):
        return path
    else:
        return os.path.abspath(path)


def get_config_dir(config_dir: Optional[str] = None) -> str:
    """
    Get the redun config dir.

    We use the following precedence:
    - command line (`config_dir`)
    - environment variable
    - search filesystem (parent directories) for config dir
    - assume `.redun` in current working directory.
    """
    if config_dir:
        # If config_dir is already defined, use it as is.
        return config_dir

    # Attempt to use environment variable for config dir.
    config_dir = os.environ.get(REDUN_CONFIG_ENV)
    if config_dir:
        return config_dir

    # Search for config_dir.
    config_dir = find_config_dir()
    if config_dir:
        return config_dir

    # Use default.
    return REDUN_CONFIG_DIR


def infer_project_name(task: Task) -> Optional[str]:
    """
    Infer a project name from the top-level task.
    """
    return task.namespace


def find_config_dir(cwd: Optional[str] = None) -> Optional[str]:
    """
    Search up directories from current working directory to find config dir.
    """
    if not cwd:
        cwd = os.getcwd()

    base = cwd
    while True:
        config_dir = os.path.join(base, REDUN_CONFIG_DIR)
        if os.path.exists(config_dir):
            return config_dir
        parent_dir = os.path.dirname(base)
        if parent_dir == base:
            return None
        base = parent_dir


def parse_func_path(path: str) -> Tuple[str, str]:
    """
    Parses a function path 'file_or_module::func'.

    Parameters
    ----------
    path : str
        path should have the format: 'file_or_module::func', where `file_or_module`
        is a filename or python module and `func` is a function name.

    Returns
    -------
    Tuple[str, str]
        A tuple of file_or_module and function name.
    """
    match = re.match(r"(?P<file>.+)::(?P<func>.+)", path)
    if not match:
        raise ValueError("Invalid function specification: '{}'".format(path))
    return match["file"], match["func"]


def get_user_setup_func(config: Config) -> Callable[..., Scheduler]:
    """
    Returns scheduler setup function based on user config.
    """
    setup_func_path = config.get("scheduler", {}).get("setup_scheduler")
    if not setup_func_path:
        # Return default setup func.
        return lambda config: Scheduler(config=config)

    file_or_module, func_name = parse_func_path(setup_func_path)
    module = import_script(file_or_module)
    setup_func = getattr(module, func_name)
    return setup_func


def get_config_path(config_dir: Optional[str] = None) -> str:
    # Determine config dir.
    config_dir = get_config_dir(config_dir)

    # Setup default config file.
    config_path = os.path.join(config_dir, REDUN_INI_FILE)
    return config_path


def setup_config(
    config_dir: Optional[str] = None,
    db_uri: Optional[str] = None,
    repo: str = DEFAULT_REPO_NAME,
    initialize=True,
) -> Config:
    """
    Setup config file.
    """
    config_path = get_config_path(config_dir)
    config_file = BaseFile(config_path)

    if not config_file.exists():
        if initialize:
            # Initialize config file.
            if not db_uri:
                db_uri = DEFAULT_DB_URI

            with config_file.open("w") as out:
                out.write(DEFAULT_REDUN_INI.format(db_uri=db_uri))
        else:
            raise RedunClientError(f"No redun config found at {config_path}")

    # Load config file.
    config = Config()
    config.read_path(config_path)

    # Postprocess config.
    config = postprocess_config(config, get_config_dir(config_dir))

    if repo != DEFAULT_REPO_NAME:
        return setup_repo_config(config, repo)

    return config


def setup_repo_config(config: Config, repo: str) -> Config:
    """
    Uses configuration from another repository specified in local config.
    """
    repo_section = config.get("repos").get(repo)
    if not repo_section:
        raise RedunClientError("Unknown repository: {}".format(repo))

    repo_config_dir = repo_section.get("config_dir")
    if not repo_config_dir:
        raise RedunClientError("config_dir is not specified for repository {}".format(repo))

    repo_config = setup_config(repo_config_dir, initialize=False)
    return repo_config


def arg_name2cli_name(arg_name: str) -> str:
    """
    Convert a snake_case argument into a --kabob-case argument.
    """
    return "--" + arg_name.lower().replace("_", "-")


def make_parse_arg_func(arg_anno: Any) -> Callable[[str], Any]:
    """
    Returns parser for argument annotation.
    """
    arg_name = arg_anno.__name__ if hasattr(arg_anno, "__name__") else repr(arg_anno)

    def parse_arg(arg: str) -> Any:
        try:
            return get_type_registry().parse_arg(arg_anno, arg)
        except Exception as error:
            # Log specific error.
            logger.error(f"Error parsing {arg_name}: {error}")
            raise

    # Set parser name for more useful help text.
    parse_arg.__name__ = arg_name

    return parse_arg


def get_anno_origin(anno: Any) -> Optional[Any]:
    """
    Returns the origin of an annotation.

    Origin is Python's term for the main type of a generic type. For example,
    the origin of `List[int]` is `list` and its argument is `int`. This function
    abstracts away change that have occurred in the  Python API since 3.6.

    https://docs.python.org/3/library/typing.html#typing.get_origin
    """
    if sys.version_info < (3, 8):
        try:
            # Python 3.6
            return anno.__extra__
        except AttributeError:
            # Python 3.7
            return getattr(anno, "__origin__", None)
    else:
        # Python 3.8+
        from typing import get_origin

        return get_origin(anno)


def add_value_arg_parser(
    parser: argparse.ArgumentParser, arg_name: str, anno: Any, default: Any
) -> argparse.Action:
    """
    Add argument parser inferred from a parameter type.
    """

    def is_callable(anno: Any) -> bool:
        return get_anno_origin(anno) == AbstractCallable

    def is_typed_list(anno: Any) -> bool:
        klass = get_anno_origin(anno)
        return isinstance(klass, type) and issubclass(klass, list) and bool(anno.__args__)

    def is_optional(anno: Any) -> bool:
        """
        Return True if the annotation represents an optional arg, False otherwise.

        Arguments annotated with Optional[T] are a shortcut for Union[T, None]. So, this method
        first checks that we have a Union. Assuming we do have a Union, then we can use the
        largely undocumented __args__ which will be a 2-tuple of classes T and NoneType in the
        case we started with Optional[T].

        NOTE: Once we are on Python 3.8+, we can use the helper get_origin and get_args. For more
        on these introspection helpers, see:

            https://docs.python.org/3/library/typing.html#typing.get_args
        """
        klass = get_anno_origin(anno)
        if klass is not Union:
            return False

        try:
            type_when_present, expected_none_type = anno.__args__
        except ValueError:
            return False

        return expected_none_type is NoneType

    if anno is not None:
        # For lists, we are going to parse each of the list args independently so we need the
        # annotation information from the List arg(which is the type of the list elements). For
        # normal elements, proceed as normal since the annotation here will be for the single
        # argument we receive on the CLI and need to parse.
        if is_typed_list(anno):
            arg_anno = anno.__args__[0]

        elif is_optional(anno):
            # We know that the first class in the tuple is the class of the arg when present and
            # the second is class NoneType so we take the first. For more info, see the docstring
            # for is_optional above.
            arg_anno = anno.__args__[0]

        elif is_callable(anno):
            arg_anno = function_type

        else:
            arg_anno = anno

        parse_arg = make_parse_arg_func(arg_anno)
    else:
        arg_anno = None
        # Parameters without an annotation are assumed to be str.
        parse_arg = str

    parser_kwargs: Dict[str, Any] = {}
    if inspect.isclass(arg_anno) and issubclass(arg_anno, enum.Enum):
        parser_kwargs["choices"] = list(arg_anno)
    if is_typed_list(anno):
        # For lists, we can tell the parser to expect one or more arguments. So, you could pass
        # a list of values like:
        #       redun run workflow.py my_task --plate_pks 1000 1001 1002
        # and the parser will make sure they are all of the expected type based on the type
        # hint on the plate_pks arg(which in the above case would be List[int]).
        parser_kwargs["nargs"] = "+"

    return parser.add_argument(
        arg_name2cli_name(arg_name),
        type=parse_arg,
        default=default,
        help=" ",  # Force default value help text.
        **parser_kwargs,
    )


def get_setup_parser(
    setup_func: Callable,
) -> Tuple[argparse.ArgumentParser, Dict[str, str]]:
    """
    Returns an ArgumentParser for setup arguments.
    """
    parser = argparse.ArgumentParser()
    sig = inspect.signature(setup_func)
    cli2arg = {}
    for param in islice(sig.parameters.values(), 1, None):
        opt = add_value_arg_parser(
            parser,
            param.name,
            param.annotation if param.annotation != param.empty else None,
            param.default if param.default != param.empty else None,
        )
        cli2arg[opt.dest] = param.name

    return parser, cli2arg


def format_setup_help(parser: argparse.ArgumentParser) -> Iterator[str]:
    """
    Yields lines of help text for setup argument parser.
    """
    yield "redun --setup <option>=<value> ..."

    if parser.description:
        yield ""
        yield dedent(parser.description).strip("\n")
        yield ""

    for action in parser._actions:
        if action.dest == "help":
            continue
        option_string = action.option_strings[0].strip("-")
        yield "  --setup {option}={value} (default: {default})".format(
            option=option_string, value=action.dest.upper(), default=str(action.default)
        )


def parse_setup_args(setup_func: Callable, setup_args: Optional[List[str]]) -> Dict[str, Any]:
    """
    Parse setup arguments into keyword arguments.
    """
    if not setup_args:
        return {}

    # Validate setup_args and convert to argv.
    argv = []
    for key_value in setup_args:
        if "=" not in key_value:
            raise ValueError("Setup argument must be the format `option=value`.")
        argv.append("--" + key_value)

    # Parse setup args to kwargs.
    parser, cli2arg = get_setup_parser(setup_func)
    args = parser.parse_args(argv)
    kwargs = {arg_name: getattr(args, cli_name) for cli_name, arg_name in cli2arg.items()}
    return kwargs


def is_config_local_db(config: Config) -> bool:
    """
    Returns True if config uses a local sqlite db.
    """
    db_uri = config.get("backend", {}).get("db_uri", "")
    return db_uri.startswith("sqlite://")


def setup_scheduler(
    config_dir: Optional[str] = None,
    setup_args: Optional[List[str]] = None,
    repo: str = DEFAULT_REPO_NAME,
    migrate: Optional[bool] = None,
    migrate_if_local: bool = False,
) -> Scheduler:
    """
    Setup Scheduler from config directory.
    """
    config = setup_config(config_dir, repo=repo)
    setup_func = get_user_setup_func(config)
    setup_kwargs = parse_setup_args(setup_func, setup_args)
    scheduler = setup_func(config, **setup_kwargs)

    if migrate_if_local and is_config_local_db(config):
        migrate = True

    scheduler.load(migrate=migrate)
    return scheduler


def setup_backend_db(
    config_dir: Optional[str] = None,
    repo: str = DEFAULT_REPO_NAME,
) -> RedunBackendDb:
    """
    Setup RedunBackendDb from config directory.
    """
    config = setup_config(config_dir, repo=repo)
    backend_config = config.get("backend") or create_config_section()
    return RedunBackendDb(config=backend_config)


def get_username() -> str:
    """Returns the current redun user"""
    user = os.environ.get(REDUN_USER_ENV)
    return user or getpass.getuser()


def is_python_filename(name: str) -> bool:
    """
    Returns True if string looks like a python filename.
    """
    return name.endswith(".py")


def import_script(filename_or_module: str, add_cwd: bool = True) -> ModuleType:
    """
    Import a python script as a module.

    Parameters
    ----------
    filename_or_module : str
        This argument can be a filepath to a python script (e.g. `path/to/script.py`)
        or a dot-delimited module (e.g. `lib.workflows.workflow`).
    add_cwd : bool
        If True, add the current working directory to the python import paths (`sys.path`).
    """
    if is_python_filename(filename_or_module):
        # dirname is added to sys.path
        filename = filename_or_module
        dirpath = os.path.dirname(os.path.realpath(filename))
        module_name, ext = os.path.splitext(os.path.basename(filename))
        add_import_path(dirpath)
    else:
        if add_cwd:
            add_import_path(os.getcwd())
        module_name = filename_or_module

    try:
        module = importlib.import_module(module_name)
    except ModuleNotFoundError as error:
        if error.name == module_name:
            # Give nicer import error for top-level import.
            raise RedunClientError(
                f"Cannot find Python script file or module: {filename_or_module}"
            )
        else:
            # Let other import errors propogate as is.
            raise
    return module


def get_task_arg_parser(
    task: BaseTask, include_defaults: bool
) -> Tuple[argparse.ArgumentParser, Dict[str, str]]:
    """
    Returns a CLI parser for a redun Task.

    Parameters
    ----------
    task : BaseTask
        The task to generate a parser for.
    include_defaults : bool
        If true, set defaults for the parser based on the task defaults. If false, do not.
        This can be useful for determining if a particular argument was actually set by the user.
    """
    parser = argparse.ArgumentParser(
        prog=task.fullname, description=task.func.__doc__, formatter_class=ArgFormatter
    )
    parser.set_defaults(show_help=False)
    parser.set_defaults(show_info=False)
    subparsers = parser.add_subparsers()

    # Help subcommand for a task.
    help_parser = subparsers.add_parser("help", help="Show help for calling a task.")
    help_parser.set_defaults(show_help=True)

    # Info subcommand for a task.
    info_parser = subparsers.add_parser("info", help="Show task information.")
    info_parser.set_defaults(show_info=True)

    cli2arg = {}
    sig = task.signature
    for param in sig.parameters.values():
        opt = add_value_arg_parser(
            parser,
            param.name,
            param.annotation if param.annotation is not param.empty else None,
            param.default if include_defaults and param.default is not param.empty else None,
        )
        cli2arg[opt.dest] = param.name

    return parser, cli2arg


@contextmanager
def with_pager(client: "RedunClient", args: Namespace) -> Iterator[None]:
    """
    Context manager for running a pager (e.g. less) for output.
    """
    if not args.no_pager:
        client.start_pager()
    yield
    if not args.no_pager:
        client.stop_pager()


def is_port_in_use(hostname: str, port: Union[int, str]) -> bool:
    """
    Check if TCP/IP `port` on `hostname` is in use
    """
    with socket() as sock:
        try:
            sock.bind((hostname, int(port)))
            return False
        except OSError as err:
            if "Address already in use" in repr(err):
                return True
            raise err


def format_tags(tags: List[Tag], max_length: int = 50) -> str:
    """
    Format a set of tags.
    """
    if not tags:
        return ""

    tag_list = ", ".join(
        sorted(format_tag_key_value(tag.key, tag.value, max_length=max_length) for tag in tags)
    )
    return f"({tag_list})"


def run_command(argv: List[str]) -> Optional[str]:
    """
    Run a command and return its output.
    """
    try:
        return subprocess.check_output(argv, stderr=subprocess.STDOUT).decode("ascii").strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None


def get_gcp_user() -> Optional[str]:
    """
    Returns the user account for any active GCP logins, otherwise None.
    """
    return run_command(
        ["gcloud", "auth", "list", "--filter=status:ACTIVE", "--format=value(account)"]
    )


def get_default_execution_tags(
    user: Optional[str] = None,
    project: Optional[str] = None,
    doc: Optional[str] = None,
    exclude_users: List[str] = [],
) -> List[Tuple[str, Any]]:
    """
    Get default tags for the Execution.
    """
    tags: List[Tuple[str, Any]] = []

    # Redun version.
    tags.append((VERSION_KEY, redun.__version__))

    # Git commit hash.
    git_commit = os.environ.get("REDUN_GIT_COMMIT") or run_command(
        ["git", "rev-parse", "--verify", "HEAD"]
    )
    if git_commit:
        tags.append(("git_commit", git_commit))

    # Git origin URL.
    git_origin_url = os.environ.get("REDUN_GIT_ORIGIN_URL") or run_command(
        ["git", "remote", "get-url", "origin"]
    )
    if git_origin_url:
        tags.append(("git_origin_url", git_origin_url))

    # User tag.
    tags.append((USER_KEY, user or get_username()))

    # AWS user tags.
    if "aws" not in exclude_users:
        aws_region = get_default_region()
        try:
            tags.append(("user_aws_arn", get_aws_user(aws_region)))
            tags.append(("user_aws", get_simple_aws_user(aws_region)))
        except (botocore.exceptions.NoCredentialsError, botocore.exceptions.ClientError):
            pass

    # GCP user tags.
    if "gcp" not in exclude_users:
        gcp_user = get_gcp_user()
        if gcp_user and "WARNING" not in gcp_user:
            tags.append(("user_gcp", gcp_user))

    # Project tag.
    if project:
        tags.append((PROJECT_KEY, project))

    # Doc tag.
    if doc:
        tags.append((DOC_KEY, doc))

    return tags


class RedunClient:
    """
    Command-line (CLI) client for interacting with redun.
    """

    STATUS_WIDTH = 6

    def __init__(self, stdout: IO = sys.stdout, stderr: IO = sys.stderr):
        self.scheduler: Optional[Scheduler] = None
        self.repo: str = DEFAULT_REPO_NAME
        self.stdout: IO = stdout
        self.stderr: IO = stderr
        self.pager: Optional[subprocess.Popen[Any]] = None

    def get_scheduler(
        self, args: Namespace, migrate: bool = False, migrate_if_local: bool = False
    ) -> Scheduler:
        if not self.scheduler or args.repo != self.repo:
            self.scheduler = setup_scheduler(
                args.config,
                args.setup,
                repo=args.repo,
                migrate=migrate,
                migrate_if_local=migrate_if_local,
            )
            self.repo = args.repo
        return self.scheduler

    def get_backend(self, args: Namespace) -> RedunBackendDb:
        scheduler = self.get_scheduler(args)
        backend = cast(RedunBackendDb, scheduler.backend)
        return backend

    def get_session(self, args: Namespace) -> Session:
        scheduler = self.get_scheduler(args)
        backend = cast(RedunBackendDb, scheduler.backend)
        assert backend.session
        return backend.session

    def execute(self, argv: Optional[List[str]] = None) -> Any:
        """
        Execute a command from the command line.
        """
        if argv is None:
            argv = sys.argv

        parser = self.get_command_parser()
        args, extra_args = parser.parse_known_args(argv[1:])

        if args.log_level:
            logger.setLevel(log_levels[args.log_level])

        if args.check_version:
            self.check_version(args.check_version)

        try:
            return args.func(args, extra_args, argv)
        except RedunVersionError as error:
            raise RedunClientError(str(error))

    def start_pager(self) -> None:
        """
        Redirect output to a pager if stdout is a tty.
        """
        if sys.stdout.isatty():
            if subprocess.call(["which", PAGER], stdout=subprocess.DEVNULL) == 0:
                # https://chase-seibert.github.io/blog/2012/10/31/python-fork-exec-vim-raw-input.html#using-less-as-a-pager
                command = [PAGER, "-F", "-K", "-R", "-S", "-X"]
                self.pager = subprocess.Popen(
                    command,
                    stdin=subprocess.PIPE,
                    stdout=sys.stdout,
                    encoding="utf8",
                )
                assert self.pager.stdin
                self.stdout = self.pager.stdin

    def stop_pager(self) -> None:
        """
        Stop sending output to a pager.
        """
        if self.pager:
            assert self.pager.stdin
            self.pager.stdin.close()
            self.pager.wait()

    def display(
        self, *messages: Any, pretty: bool = False, indent: int = 0, newline: bool = True
    ) -> None:
        """
        Write text to standard output.
        """
        try:
            if pretty:
                [value] = messages
                pprint(value, stream=self.stdout)
            else:
                text = " ".join(map(str, messages))

                if indent:
                    text = textwrap.indent(text, " " * indent)

                self.stdout.write(text)
                if newline:
                    self.stdout.write("\n")

        except BrokenPipeError:
            # Gracefully exit, when stdout is closed.
            sys.stderr.close()
            sys.exit()

    def display_doc_tags(self, tags: List[Tag], indent: int = 0) -> None:
        """
        Display doc tags.
        """
        docs = [tag.value for tag in tags if tag.key == DOC_KEY]

        if docs:
            self.display()
            for doc in docs:
                self.display(doc, indent=indent + 2)

    def get_command_parser(self) -> argparse.ArgumentParser:
        """
        Returns the command line parser.
        """
        parser = argparse.ArgumentParser(
            prog="redun",
            formatter_class=ArgFormatter,
            description=REDUN_DESCRIPTION.format(version=redun.__version__),
        )
        parser.add_argument("-c", "--config", help="redun configuration directory.")
        parser.add_argument(
            "-r", "--repo", default=DEFAULT_REPO_NAME, help="Redun repository to use."
        )
        parser.add_argument("-V", "--version", action="store_true", help="Show redun version.")
        parser.add_argument("--check-version", help="Enforce required redun version.")
        parser.add_argument(
            "--log-level",
            default="INFO",
            choices=log_levels.keys(),
            help="Set redun logging level.",
        )
        parser.add_argument("-s", "--setup", action="append", help="Specify a setup argument.")
        parser.add_argument("--setup-help", action="store_true", help="Show setup argument help.")
        parser.set_defaults(func=self.help_command)
        subparsers = parser.add_subparsers()

        # Help command.
        help_parser = subparsers.add_parser("help", help="Show help information.")
        help_parser.set_defaults(func=self.help_command)

        # Init command.
        init_parser = subparsers.add_parser(
            "init", help="Initialize a redun configuration directory."
        )
        init_parser.set_defaults(func=self.init_command)
        init_parser.set_defaults(show_help=False)

        # Run command.
        run_parser = subparsers.add_parser("run", allow_abbrev=False, help="Run a workflow task.")
        run_parser.add_argument("--no-cache", action="store_true", help="Do not use cache.")
        run_parser.add_argument("--dryrun", action="store_true", help="Perform a dry run.")
        run_parser.add_argument("--pdb", action="store_true", help="Start debugger on exception.")
        run_parser.add_argument(
            "--rerun", action="store_true", help="Rerun task by job or execution id."
        )
        run_parser.add_argument(
            "-o", "--option", action="append", help="Override task option (format: key=value)."
        )
        run_parser.add_argument(
            "-t", "--tag", action="append", help="Execution tag (format: key=value)."
        )
        run_parser.add_argument(
            "-p", "--project", help="Specify project tag (default: current directory)."
        )
        run_parser.add_argument("--doc", help="Specify a documentation tag for the execution.")
        run_parser.add_argument("-u", "--user", help="Specify user tag for execution.")
        run_parser.add_argument(
            "--exclude-user",
            action="append",
            default=[],
            help="Exclude adding a specific user tag (e.g. aws, gcp).",
        )
        run_parser.add_argument("script", help="Python script to import.")
        run_parser.add_argument("task", help="task within script to run.")
        run_parser.add_argument(
            "-i",
            "--input",
            help="Input file for task arguments. Should be a "
            "pickle of `Tuple, Dict` containing "
            "`args, kwargs`. Additional variables specified as command line arguments will "
            "override/supplement the contents of this file.",
        )
        run_parser.add_argument(
            "--execution-id",
            help="If provided, the execution id. Must be a UUID that has not been used "
            "previously.",
        )

        run_parser.set_defaults(func=self.run_command)
        run_parser.set_defaults(show_help=False)

        # Launch command.
        launch_parser = subparsers.add_parser(
            "launch",
            allow_abbrev=False,
            help="Launch a workflow within an executor (e.g. docker, batch).",
        )
        launch_parser.add_argument("--executor", help="Executor to run Scheduler within.")
        launch_parser.add_argument(
            "-o",
            "--option",
            action="append",
            help="Override executor options (format: key=value).",
        )
        launch_parser.add_argument(
            "--wait", action="store_true", help="Wait for workflow to complete."
        )
        launch_parser.set_defaults(func=self.launch_command)
        launch_parser.set_defaults(show_help=False)

        # Log command.
        log_parser = subparsers.add_parser("log", help="Show information on historical runs.")
        log_parser.add_argument(
            "--no-pager",
            action="store_true",
            help="Do not use pager for log output.",
        )
        log_parser = setup_query_parser(log_parser)
        log_parser.add_argument("--count", action="store_true", help="Show record counts.")
        log_parser.add_argument(
            "--format", help="Output format.", default="text", choices=["text", "json"]
        )
        log_parser.add_argument(
            "--detail",
            action="store_true",
            default=False,
            help="Show full record details.",
        )
        log_parser.set_defaults(func=self.log_command)

        # Console command.
        console_parser = subparsers.add_parser(
            "console", help="Show information on historical runs in a TUI (Textual UI)."
        )
        console_parser.set_defaults(func=self.console_command)

        # Viz command
        if viz_is_enabled:
            viz_parser = subparsers.add_parser(
                "viz", help="Produce visualization of specific execution."
            )
            viz_parser.add_argument(
                "--format", help="Output format. [Universal]", choices=["dot", "png"]
            )
            viz_parser.add_argument(
                "--output", help="Absolute filepath to save image or dot text into. [Universal]"
            )
            viz_parser.add_argument(
                "--horizontal",
                action="store_true",
                help="Orient the graph from left to right instead of top to bottom. [Universal]",
            )
            viz_parser.add_argument(
                "--no-truncation",
                action="store_true",
                help="Prevent truncation of value nodes. [Universal]",
            )
            viz_parser.add_argument(
                "--hash", action="store_true", help="Display each object's hash. [Universal]"
            )
            viz_parser.add_argument(
                "--no-detail",
                action="store_true",
                help="Produce only the job graph. [Non-value Queries]",
            )
            viz_parser.add_argument(
                "--jobs",
                action="store_true",
                help="Visualize jobs as their own nodes. [Execution Queries]",
            )
            viz_parser.add_argument(
                "--dataflow",
                action="store_true",
                help="Deconstruct values into their leaf subvalues to track dataflow. \
                      [Non-value Queries]",
            )
            viz_parser.add_argument(
                "--deduplicate",
                action="store_true",
                help="""Condense identical values into the same node. \
                     (Note this should NOT be used if your program moves values that \
                      have low entropy and a high chance of hash collisions). \
                      [Non-value Queries]""",
            )
            viz_parser.add_argument(
                "--wrap-calls",
                action="store_true",
                help="Wrap routing calls around arguments in value queries. [Value Queries]",
            )
            viz_parser.set_defaults(func=self.viz_command)

        # Repl command.
        repl_parser = subparsers.add_parser("repl", help="Get a repl for querying history.")
        repl_parser.set_defaults(func=self.repl_command)

        # Tag command.
        tag_parser = subparsers.add_parser("tag", help="Show and manipulate tags.")
        tag_subparsers = tag_parser.add_subparsers()

        # Tag listing command.
        tag_list_parser = tag_subparsers.add_parser("list", help="List and search tags.")
        tag_list_parser.add_argument("--values", action="store_true", help="Show tag values.")
        tag_list_parser.set_defaults(func=self.tag_list_command)

        # Tag add command.
        tag_add_parser = tag_subparsers.add_parser(
            "add",
            help=(
                "Add tags to entities (Executions, Values, etc)."
                "redun tag add <entity_id> [<key>=<value> ...]"
            ),
        )
        tag_add_parser.set_defaults(func=self.tag_add_command)

        # Tag add command.
        tag_update_parser = tag_subparsers.add_parser(
            "update",
            help=(
                "Update tags on entities (Executions, Values, etc)."
                "redun tag update <entity_id> [<key>=<value> ...]"
            ),
        )
        tag_update_parser.set_defaults(func=self.tag_update_command)

        # Tag delete command.
        tag_rm_parser = tag_subparsers.add_parser(
            "rm",
            help=(
                "Delete tags from entities (Executions, Values, etc)."
                "redun tag rm <entity_id> [<key>=<value> ...]"
            ),
        )
        tag_rm_parser.set_defaults(func=self.tag_rm_command)

        # Oneshot command.
        oneshot_parser = subparsers.add_parser("oneshot", help="Execute one task.")
        oneshot_parser.add_argument("--no-cache", action="store_true", help="Do not use cache.")
        oneshot_parser.add_argument("-c", "--code", help="Code package containing script.")
        oneshot_parser.add_argument("-i", "--input", help="Input file for task arguments.")
        oneshot_parser.add_argument("-o", "--output", help="Output file for task result.")
        oneshot_parser.add_argument("-e", "--error", help="Output file for task error.")
        oneshot_parser.add_argument(
            "--array-job", action="store_true", help="Indicates task is part of an array job"
        )
        oneshot_parser.add_argument(
            "-p",
            "--import-path",
            action="append",
            default=[],
            help="Additional python import path.",
        )
        oneshot_parser.add_argument("script", help="Python script to import.")
        oneshot_parser.add_argument("task", help="task within script to run.")
        oneshot_parser.set_defaults(func=self.oneshot_command)

        # Db command.
        db_parser = subparsers.add_parser("db", help="Manage redun repo database.")
        db_subparsers = db_parser.add_subparsers()

        # Db info command.
        db_info_parser = db_subparsers.add_parser(
            "info", help="Display information about redun repo database."
        )
        db_info_parser.set_defaults(func=self.db_info_command)

        # Db upgrade command.
        db_upgrade_parser = db_subparsers.add_parser(
            "upgrade", help="Upgrade redun repo database."
        )
        db_upgrade_parser.add_argument(
            "db_version",
            nargs="?",
            default="latest",
            help="DB version to upgrade towards.",
            type=str,
        )
        db_upgrade_parser.set_defaults(func=self.db_upgrade_command)

        # Db downgrade command.
        db_downgrade_parser = db_subparsers.add_parser(
            "downgrade", help="Downgrade redun repo database."
        )
        db_downgrade_parser.add_argument(
            "db_version",
            help="DB version to downgrade towards.",
            type=str,
        )
        db_downgrade_parser.set_defaults(func=self.db_downgrade_command)

        # Db versions command.
        db_versions_parser = db_subparsers.add_parser(
            "versions", help="Show all available redun repo database versions."
        )
        db_versions_parser.set_defaults(func=self.db_versions_command)

        # Export command.
        export_parser = subparsers.add_parser("export", help="Export records from redun repo.")
        export_parser.add_argument("-f", "--file", default="-", help="File to export records.")
        export_parser.set_defaults(func=self.export_command)

        # Import command.
        import_parser = subparsers.add_parser("import", help="Import records into redun repo.")
        import_parser.add_argument("-f", "--file", default="-", help="File to import records.")
        import_parser.set_defaults(func=self.import_command)

        # Add repo command
        repo_parser = subparsers.add_parser("repo", help="Define additional repositories.")
        repo_subparsers = repo_parser.add_subparsers()
        repo_add_parser = repo_subparsers.add_parser("add", help="Add a repository")
        repo_add_parser.add_argument("repo_name", help="Repository name")
        repo_add_parser.add_argument(
            "repo_config_dir", help="Directory with redun configuration. Can be local or on S3"
        )
        repo_add_parser.set_defaults(func=self.repo_add_command)

        repo_rm_parser = repo_subparsers.add_parser("remove", help="Remove a repository")
        repo_rm_parser.add_argument("repo_name", help="Repository name")
        repo_rm_parser.set_defaults(func=self.repo_remove_command)

        repo_list_parser = repo_subparsers.add_parser("list", help="List repositories")
        repo_list_parser.set_defaults(func=self.repo_list_command)

        # Push command.
        push_parser = subparsers.add_parser("push", help="Sync records to another redun repo.")
        push_parser.add_argument("push_repo", help="Redun repo name.")
        push_parser.set_defaults(func=self.push_command)

        # Pull command.
        pull_parser = subparsers.add_parser("pull", help="Sync records from another redun repo.")
        pull_parser.add_argument("pull_repo", help="Redun repo name.")
        pull_parser.set_defaults(func=self.pull_command)

        # aws commands.
        aws_parser = subparsers.add_parser("aws", help="Execute a command for AWS executors.")
        aws_subparsers = aws_parser.add_subparsers()
        aws_statuses = [
            "SUBMITTED",
            "PENDING",
            "RUNNABLE",
            "STARTING",
            "RUNNING",
            "SUCCEEDED",
            "FAILED",
        ]

        # aws list-jobs.
        aws_list_jobs_parser = aws_subparsers.add_parser("list-jobs", help="List AWS Batch jobs.")
        aws_list_jobs_parser.add_argument(
            "-s",
            "--status",
            help="Filter on comma separated statuses ({}).".format(",".join(aws_statuses)),
        )
        aws_list_jobs_parser.set_defaults(func=self.aws_list_jobs_command)

        # aws kill-jobs.
        aws_kill_jobs_parser = aws_subparsers.add_parser("kill-jobs", help="Kill AWS Batch jobs.")
        aws_kill_jobs_parser.add_argument(
            "-s",
            "--status",
            help="Filter on comma separated statuses ({}).".format(",".join(aws_statuses)),
        )
        aws_kill_jobs_parser.set_defaults(func=self.aws_kill_jobs_command)

        # aws logs
        aws_logs_parser = aws_subparsers.add_parser("logs", help="Fetch AWS Batch job logs.")
        aws_logs_parser.add_argument(
            "-j", "--job", default=[], action="append", help="Show logs for redun Job id."
        )
        aws_logs_parser.add_argument(
            "-b",
            "--batch-job",
            default=[],
            action="append",
            help="Show logs for AWS Batch Job id.",
        )
        aws_logs_parser.add_argument(
            "-l", "--log-stream", default=[], action="append", help="Show logs for AWS Log Stream."
        )
        aws_logs_parser.add_argument(
            "-a", "--all", action="store_true", help="Show logs for all jobs."
        )
        aws_logs_parser.add_argument(
            "-s",
            "--status",
            help="Filter on comma separated statuses ({}).".format(",".join(aws_statuses)),
        )
        aws_logs_parser.set_defaults(func=self.aws_logs_command)

        # Filesystem copy command.
        fs_parser = subparsers.add_parser(
            "fs", help="Filesystem commands (local, s3, gs, http, etc)."
        )
        fs_parser.set_defaults(func=self.fs_command)
        fs_subparsers = fs_parser.add_subparsers()

        fs_copy_parser = fs_subparsers.add_parser(
            "cp", allow_abbrev=False, help="Copy files across filesystems."
        )
        fs_copy_parser.add_argument("src_path", help="Source file path.")
        fs_copy_parser.add_argument("dest_path", help="Destination file path.")
        fs_copy_parser.set_defaults(func=self.fs_copy_command)
        fs_copy_parser.set_defaults(show_help=False)

        # Server command.
        server_parser = subparsers.add_parser(
            "server",
            help="Run the redun local web server UI. "
            "This command must be run from the base of the redun repository.",
        )
        server_parser.set_defaults(func=self.server_command)

        return parser

    def help_command(self, args: Namespace, extra_args: List[str], argv: List[str]) -> None:
        """
        Show help information.
        """
        if args.version:
            # Print version information.
            self.display(redun.__version__)

        elif args.setup_help:
            # Display setup help.
            config = setup_config(args.config)
            setup_func = get_user_setup_func(config)
            parser, _ = get_setup_parser(setup_func)
            self.display("\n".join(format_setup_help(parser)))

        else:
            # Print full help information.
            parser = self.get_command_parser()
            self.display(parser.format_help())

    def check_version(self, required_version: str) -> None:
        """
        Enfore a required redun version.
        """
        if not check_version(redun.__version__, required_version):
            raise RedunClientError(
                "redun version {version} does not meet requirement {required_version}".format(
                    version=redun.__version__, required_version=required_version
                )
            )

    def init_command(self, args: Namespace, extra_args: List[str], argv: List[str]) -> None:
        """
        Initialize redun project directory.
        """
        if not args.config:
            if extra_args:
                basedir = extra_args[0]
            else:
                basedir = "."
            args.config = os.path.join(basedir, REDUN_CONFIG_DIR)

        self.get_scheduler(args, migrate=True)
        self.display("Initialized redun repository: {}".format(args.config))

    def run_command(self, args: Namespace, extra_args: List[str], argv: List[str]) -> Any:
        """
        Performs the run command.
        """
        logger.info(f"redun :: version {redun.__version__}")
        logger.info(f"config dir: {get_config_dir(args.config)}")

        # Get main task.
        module: Any = import_script(args.script)

        scheduler = self.get_scheduler(args, migrate_if_local=True)

        # Determine if module-level help is needed.
        if args.task == "help":
            # Exclude redun builtin tasks.
            tasks = [task_ for task_ in scheduler.task_registry if task_.namespace != "redun"]
            tasks = sorted(tasks, key=lambda task: task.fullname)

            self.display("Tasks available:")
            for task in tasks:
                self.display("  ", task.fullname)
            return None

        # Determine rerun job.
        rerun_job: Optional[Job] = None
        if args.rerun:
            record = self.infer_id(args.task)
            if isinstance(record, Job):
                rerun_job = record
            elif isinstance(record, Execution):
                rerun_job = record.job

            if not rerun_job:
                raise RedunClientError('Unknown job or execution "{}"'.format(args.task))

        # Get requested task.
        if rerun_job:
            task_fullname = rerun_job.task.fullname
        elif "." in args.task or not hasattr(module, "redun_namespace"):
            task_fullname = args.task
        else:
            # Convert relative task name to fullname.
            task_fullname = "{}.{}".format(module.redun_namespace, args.task)
        task = scheduler.task_registry.get(task_fullname)
        if not task:
            raise RedunClientError('Unknown task "{}"'.format(task_fullname))

        # Determine arguments for task.
        task_arg_parser_defaults, cli2arg = get_task_arg_parser(task, include_defaults=True)
        task_arg_parser_no_defaults, _ = get_task_arg_parser(task, include_defaults=False)
        task_args_defaults = task_arg_parser_defaults.parse_args(extra_args)
        task_args_no_defaults = task_arg_parser_no_defaults.parse_args(extra_args)

        # Determine if task-level help is needed.
        if task_args_defaults.show_help:
            self.display(task_arg_parser_defaults.format_help())
            return None

        # Determine if task-level info is needed.
        if task_args_defaults.show_info:
            self.log_task(task, show_job=False)
            return None

        pos_args = []
        kwargs = {}

        # Determine arguments for task.
        if args.input:
            # Parse task args from input file.
            input_file = BaseFile(args.input)
            with input_file.open("rb") as infile:
                pos_args, kwargs = pickle.load(infile)

        # Determine task arguments from rerun job.
        elif rerun_job:
            rerun_args = sorted(
                rerun_job.call_node.arguments, key=lambda arg: arg.arg_position or -1
            )
            for arg in rerun_args:
                if arg.arg_key:
                    kwargs[arg.arg_key] = arg.value_parsed
                else:
                    pos_args.append(arg.value_parsed)

        # Parse cli arguments to task arguments, potentially overriding other types of inputs.
        for dest, arg in cli2arg.items():
            # Use the parsed values without defaults, because we know the python signature
            # already includes them. Moreover, we only want to overwrite values from the prior
            # steps with explicit user input, not defaults.
            value = getattr(task_args_no_defaults, dest)

            if value is not None:
                kwargs[arg] = value

        # Apply task options override.
        if args.option:
            task_options = dict(map(parse_tag_key_value, args.option))
            task = task.options(**task_options)

        # Determine execution tags.
        tags: List[Tuple[str, Any]] = get_default_execution_tags(
            user=args.user,
            project=args.project or infer_project_name(task),
            doc=args.doc,
            exclude_users=args.exclude_user,
        )
        if args.tag:
            tags.extend(map(parse_tag_key_value, args.tag))

        # Run the task.
        expr = task(*pos_args, **kwargs)
        try:
            result = scheduler.run(
                expr,
                exec_argv=argv,
                dryrun=args.dryrun,
                cache=not args.no_cache,
                tags=tags,
                execution_id=args.execution_id,
            )

        except DryRunResult:
            sys.exit(1)

        except Exception:
            if args.pdb:
                # Start postmortem debugger.
                print("Uncaught exception. Entering postmortem debugging.")
                tb = sys.exc_info()[2]
                debugger = pdb.Pdb()

                # Addresses issue with pdbpp performing continue.
                debugger.botframe = None

                debugger.interaction(None, tb)

            raise

        if result is not None:
            self.display(result, pretty=True)

        return result

    def launch_command(self, args: Namespace, extra_args: List[str], argv: List[str]) -> None:
        """
        Run a workflow within an Executor (e.g. docker or batch).
        """
        logger.info(f"redun :: version {redun.__version__}")
        logger.info(f"config dir: {get_config_dir(args.config)}")

        config = setup_config(args.config)
        executor_name = args.executor
        if not executor_name:
            raise RedunClientError("--executor is required.")

        # Parse task options.
        task_options = dict(map(parse_tag_key_value, args.option)) if args.option else {}
        task_options["executor"] = executor_name

        if args.wait:
            # Prepare command to execute within Executor.
            remote_run_command = " ".join(quote(arg) for arg in extra_args)

            logger.info(f"Run within Executor {executor_name}: {remote_run_command}")

            # Setup Scheduler.
            scheduler = self.get_scheduler(args, migrate_if_local=True)

            # Setup job for inner run command.
            run_expr = cast(
                TaskExpression, script_task.options(**task_options)(remote_run_command)
            )

            # Run scheduler and wait for completion.
            result = scheduler.run(run_expr)
            if result is not None:
                self.display(result, pretty=True)

        else:
            launch_script(
                config,
                executor_name=executor_name,
                script_command=extra_args,
                task_options=task_options,
            )

    def infer_file_path(self, path: str) -> Optional[Base]:
        """
        Try to infer if path matches any File.

        Returns a query iterating over Files and relevant Jobs.
        """
        assert self.scheduler
        assert isinstance(self.scheduler.backend, RedunBackendDb)
        assert self.scheduler.backend.session

        return (
            self.scheduler.backend.session.query(File, Job)
            .outerjoin(Subvalue, Subvalue.value_hash == File.value_hash)
            .join(
                CallNode,
                sa.or_(
                    CallNode.value_hash == File.value_hash,
                    CallNode.value_hash == Subvalue.parent_value_hash,
                ),
            )
            .join(Job)
            .filter(File.path == path)
            .order_by(Job.end_time.desc())
            .first()
        )

    def infer_id(self, id: str, include_files: bool = True, required: bool = False) -> Any:
        """
        Try to infer the record based on an id prefix.
        """
        assert self.scheduler
        assert isinstance(self.scheduler.backend, RedunBackendDb)
        assert self.scheduler.backend.session

        return infer_id(
            self.scheduler.backend.session, id, include_files=include_files, required=required
        )

    def viz_command(self, args: Namespace, extra_args: List[str], argv: List[str]) -> None:
        """
        Performs the visualization command.
        """

        self.get_scheduler(args)

        scheduler = self.scheduler
        assert scheduler
        assert isinstance(scheduler.backend, RedunBackendDb)
        assert scheduler.backend.session

        props = args.__dict__
        props["detail"] = not args.no_detail
        props["direction"] = "LR" if props["horizontal"] else "TB"

        if extra_args:
            id = extra_args[0]
            query = CallGraphQuery(scheduler.backend.session)
            record = query.like_id(id).one_or_none()

            if not record:
                raise RedunClientError(f"Provided id {id} is invalid.")

            viz_record(scheduler, record, props)

    def log_command(self, args: Namespace, extra_args: List[str], argv: List[str]) -> None:
        """
        Performs the log command.

        This is the main command for querying the CallGraph.
        """
        self.get_scheduler(args)

        assert self.scheduler
        assert isinstance(self.scheduler.backend, RedunBackendDb)
        assert self.scheduler.backend.session

        # Display defaults.
        indent = 0
        detail = args.detail
        compact = True
        display = "general"

        if args.file:
            display = "file"

        # Filter by execution id.
        if args.exec_id:
            # Replace special id into regular execution ids.
            execs: Iterable[Execution] = list(
                filter(
                    lambda record: isinstance(record, Execution),
                    [self.infer_id(exec_id) for exec_id in args.exec_id.split(",")],
                )
            )
            args.exec_id = ",".join(exec.id for exec in execs)

        has_type_filters = (
            args.all
            or args.exec
            or args.job
            or args.call_node
            or args.task
            or args.value
            or args.file
            or args.file_path
        )

        query = CallGraphQuery(self.scheduler.backend.session).order_by("time")
        query = parse_callgraph_query(query, args)

        if extra_args:
            # Check unknown filters.
            for arg in extra_args:
                if arg != "-" and arg.startswith("-"):
                    raise RedunClientError(f"Unknown filter: {arg}")

            # Search for specialty ids first.
            id = extra_args[0]
            detail = True
            record = infer_specialty_id(self.scheduler.backend.session, id)
            if record:
                with with_pager(self, args):
                    self.log_record(record, indent=indent, detail=detail, format=args.format)
                return

            # Search by entities by id.
            query = query.like_id(id)

        elif not has_type_filters:
            # Default show executions.
            self.display("Recent executions:\n")

            query = query.filter_types({"Execution"})

        # Display results.
        with with_pager(self, args):
            if args.count:
                for record_type, count in query.count():
                    self.display(
                        "{count:>8} {record_type}".format(record_type=record_type, count=count)
                    )

            elif display == "file":
                self.log_files(query)

            elif display == "general":
                first = True
                for record in query.all():
                    if not first and not compact:
                        self.display()
                    self.log_record(record, indent=indent, detail=detail, format=args.format)
                    first = False

            else:
                raise NotImplementedError(display)

    def log_record(
        self, record: Any, detail: bool = True, indent: int = 0, format: str = "text"
    ) -> None:
        """
        Display one record or a list of records.
        """
        if isinstance(record, list):
            for child in record:
                self.log_record(child, detail=detail, indent=indent, format=format)

        elif format == "json":
            if isinstance(record, (Execution, Job, CallNode, Task, Value)):
                record_serializer = RecordSerializer()
                self.display(json.dumps(record_serializer.serialize(record), sort_keys=True))

        elif isinstance(record, Execution):
            self.log_execution(record, detail=detail, indent=indent)

        elif isinstance(record, Job):
            self.log_job(record, detail=detail)

        elif isinstance(record, Task):
            self.log_task(record, detail=detail)

        elif isinstance(record, CallNode):
            self.log_call_node(record, detail=detail)

        elif isinstance(record, Value):
            self.log_value(record, detail=detail)

        elif isinstance(record, tuple) and isinstance(record[0], File):
            file, job, kind = record
            self.log_file(file, kind)

    def log_execution(
        self, execution: Execution, show_jobs: bool = True, indent: int = 0, detail: bool = True
    ) -> None:
        """
        Display an Execution.
        """
        status = execution.status

        self.display(
            "Exec {id} [{status}] {start_time}:  {args} {tags}".format(
                id=format_id(execution.id, detail),
                status=status.center(self.STATUS_WIDTH),
                start_time=execution.job.start_time.strftime("%Y-%m-%d %H:%M:%S"),
                args=" ".join(json.loads(execution.args)[1:]),
                tags=format_tags(execution.tags),
            ),
            indent=indent,
        )

        if detail and show_jobs:
            if status == "DONE":
                duration = execution.job.duration
                end_time = execution.job.end_time
            else:
                times = [(job.start_time, job.end_time) for job in execution.jobs]
                start_time = min(start_time for start_time, _ in times)
                end_time = max((end_time for _, end_time in times if end_time), default=start_time)
                duration = end_time - start_time

            self.display("Duration: {}".format(format_timedelta(duration)), indent=indent)
            self.display_doc_tags(execution.tags, indent=indent)
            self.display()

            # Display job status table.
            job_status_counts: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
            for job in execution.jobs:
                job_status_counts[job.task.fullname][job.status] += 1
                job_status_counts[job.task.fullname]["TOTAL"] += 1

            for line in format_job_statuses(job_status_counts, end_time):
                self.display(line)

            # Display job tree.
            job_statuses = Counter(job.status for job in execution.jobs)
            self.display(
                "Jobs: {total} (DONE: {done}, CACHED: {cached}, FAILED: {failed})".format(
                    total=sum(job_statuses.values()),
                    done=job_statuses["DONE"],
                    cached=job_statuses["CACHED"],
                    failed=job_statuses["FAILED"],
                ),
                indent=indent,
            )
            self.display("-" * 80, indent=indent)
            self.log_job(execution.job, indent=indent, show_children=True, detail=False)

    def log_traceback(self, job: Job, indent: int = 0, detail: bool = True) -> None:
        """
        Display a Job traceback.
        """
        # Determine job stack.
        job_stack = []
        current_job = job
        while current_job:
            job_stack.append(current_job)
            current_job = current_job.parent_job

        if not detail:
            parts = ["Exec {exec} > ".format(exec=job.execution.id[:8])]

            if len(job_stack) > 2:
                parts.append(
                    "({num_jobs} {unit}) > ".format(
                        num_jobs=len(job_stack) - 2,
                        unit="Jobs" if len(job_stack) - 2 > 1 else "Job",
                    )
                )
            if len(job_stack) > 1:
                parts.append(
                    "Job {job_id} {task_name} > ".format(
                        job_id=job_stack[1].id[:8],
                        task_name=job_stack[1].task.name,
                    )
                )
            parts.append(
                "Job {job_id} {task_name}".format(
                    job_id=job_stack[0].id[:8],
                    task_name=job_stack[0].task.name,
                )
            )

            self.display("Traceback: {traceback}".format(traceback="".join(parts)), indent=indent)

        elif job.call_node:
            self.display("Traceback:", indent=indent)
            error_value = job.call_node.value.value_parsed
            if isinstance(error_value, ErrorValue) and error_value.traceback:
                for line in error_value.traceback.format():
                    self.display(line.rstrip("\n"), indent=indent)

    def log_job(
        self, job: Job, indent: int = 0, show_children: bool = False, detail: bool = True
    ) -> None:
        """
        Display a Job.
        """
        args = format_arguments(job.call_node.arguments) if job.call_hash else ""

        self.display(
            "Job {id} [{status}] {start_time}:  {task_name}({args}) {tags}".format(
                id=format_id(job.id, detail),
                status=job.status.center(self.STATUS_WIDTH),
                start_time=job.start_time.strftime("%Y-%m-%d %H:%M:%S"),
                task_name=job.task.fullname,
                args=args,
                tags=format_tags(job.tags),
            ),
            indent=indent,
        )

        if detail:
            self.log_traceback(job, indent=indent, detail=False)
            if job.status == "DONE":
                self.display(
                    "Duration: {}".format(
                        format_timedelta(job.duration) if job.duration else "Unknown"
                    ),
                    indent=indent,
                )

            if job.call_node:
                self.display()
                self.log_call_node(
                    job.call_node, show_parents=False, show_children=False, indent=indent + 2
                )
            else:
                self.display()
                self.display(
                    "CallNode UNKNOWN: Job has not finished or has been killed.\n",
                    indent=indent + 2,
                )
                self.log_task(job.task, show_job=False, indent=indent + 2)

            if job.status == "FAILED":
                self.log_traceback(job, indent=indent + 2, detail=True)

        elif show_children:
            for child_job in job.child_jobs:
                self.log_job(
                    child_job, indent=indent + 2, show_children=show_children, detail=False
                )

    def log_task(
        self,
        task: Union[Task, BaseTask],
        show_source: bool = True,
        show_job: bool = True,
        indent: int = 0,
        detail: bool = True,
    ) -> None:
        """
        Display a Task.
        """
        if isinstance(task, Task):
            tags = task.tags
        else:
            tags = []

        self.display(
            "Task {hash} {name} {tags}".format(
                hash=format_id(task.hash, detail),
                name=task.fullname,
                tags=format_tags(tags),
            ),
            indent=indent,
        )

        if detail:
            if show_job and isinstance(task, Task):
                jobs = sorted(task.jobs, key=lambda job: job.start_time)
                if jobs:
                    job = jobs[0]
                    self.display(
                        "First Job {job_id} {start_time}".format(
                            job_id=job.id[:8],
                            start_time=job.start_time,
                        ),
                        indent=indent + 2,
                    )

            self.display_doc_tags(tags, indent=indent + 2)

            if show_source:
                self.display()
                lines = task.source.split("\n")
                for line in lines:
                    self.display(line, indent=indent + 2)

    def log_call_node(
        self,
        call_node: CallNode,
        indent: int = 0,
        show_children: bool = True,
        show_parents: bool = True,
        show_result: bool = True,
        show_task: bool = True,
        show_dataflow: bool = True,
        detail: bool = True,
    ) -> None:
        """
        Display a CallNode.
        """
        self.display(
            "CallNode {call_hash} {task} {tags}".format(
                call_hash=format_id(call_node.call_hash, detail),
                task=call_node.task.fullname,
                tags=format_tags(call_node.tags),
            ),
            indent=indent,
        )
        if detail:
            if show_result:
                self.display(
                    "Args:   {args}".format(args=format_arguments(call_node.arguments)),
                    indent=indent + 2,
                )

                if call_node.value.type == "redun.ErrorValue":
                    # Protect against not being able to unpickle the error.
                    error = (
                        call_node.value_parsed.error
                        if isinstance(call_node.value_parsed, ErrorValue)
                        else "Unknown"
                    )
                    self.display(
                        "Raised: {error}".format(error=trim_string(repr(error))),
                        indent=indent + 2,
                    )
                else:
                    self.display(
                        "Result: {result}".format(
                            result=trim_string(repr(call_node.value_parsed))
                        ),
                        indent=indent + 2,
                    )

            if show_parents and call_node.parents:
                self.display("Parent CallNodes:", indent=indent + 2)
                for parent_node in call_node.parents:
                    self.log_call_node(
                        parent_node,
                        indent=indent + 4,
                        show_children=False,
                        show_parents=False,
                        show_result=False,
                        detail=False,
                    )

            if show_children and call_node.children:
                self.display("Child CallNodes:", indent=indent + 2)
                for child_node in call_node.children:
                    self.log_call_node(
                        child_node,
                        indent=indent + 4,
                        show_children=show_children,
                        show_parents=False,
                        detail=False,
                    )

            if show_task:
                self.display()
                self.log_task(call_node.task, show_job=False, indent=indent)

            if show_dataflow:
                self.display()
                self.log_dataflow(call_node.value, "result", indent=indent)

    def log_file(self, file: File, kind: Optional[str] = None, indent: int = 0) -> None:
        """
        Display a File.
        """
        assert self.scheduler
        assert isinstance(self.scheduler.backend, RedunBackendDb)
        assert self.scheduler.backend.session

        # Get first CallNode producing this File.

        if not kind:
            # Determine file display kind. Prefer results.
            if any(value.results for value in file.values):
                kind == "result"
            elif any(value.arguments for value in file.values):
                kind = "arg"
            else:
                kind = None

        verb: Optional[str]
        job: Optional[Job]

        if kind is None:
            verb = ""
            job = None

        elif kind == "result":
            verb = "Produced"
            # Earlier job end is deepest.
            call_node = sorted(
                (call_node for value in file.values for call_node in value.results),
                key=lambda call_node: min(job.end_time for job in call_node.jobs),
            )[0]
            job = min(call_node.jobs, key=lambda job: job.end_time)

        elif kind == "arg":
            verb = "Consumed"
            # Latest job start is deepest.
            call_node = sorted(
                (arg.call_node for value in file.values for arg in value.arguments),
                key=lambda call_node: max(job.start_time for job in call_node.jobs),
            )[-1]
            job = max(call_node.jobs, key=lambda job: job.start_time)

        else:
            raise ValueError('Unknown kind "{}"'.format(kind))

        query = CallGraphQuery(self.scheduler.backend.session).filter_ids([file.value_hash])
        self.log_files(query, indent=indent)

        if verb and job:
            self.display("{verb} by Job {job}".format(verb=verb, job=job.id[:8]))
            self.display()
            self.log_job(job, indent=indent + 2)

    def log_value(self, value: Value, indent: int = 0, detail: bool = True) -> None:
        """
        Display a Value.
        """
        tags = format_tags(value.tags)

        if detail:
            if value.file:
                self.log_file(value.file, indent=indent)
            else:
                self.display(
                    "Value {hash} {type} {tags}".format(
                        hash=value.value_hash[:8],
                        type=value.type,
                        tags=tags,
                    ),
                    indent=indent,
                )
                self.display(
                    "{value}".format(
                        value=repr(value.value_parsed) or value.type,
                    ),
                    indent=indent,
                )

            # Display dataflow.
            self.display()
            self.log_dataflow(value, indent=indent)

        else:
            self.display(
                "Value {hash} {type} {value} {tags}".format(
                    hash=value.value_hash[:8],
                    type=value.type,
                    value=trim_string(repr(value.preview) or value.type),
                    tags=tags,
                ),
                indent=indent,
            )

    def log_dataflow(self, value: Value, value_name: str = "value", indent: int = 0):
        """
        Display the upstream dataflow of a Value.
        """
        assert self.scheduler
        assert isinstance(self.scheduler.backend, RedunBackendDb)

        self.display("Upstream dataflow:\n", indent=indent)
        self.display(
            "{value_name} = {value}\n".format(
                value_name=value_name,
                value=repr(value.preview),
            ),
            indent=indent + 2,
        )

        edges = walk_dataflow(self.scheduler.backend, value)
        dom = make_dataflow_dom(edges, new_varname=value_name)
        lines = display_dataflow(dom)
        for line in lines:
            self.display(line, indent=indent + 2)

    def log_files(self, query: CallGraphQuery, indent: int = 0) -> None:
        """
        Display File-specific query results.
        """
        assert self.scheduler
        assert isinstance(self.scheduler.backend, RedunBackendDb)
        session = cast(RedunBackendDb, self.scheduler.backend).session
        assert session

        # TODO: Make config option.
        show_all_files = False

        # Just select the value_hash as an optimization.
        # We use a CTE in order to force the Values subquery to complete before
        # joining with Files.
        query = query.clone(values=session.query(Value.value_hash))
        value_cte = query.build()._values.cte()
        value_subquery = sa.select("*").select_from(value_cte)
        files, value_hashes = zip(
            *(
                (file, file.value_hash)
                for file, _ in (
                    session.query(File, File.path)
                    .filter(File.value_hash.in_(value_subquery))
                    .order_by(File.path)
                    .all()
                )
            )
        )

        # Build query file tags.
        tags = session.query(Tag).filter(Tag.entity_id.in_(value_hashes))
        value_hash2tags = defaultdict(list)
        for tag in tags:
            value_hash2tags[tag.entity_id].append(tag)

        # Display results.
        prev_path = None
        for file in files:
            if file.path == prev_path and not show_all_files:
                continue

            self.display(
                "File {hash} {path} {tags}".format(
                    hash=file.value_hash[:8],
                    path=file.path if prev_path != file.path else "...",
                    tags=format_tags(value_hash2tags[file.value_hash]),
                ),
                indent=indent,
            )
            prev_path = file.path

    def console_command(self, args: Namespace, extra_args: List[str], argv: List[str]) -> None:
        """
        Performs the console command (CLI GUI).
        """
        scheduler = self.get_scheduler(args)

        from redun.console.app import RedunApp

        RedunApp(scheduler, args, extra_args, argv).run()

    def repl_command(self, args: Namespace, extra_args: List[str], argv: List[str]) -> None:
        """
        Get a read-eval-print-loop for querying the history.
        """
        scheduler = self.get_scheduler(args)
        session = cast(RedunBackendDb, scheduler.backend).session
        assert session

        # Put commonly used variables in the environment.
        def get_executions():
            return (
                session.query(Execution)
                .join(Job, Execution.job_id == Job.id)
                .order_by(Job.start_time.desc())
                .all()
            )

        def query(expr, *args, **kwargs):
            if isinstance(expr, str):
                return self.infer_id(expr)
            else:
                return session.query(expr, *args, **kwargs)

        execs = get_executions()
        if execs:
            # Create reference to last Execution as it is handy to have available in the repl.
            last_exec = execs[0]  # noqa: F841

        import pdb

        if extra_args:
            obj = query(extra_args[0])
            if obj:
                self.display("obj = query('{}')".format(extra_args[0]))
                self.display(repr(obj) + "\n")

        # Start REPL using pdb.
        _pdb = pdb.Pdb()
        _pdb.prompt = "(redun) "
        _pdb.set_trace()

    def tag_list_command(self, args: Namespace, extra_args: List[str], argv: List[str]) -> None:
        """
        Search tags.
        """
        session = self.get_session(args)
        max_length = sys.maxsize

        if args.values:
            # Show tag counts.
            query = session.query(
                Tag.entity_type, Tag.key, Tag.value, sa.func.count(Tag.key)
            ).filter(Tag.is_current.is_(True), Tag.key != "")

            # Apply filters.
            if extra_args:
                tags = [parse_tag_key_value(tag, value_required=False) for tag in extra_args]
                for key, value in tags:
                    if value is ANY_VALUE:
                        query = query.filter(Tag.key == key)
                    else:
                        query = query.filter(Tag.key == key, Tag.value == sa_cast(value, JSON))

            rows = (
                query.group_by(Tag.entity_type, Tag.key, Tag.value)
                .order_by(Tag.key, Tag.value, Tag.entity_type)
                .all()
            )

            key_value2counts = defaultdict(list)
            for entity_type, key, value, count in rows:
                key_value2counts[(key, value)].append((entity_type, count))

            # Display results.
            self.display("Tags:")
            for (key, value), counts in sorted(key_value2counts.items()):
                self.display(
                    "  {key_value:30} ({counts})".format(
                        key_value=format_tag_key_value(key, value, max_length=max_length),
                        counts=", ".join(
                            "{} {}".format(count, entity_type.name)
                            for entity_type, count in counts
                        ),
                    )
                )

        elif not extra_args:
            # Full tag counts.
            keys = (
                session.query(Tag.key, Tag.entity_type, sa.func.count(Tag.key))
                .filter(Tag.is_current.is_(True), Tag.key != "")
                .group_by(Tag.key, Tag.entity_type)
                .order_by(Tag.key, Tag.entity_type)
                .all()
            )
            key2counts = defaultdict(list)
            for key, entity_type, count in keys:
                key2counts[key].append((entity_type, count))

            # Display results.
            self.display("Tags:")
            for key, counts in sorted(key2counts.items()):
                self.display(
                    "  {key:10} ({counts})".format(
                        key=key,
                        counts=", ".join(
                            "{} {}".format(count, entity_type.name)
                            for entity_type, count in counts
                        ),
                    )
                )

        else:
            # Search by tags.

            # Build argv.
            argv = ["log", "--all"]
            for extra_arg in extra_args:
                argv.extend(["--tag", extra_arg])

            # Use log command to perform filtering.
            parser = self.get_command_parser()
            args, extra_args = parser.parse_known_args(argv)
            return self.log_command(args, extra_args, argv)

    def _parse_entity_info(self, id_prefix: str) -> Tuple[str, TagEntity]:
        model_pks = {
            Execution: "id",
            Job: "id",
            CallNode: "call_hash",
            Value: "value_hash",
            Task: "hash",
        }

        entity = self.infer_id(id_prefix, required=True)
        full_id = getattr(entity, model_pks[type(entity)])

        try:
            entity_type = TagEntity(type(entity).__name__)
        except ValueError:
            raise ValueError(
                "Entity {} is not a taggable entity type ({}).".format(
                    full_id, type(entity).__name__
                )
            )

        return full_id, entity_type

    def tag_add_command(self, args: Namespace, extra_args: List[str], argv: List[str]) -> None:
        """
        Add new tags to entities.
        """
        backend = self.get_backend(args)

        # Parse entities from command line.
        i = 0
        entity_ids = []
        while i < len(extra_args) and "=" not in extra_args[i]:
            entity_ids.append(extra_args[i])
            i += 1

        # Parse key values from command line.
        key_values = [parse_tag_key_value(arg) for arg in extra_args[i:]]

        # Add tags.
        for entity_id in entity_ids:
            full_id, entity_type = self._parse_entity_info(entity_id)
            backend.record_tags(entity_type, full_id, key_values, new=True)

            self.display(
                "add tag {entity_type}:{entity_id} {key_values}".format(
                    entity_type=entity_type.name,
                    entity_id=full_id,
                    key_values=" ".join(
                        "{}={}".format(key, json.dumps(value)) for key, value in key_values
                    ),
                )
            )

    def tag_update_command(self, args: Namespace, extra_args: List[str], argv: List[str]) -> None:
        """
        Update entity tags.
        """
        backend = self.get_backend(args)

        # Parse entities from command line.
        i = 0
        entity_ids = []
        while i < len(extra_args) and "=" not in extra_args[i]:
            entity_ids.append(extra_args[i])
            i += 1

        # Parse key values from command line.
        key_values = [parse_tag_key_value(arg) for arg in extra_args[i:]]

        # Add tags.
        for entity_id in entity_ids:
            full_id, entity_type = self._parse_entity_info(entity_id)
            backend.record_tags(entity_type, full_id, key_values, update=True)

            self.display(
                "update tag {entity_type}:{entity_id} {key_values}".format(
                    entity_type=entity_type.name,
                    entity_id=full_id,
                    key_values=" ".join(
                        "{}={}".format(key, json.dumps(value)) for key, value in key_values
                    ),
                )
            )

    def tag_rm_command(self, args: Namespace, extra_args: List[str], argv: List[str]) -> None:
        """
        Delete tags.
        """
        backend = self.get_backend(args)

        # Parse entities from command line.
        i = 0
        entity_ids = []
        while i < len(extra_args) and "=" not in extra_args[i] and extra_args[i] != "--":
            entity_ids.append(extra_args[i])
            i += 1

        if extra_args[i] == "--":
            i += 1

        # Parse key values from command line.
        key_values = [parse_tag_key_value(arg, value_required=False) for arg in extra_args[i:]]
        keys = [key for key, value in key_values if value == ANY_VALUE]
        key_values = [(key, value) for key, value in key_values if value != ANY_VALUE]

        # Delete tags.
        for entity_id in entity_ids:
            full_id, entity_type = self._parse_entity_info(entity_id)
            backend.delete_tags(full_id, key_values, keys)

            self.display(
                "delete tag {entity_type}:{entity_id} {key_values}".format(
                    entity_type=entity_type.name,
                    entity_id=full_id,
                    key_values=" ".join(
                        "{}={}".format(key, json.dumps(value)) for key, value in key_values
                    ),
                )
            )

    def oneshot_command(self, args: Namespace, extra_args: List[str], argv: List[str]) -> Any:
        """
        Evaluate a single task.
        """
        array_job_index: int = -1
        if args.array_job:
            index = get_job_array_index()
            if index is None:
                raise RedunClientError("Array job environment variable not set")
            else:
                array_job_index = index

            # Get path to actual error file based on index.
            if args.error:
                with BaseFile(args.error).open() as error_spec_file:
                    efiles = json.load(error_spec_file)
                args.error = efiles[array_job_index]

        # Remove previous error file if it exists to avoid reporting stale information.
        if args.error:
            BaseFile(args.error).remove()

        try:
            # Extract code package if specified.
            if args.code:
                code_file = BaseFile(args.code)
                extract_tar(code_file, ".")

            # Setup import paths.
            for path in args.import_path:
                add_import_path(path)

            # Get main task.
            import_script(args.script)

            # Determine if module-level help is needed.
            if args.task == "help":
                tasks = sorted(get_task_registry(), key=lambda task: task.fullname)
                self.display("Tasks available:")
                for task in tasks:
                    self.display("  ", task.fullname)
                return None

            # Get requested task.
            task = get_task_registry().get(args.task)
            if not task:
                raise RedunClientError('Unknown task "{}"'.format(args.task))

            output_path = args.output
            if args.array_job:
                # Get path to actual output file based on index.
                if args.output:
                    with BaseFile(args.output).open() as output_spec_file:
                        ofiles = json.load(output_spec_file)
                    output_path = ofiles[array_job_index]

            # Determine arguments for task.
            if args.input:
                # Parse task args from input file.
                input_file = BaseFile(args.input)
                with input_file.open("rb") as infile:
                    task_args, task_kwargs = pickle.load(infile)

                    # If it's an array job, args will be a list of args for the
                    # array, and we find ours by index
                    if args.array_job:
                        task_args = task_args[array_job_index]
                        task_kwargs = task_kwargs[array_job_index]

            else:
                # Parse task args from cli.
                task_arg_parser, cli2arg = get_task_arg_parser(task, include_defaults=True)
                task_opts = task_arg_parser.parse_args(extra_args)

                # Parse cli arguments to task arguments.
                task_args = ()
                task_kwargs = {}
                for dest, arg in cli2arg.items():
                    value = getattr(task_opts, dest)
                    if value is not None:
                        task_kwargs[arg] = value

            logger.info("redun :: version {}".format(redun.__version__))
            logger.info("oneshot:  redun {argv}".format(argv=" ".join(map(quote, argv[1:]))))

            # If output already exists, exit.
            if output_path and not args.no_cache:
                output_file = BaseFile(output_path)
                if output_file.exists():
                    with output_file.open("rb") as infile:
                        result = pickle.load(infile)
                    if get_type_registry().is_valid_nested(result):
                        logger.info("Existing output found in {ofile}".format(ofile=output_path))
                        return result

                # Remove previous output if it exists to avoid reporting stale information.
                output_file.remove()

            result = task.func(*task_args, **task_kwargs)

            if output_path:
                # Write result output.
                output_file = BaseFile(output_path)
                with output_file.open("wb") as out:
                    pickle_dump(result, out)
            else:
                self.display(result)

            return result

        except Exception as error:
            if args.error:
                # Write error and traceback.
                error_traceback = Traceback.from_error(error)
                with BaseFile(args.error).open("wb") as out:
                    pickle_dump((error, error_traceback), out)
            raise error

    def db_info_command(self, args: Namespace, extra_args: List[str], argv: List[str]) -> None:
        """
        Display information about redun repo database.
        """
        backend = setup_backend_db(args.config, args.repo)
        backend.create_engine()

        version = backend.get_db_version()
        min_version, max_version = backend.get_db_version_required()
        is_compat = backend.is_db_compatible()

        self.display(f"redun :: version {redun.__version__}")
        self.display(f"config dir: {get_config_dir(args.config)}")
        self.display()
        self.display(f"db version: {version} '{version.description}'")
        self.display(f"CLI requires db versions: >={min_version},<{max_version.major + 1}")
        self.display(f"CLI compatible with db: {is_compat}")

    def db_upgrade_command(self, args: Namespace, extra_args: List[str], argv: List[str]) -> None:
        return self.db_migrate_command("upgrade", args, extra_args, argv)

    def db_downgrade_command(
        self, args: Namespace, extra_args: List[str], argv: List[str]
    ) -> None:
        return self.db_migrate_command("downgrade", args, extra_args, argv)

    def db_migrate_command(
        self, direction: str, args: Namespace, extra_args: List[str], argv: List[str]
    ) -> None:
        """
        Migrate redun repo database to a new version.
        """
        backend = setup_backend_db(args.config, args.repo)
        backend.create_engine()

        version = backend.get_db_version()
        desired_version = parse_db_version(args.db_version)

        self.display(f"redun :: version {redun.__version__}")
        self.display(f"config dir: {get_config_dir(args.config)}")
        self.display()
        self.display(f"Initial db version: {version}")

        if desired_version == version:
            self.display("No migration needed.")
            return
        elif desired_version > version and direction != "upgrade":
            raise RedunClientError(
                f"Must use upgrade command to migrate from {version} to {desired_version}."
            )
        elif desired_version < version and direction != "downgrade":
            raise RedunClientError(
                f"Must use downgrade command to migrate from {version} to {desired_version}."
            )

        backend.migrate(desired_version)

        version = backend.get_db_version()
        self.display(f"Final db version: {version}")

    def db_versions_command(self, args: Namespace, extra_args: List[str], argv: List[str]) -> None:
        """
        Display all available redun repo database versions.
        """
        scheduler = self.get_scheduler(args, migrate=False)
        backend = cast(RedunBackendDb, scheduler.backend)
        backend.create_engine()

        table = [
            ["Version", "Migration", "Description"],
        ]
        table.extend(
            [str(info), info.migration_id, info.description]
            for info in backend.get_all_db_versions()
        )
        for line in format_table(table, "lll"):
            self.display(line)

    def export_command(self, args: Namespace, extra_args: List[str], argv: List[str]) -> None:
        """
        Export records from redun database.
        """
        scheduler = self.get_scheduler(args)
        session = cast(RedunBackendDb, scheduler.backend).session
        assert session

        if extra_args:
            root_ids = extra_args
        else:
            rows = (
                session.query(Execution.id)
                .join(Job, Execution.job_id == Job.id)
                .order_by(Job.start_time.desc())
                .all()
            )
            root_ids = [row[0] for row in rows]

        record_ids = scheduler.backend.iter_record_ids(root_ids)
        records = scheduler.backend.get_records(record_ids)

        if args.file == "-":
            out = sys.stdout
            do_close = False
        else:
            out = cast(TextIO, BaseFile(args.file).open("w"))
            do_close = True

        for record in records:
            out.write(json.dumps(record))
            out.write("\n")

        if do_close:
            out.close()

    def import_command(self, args: Namespace, extra_args: List[str], argv: List[str]) -> None:
        """
        Import records into redun database.
        """
        scheduler = self.get_scheduler(args)

        def read_json_lines(infile):
            for line in infile:
                yield json.loads(line)

        infile: IO[Any]

        if args.file == "-":
            infile = sys.stdin
            do_close = False
        else:
            infile = BaseFile(args.file).open()
            do_close = True

        records = read_json_lines(infile)
        scheduler.backend.put_records(records)

        if do_close:
            infile.close()

    def repo_add_command(self, args: Namespace, extra_args: List[str], argv: List[str]) -> None:
        # Convert relative to absolute path.
        repo_config_path = get_abs_path(args.repo_config_dir)

        # The repo name 'default' is reserved for the default repo.
        if args.repo_name == DEFAULT_REPO_NAME:
            raise RedunClientError("Repositories may not be named 'default'")

        # Only write if repo is not already configured.
        config = setup_config(args.config, initialize=True)
        repos = config.get("repos", {})
        for repo_name, repo in repos.items():
            if repo_name == args.repo_name:
                raise RedunClientError(f"Repository '{args.repo_name}' already configured")

            # Compare absolute paths only for local repositories.
            if get_abs_path(repo.get("config_dir")) == repo_config_path:
                self.display(
                    f"Existing repository '{repo_name}' also uses config at {repo_config_path}"
                )

        # Ensure config file actually exists and is valid.
        repo_config = BaseFile(get_config_path(repo_config_path))
        if not repo_config.exists():
            raise FileNotFoundError(f"Config file {repo_config.path} not found")
        _ = setup_config(repo_config_path, initialize=False)

        # Don't write with configparser as it can delete comments.
        config_path = get_config_path(args.config)
        with open(config_path, "a") as conf:
            conf.write(f"\n[repos.{args.repo_name}]\n")
            conf.write(f"config_dir = {repo_config_path}\n")

    def repo_remove_command(self, args: Namespace, extra_args: List[str], argv: List[str]) -> None:
        # Check repo is in the config file
        config = setup_config(args.config)
        repos = config.get("repos", {}).get(args.repo_name)
        if repos is None:
            raise RedunClientError(f"Repository '{args.repo_name}' is not in the config")

        raise RedunClientError(
            f"Please manually edit your config file '{get_config_path(args.config)}' to remove "
            f"the section titled [repos.{args.repo_name}]"
        )

    def repo_list_command(self, args: Namespace, extra_args: List[str], argv: List[str]) -> None:
        config = setup_config(args.config)
        self.display("Repositories:")

        repos = config.get("repos", {})
        for repo_name, repo in sorted(repos.items()):
            repo_path = repo.get("config_dir")
            if not repo_path:
                raise RedunClientError(
                    "Malformed config: config_dir not specified for repository {}".format(
                        repo_name
                    )
                )
            self.display(f"{repo_name}: {repo_path}", indent=2)

    def get_record_ids(self, prefix_ids: Sequence[str]) -> List[str]:
        """
        Expand prefix record ids to full record ids.
        """
        records = [self.infer_id(id, include_files=False) for id in prefix_ids]
        unknown_ids = [prefix for prefix, record in zip(prefix_ids, records) if record is None]
        if unknown_ids:
            raise RedunClientError(f"Unknown record ids: {' '.join(unknown_ids)}")

        def get_record_id(record):
            return getattr(record, CallGraphQuery.MODEL_PKS[type(record)])

        return list(map(get_record_id, records))

    def push_command(self, args: Namespace, extra_args: List[str], argv: List[str]) -> None:
        """
        Push records into a redun repository.
        """
        dest_config = setup_config(args.config, repo=args.push_repo)
        dest_backend = RedunBackendDb(config=dest_config.get("backend"))
        src_backend = cast(RedunBackendDb, self.get_scheduler(args).backend)

        if dest_backend.db_uri == src_backend.db_uri:
            raise RedunClientError(f"Cannot push repo {args.push_repo} to itself")
        dest_backend.load()

        root_ids = self.get_record_ids(extra_args) if extra_args else None
        num_records = self._sync_records(src_backend, dest_backend, root_ids)
        if num_records:
            self.display(f"Pushed {num_records} record(s) to repo '{args.push_repo}'")
        else:
            self.display(f"Repo '{args.push_repo}' is up to date.")

    def pull_command(self, args, extra_args, argv):
        """
        Pull records into another redun database.
        """
        src_config = setup_config(args.config, repo=args.pull_repo)
        src_backend = RedunBackendDb(config=src_config.get("backend"))

        dest_backend = self.get_scheduler(args).backend
        if dest_backend.db_uri == src_backend.db_uri:
            raise RedunClientError(f"Cannot pull repo {args.push_repo} from itself")
        src_backend.load()

        root_ids = self.get_record_ids(extra_args) if extra_args else None
        num_records = self._sync_records(src_backend, dest_backend, root_ids)
        self.display(f"Pulled {num_records} new record(s) from repo '{args.pull_repo}'")

    def _sync_records(
        self,
        src_backend: RedunBackendDb,
        dest_backend: RedunBackendDb,
        root_ids: Optional[List[str]] = None,
    ) -> int:
        assert src_backend.session
        assert dest_backend.session

        if not root_ids:
            rows = (
                src_backend.session.query(Execution.id)
                .join(Job, Execution.job_id == Job.id)
                .order_by(Job.start_time.desc())
                .all()
            )
            root_ids = [row[0] for row in rows]

        record_ids = src_backend.iter_record_ids(root_ids)
        records = src_backend.get_records(record_ids)

        num_records = dest_backend.put_records(records)
        return num_records

    def aws_list_jobs_command(
        self, args: Namespace, extra_args: List[str], argv: List[str]
    ) -> None:
        """
        List AWS Batch jobs.
        """
        scheduler = self.get_scheduler(args)

        # Get non-debug AWS executors.
        executors = [
            executor
            for executor in scheduler.executors.values()
            if isinstance(executor, AWSBatchExecutor) and not executor.debug
        ]
        statuses = args.status.split(",") if args.status else None

        for executor in executors:
            self.display("Executor {} jobs:".format(executor.name))

            jobs = sorted(
                executor.get_jobs(statuses=statuses),
                key=lambda job: job["createdAt"],
                reverse=True,
            )
            for job in jobs:
                self.display("  " + json.dumps(job, sort_keys=True))
            self.display()

    def aws_kill_jobs_command(
        self, args: Namespace, extra_args: List[str], argv: List[str]
    ) -> None:
        """
        Kill AWS Batch jobs.
        """
        scheduler = self.get_scheduler(args)

        # Get AWS executors.
        executors = [
            executor
            for executor in scheduler.executors.values()
            if isinstance(executor, AWSBatchExecutor)
        ]
        statuses = args.status.split(",") if args.status else None

        for executor in executors:
            job_ids = [job["jobId"] for job in executor.get_jobs(statuses=statuses)]
            for job_id in job_ids:
                self.display("Killing job {}...".format(job_id))
                executor.kill_jobs([job_id])

    def aws_logs_command(self, args: Namespace, extra_args: List[str], argv: List[str]) -> None:
        """
        Fetch AWS Batch job logs.
        """
        scheduler = self.get_scheduler(args)
        executors = [
            executor
            for executor in scheduler.executors.values()
            if isinstance(executor, AWSBatchExecutor)
        ]
        aws_region2executor = {executor.aws_region: executor for executor in executors}
        statuses = args.status.split(",") if args.status else None
        default_aws_region = list(aws_region2executor.keys())[0]

        # Build up query of log streams.
        query: List[Tuple[dict, Optional[Job], str, str]] = []
        if args.all:
            for executor in aws_region2executor.values():
                for batch_job in executor.get_jobs(statuses=statuses):
                    args.batch_job.append(batch_job["jobId"])

        for job_id in args.job:
            job = self.infer_id(job_id)
            if isinstance(job, Job):
                for tag in job.tags:
                    if tag.key == "aws_log_stream":
                        query.append(({}, job, tag.value, default_aws_region))

        if args.batch_job:
            for aws_region in aws_region2executor.keys():
                for batch_job in aws_describe_jobs(args.batch_job, aws_region=aws_region):
                    log_stream = batch_job["container"].get("logStreamName")
                    query.append((batch_job, None, log_stream, aws_region))

        if args.log_stream:
            query.extend(
                [({}, None, log_stream, default_aws_region) for log_stream in args.log_stream]
            )

        query.sort(key=lambda query: query[0].get("createdAt", 0), reverse=True)

        # Display logs
        for batch_job, job, log_stream, aws_region in query:
            # Display header.
            if job:
                self.display(f"# redun Job {job.id}")
            if batch_job:
                self.display(f"# AWS Batch job {batch_job['jobId']}")
            if log_stream:
                self.display(f"# AWS Log Stream {log_stream}")

            # Display events.
            events = iter_log_stream(
                log_group_name=BATCH_LOG_GROUP,
                log_stream=log_stream,
                aws_region=aws_region,
            )
            for event in events:
                self.display(format_log_stream_event(event))

            self.display()

    def fs_command(self, args: Namespace, extra_args: List[str], argv: List[str]) -> None:
        """
        Give information on redun-supported filesystems.
        """
        filesystems = sorted(cls.name for cls in list_filesystems())
        self.display("Supported filesystems:")
        for fs in filesystems:
            self.display(f"  {fs}")

    def fs_copy_command(self, args: Namespace, extra_args: List[str], argv: List[str]) -> None:
        """
        Copy files between redun-supported filesystems.
        """
        # Detect stdin and stdout paths ('-').
        src_path = args.src_path if args.src_path != "-" else None
        dest_path = args.dest_path if args.dest_path != "-" else None
        copy_file(src_path, dest_path)

    def server_command(self, args: Namespace, extra_args: List[str], argv: List[str]) -> None:
        """
        Run redun local web server UI.
        """
        logger.info(
            "Attempting to start the redun server application, with the assumption that "
            "the `redun server` command was run from the root of the redun repository"
        )
        config: Config = setup_config(args.config, repo=args.repo)
        compose_env: Dict[str, Any] = {
            **os.environ,
            **{"REDUN_SERVER_DEV": 0, "TARGET": "prod-image"},
        }
        parsed_db_uri: ParseResult = urlparse(config["backend"]["db_uri"])
        if config["backend"].get("db_aws_secret_name"):
            # RedunBackendDB will handle fetching the AWS secret and establishing a connection
            # the configured database. Pass the secret name to the container via an environment
            # variable.
            compose_env.update(
                {"REDUN_DB_AWS_SECRET_NAME": config["backend"]["db_aws_secret_name"]}
            )
            compose_profile = "postgres_remote"
        elif parsed_db_uri.scheme == "postgresql":
            if parsed_db_uri.hostname == "localhost":
                db_port: int = parsed_db_uri.port or DEFAULT_POSTGRESQL_PORT
                # `localhost` for the containerized application is different from that for the
                # host machine. Provide the application with the hostname of the host, so it can
                # connect to the database instance serving there.
                parsed_db_uri = parsed_db_uri._replace(netloc=f"{gethostname()}:{db_port}")
                if is_port_in_use("localhost", db_port):
                    # If there is a local database instance already running, only the application
                    # needs to be spun up. Set the Compose profile to `postgres_remote` to convey
                    # that the DB is independently (ie. not through this Compose file) managed
                    logger.info(
                        "Detected service already running on localhost:{db_port}, as configured "
                        "in the DB URI. Starting the application."
                    )
                    compose_profile = "postgres_remote"
                else:
                    logger.info(
                        "Did not detect a running service at the location configured in the DB "
                        f"URI (localhost:{db_port}). Starting the application and PostgreSQL "
                        f"database service through Compose"
                    )
                    # The `postgres_local` Compose profile will spin up an instance of the DB
                    compose_profile = "postgres_local"
                    # Set env var used to bind DB service instance port to the host
                    compose_env.update({"POSTGRESQL_PORT": db_port})
            else:
                compose_profile = "postgres_remote"
            compose_env.update({"REDUN_DB_URI": parsed_db_uri.geturl()})
        elif parsed_db_uri.scheme == "sqlite":
            compose_profile = "sqlite"
            if "s3://" in parsed_db_uri.path:
                raise RedunClientError(
                    "SQLite files in S3 aren't currently a supported backend for redun server. "
                    "Please consider importing its records to a local SQLite database and "
                    "retrying."
                )
            if ":memory:" not in parsed_db_uri.path:
                # Make the SQLite DB accessible to the container by mounting its parent directory
                # as a volume, and pointing the application to it
                sqlite_db_filepath: str = parsed_db_uri.path[1:]
                compose_env.update({"SQLITE_DB_FILE": sqlite_db_filepath})
        else:
            raise RedunClientError(
                f"redun server supports SQLite and PostgreSQL backends. Found {config['backend']}"
            )
        subprocess.run(
            f"docker-compose -f redun_server/docker-compose.yml --profile={compose_profile} up",
            env={k: str(v) for (k, v) in compose_env.items() if v is not None},
            shell=True,
        )
