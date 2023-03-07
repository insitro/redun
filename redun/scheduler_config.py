import os
import re
from typing import Optional
from urllib.parse import urlparse, urlunparse

from redun.config import Config, create_config_section
from redun.file import Dir

REDUN_CONFIG_ENV = "REDUN_CONFIG"
REDUN_USER_ENV = "REDUN_USER"
REDUN_CONFIG_DIR = ".redun"
REDUN_INI_FILE = "redun.ini"
DEFAULT_REPO_NAME = "default"
DEFAULT_DB_URI = "sqlite:///redun.db"
DEFAULT_POSTGRESQL_PORT = 5432
DEFAULT_REDUN_INI = """\
# redun configuration.

[backend]
db_uri = {db_uri}

[executors.default]
type = local
max_workers = 20

# [executors.batch]
# type = aws_batch
#
# # Required:
# image =
# queue =
# s3_scratch =
#
# # Optional:
# aws_region =
# role =
# debug = False
"""


def get_abs_url(uri: str, base: str) -> str:
    """
    Returns URI with absolute path.
    """
    if re.match(r"^[^:]+:////", uri):
        # URI starts with four slashes, so it is already absolute.
        return uri

    # Parse db_uri.
    url_parts = urlparse(uri)
    if url_parts.netloc != "":
        # Non-file based URL. Use as is.
        return uri

    # Construct absolute path.
    path = url_parts.path[1:]
    abs_path = os.path.join(base, path)

    # Format a new URL with absolute path.
    abs_url_parts = url_parts._replace(path=abs_path, netloc="")
    abs_uri_prep = urlunparse(abs_url_parts)

    # Fix leading slashes to match sqlalchemy scheme (4 slashes for abs path).
    abs_uri = re.sub(r"^([^:]+):", r"\1:///", abs_uri_prep)
    return abs_uri


def get_abs_db_uri(db_uri: str, config_dir: str, cwd: Optional[str] = None) -> str:
    """
    Returns DB_URI with absolute path.

    If `db_uri` is a relative path, it is assumed to be relative to `config_dir`.
    `config_dir` itself may be relative to the current working directory (cwd).
    """
    if not cwd:
        cwd = os.getcwd()
    abs_config_dir = os.path.normpath(os.path.join(cwd, config_dir))
    abs_db_uri = get_abs_url(db_uri, abs_config_dir)
    return abs_db_uri


def postprocess_config(config: Config, config_dir: str) -> Config:
    """
    Postprocess config.

    Parameters
    ----------
    config : Config
        The body of the config to update. Mutated by this operation.
    config_dir : str
        The location the config came from, for the purposes of recording into the config.
    """
    # Add default repository if not specified in config.
    default_repo = create_config_section({"config_dir": config_dir})
    if config.get("repos"):
        config["repos"][DEFAULT_REPO_NAME] = default_repo
    else:
        config["repos"] = {DEFAULT_REPO_NAME: default_repo}

    # Set default db_uri if not specified in config.
    if not config.get("backend"):
        config["backend"] = create_config_section()
    if not config["backend"].get("db_uri"):
        config["backend"]["db_uri"] = DEFAULT_DB_URI

    # Grandfather old default db_uri.
    if config["backend"]["db_uri"] == "sqlite:///.redun/redun.db":
        config["backend"]["db_uri"] = DEFAULT_DB_URI

    # Convert db_uri to absolute path.
    config["backend"]["db_uri"] = get_abs_db_uri(config["backend"]["db_uri"], config_dir)

    # Populate config_dir in backend section.
    config["backend"]["config_dir"] = config_dir

    # Create scheduler section if needed
    if not config.get("scheduler"):
        config["scheduler"] = create_config_section()

    # Merge executors and federated tasks from any federated imported configs. Throw errors
    # on naming conflicts.
    if not config.get("executors"):
        config["executors"] = {}
    if not config.get("federated_tasks"):
        config["federated_tasks"] = {}

    executors_config = config.get("executors")
    federated_tasks_configs = config.get("federated_tasks")

    federated_config_dirs = config["scheduler"].get("federated_configs", "").strip().split("\n")

    # Skip over any empty strings
    for federated_config_dir in (x for x in federated_config_dirs if x):
        federated_config_file = Dir(federated_config_dir).file(REDUN_INI_FILE)
        federated_config = Config()
        federated_config.read_path(federated_config_file.path)

        federated_executors = federated_config.get("executors", {})
        for executor_name, executor_config in federated_executors.items():
            assert (
                executors_config.get(executor_name) is None
            ), f"Imported executor name `{executor_name}` is a duplicate."
            executors_config[executor_name] = executor_config

        federated_entrypoints = federated_config.get("federated_tasks", {})
        for entrypoint_name, entrypoint_config in federated_entrypoints.items():
            assert (
                federated_tasks_configs.get(entrypoint_name) is None
            ), f"Imported federated_task name `{entrypoint_name}` is a duplicate."
            federated_tasks_configs[entrypoint_name] = entrypoint_config

    return config
