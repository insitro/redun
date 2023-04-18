import json
import os
from typing import Any, Dict, Optional, Tuple

import requests

from redun import File, Scheduler
from redun.config import Config
from redun.executors.aws_utils import DEFAULT_AWS_REGION, get_aws_client
from redun.executors.base import get_executor_from_config
from redun.executors.launch import launch_script
from redun.executors.scratch import SCRATCH_INPUT, get_execution_scratch_file
from redun.expression import SchedulerExpression, TaskExpression
from redun.promise import Promise
from redun.scheduler import Execution, Job, subrun
from redun.task import CacheScope, scheduler_task
from redun.utils import pickle_dump, str2bool


class InvocationException(Exception):
    """
    Custom exception that is raised when there is an error encountered invoking an AWS lambda.
    """

    pass


@scheduler_task(namespace="redun")
def federated_task(
    scheduler: Scheduler,
    parent_job: Job,
    sexpr: SchedulerExpression,
    entrypoint: str,
    *task_args,
    **task_kwargs,
) -> Promise:
    """Execute a task that has been indirectly specified in the scheduler config file by providing
    a `federated_task` and its executor. This allows us to invoke code we do not have locally.

    Since the code isn't visible, the cache_scope for the subrun is always set to CSE.

    Parameters
    ----------
    entrypoint: str
        The name of the `federated_task` section in the config that identifies the task to perform.
    task_args: Optional[List[Any]]
        Positional arguments for the task
    task_kwargs: Any
        Keyword arguments for the task
    """
    federated_task_configs = scheduler.config.get("federated_tasks")
    entrypoint_config = _get_federated_config(federated_task_configs, entrypoint)

    # This argument needs to be parsed.
    if "new_execution" in entrypoint_config:
        entrypoint_config["new_execution"] = str2bool(entrypoint_config["new_execution"])

    # Since we require a namespace, we can simply dot format this ourselves.
    wrapper_task_expr: TaskExpression = TaskExpression(
        task_name=f'{entrypoint_config.pop("namespace")}.{entrypoint_config.pop("task_name")}',
        args=task_args,
        kwargs=task_kwargs,
    )

    subrun_task = subrun(
        expr=wrapper_task_expr,
        executor=entrypoint_config.pop("executor"),
        load_modules=[entrypoint_config.pop("load_module")],
        # Since the code isn't visible, only accept CSE hits.
        cache_scope=CacheScope.CSE,
        **entrypoint_config,
    )

    return scheduler.evaluate(subrun_task, parent_job=parent_job)


@scheduler_task(namespace="redun")
def lambda_federated_task(
    scheduler: Scheduler,
    parent_job: Job,
    sexpr: SchedulerExpression,
    config_name: str,
    entrypoint: str,
    lambda_function_name: str,
    scratch_prefix: str,
    dryrun: bool,
    *task_args,
    **task_kwargs,
) -> Promise[Tuple[str, Dict]]:
    """Submit a task to an AWS Lambda Function, by packaging the inputs and sending as the payload
    with instructions.

    This allows a workflow to trigger execution of a remote workflow, to be executed by another
    service.

    This is fire-and-forget, you do not get back the results. We do not implement a way to wait
    for results, because it's not currently possible to monitor the backend database and gracefully
    determine whether an execution has finished. Specifically, executions look "errored"
    until they succeed, so we would have to rely on timeouts to determine if an error actually
    happened. Or, we would need to create another side-channel to record progress, which we
    did not want to undertake.

    Parameters
    ----------
    config_name : str
        The name of the config to use; the lambda defines how this name is chosen, not redun.
    entrypoint : str
        The name of the entrypoint into the above config.
    url : str
        The name of the AWS Lambda function to invoke
    scratch_prefix : str
        The path where the packaged inputs can be written. This is dictated by the lambda, hence
        is a user input.
    dryrun : bool
        If false, actually invoke the lambda. If true, skip it.

    Other args are intended for the task itself.

    Returns
    -------
        str
            The execution ID we asked the proxy lambda to use
        Dict
            The data package.
    """
    promise: Promise[Tuple[str, Dict]] = Promise()

    execution = Execution()

    input_path = get_execution_scratch_file(scratch_prefix, execution.id, SCRATCH_INPUT)
    with File(input_path).open("wb") as out:
        pickle_dump([task_args, task_kwargs], out)

    request_data = {
        "config_name": config_name,
        "entrypoint": entrypoint,
        "input_path": input_path,
        "execution_id": execution.id,
    }

    if not dryrun:
        aws_region = os.environ.get("AWS_REGION", DEFAULT_AWS_REGION)
        lambda_client = get_aws_client("lambda", aws_region)
        response = lambda_client.invoke(
            FunctionName=lambda_function_name,
            Payload=json.dumps(request_data),
        )
        if not response["StatusCode"] == 200:
            raise InvocationException(
                f"Failed to invoke lambda '{lambda_function_name}'. Encountered erorr "
                f"{response['FunctionError']}: {response['Payload']}"
            )

    promise.do_resolve((execution.id, request_data))
    return promise


@scheduler_task(namespace="redun")
def rest_federated_task(
    scheduler: Scheduler,
    parent_job: Job,
    sexpr: SchedulerExpression,
    config_name: str,
    entrypoint: str,
    url: str,
    scratch_prefix: str,
    dryrun: bool,
    *task_args,
    **task_kwargs,
) -> Promise[Tuple[str, Dict]]:
    """Submit a task to a server over REST, by packaging the inputs and POSTing a data blob
    with instructions.

    This allows a workflow to trigger execution of a remote workflow, to be executed by another
    service.

    This is fire-and-forget, you do not get back the results. We do not implement a way to wait
    for results, because it's not currently possible to monitor the backend database and gracefully
    determine whether an execution has finished. Specifically, executions look "errored"
    until they succeed, so we would have to rely on timeouts to determine if an error actually
    happened. Or, we would need to create another side-channel to record progress, which we
    did not want to undertake.

    Parameters
    ----------
    config_name : str
        The name of the config to use; the server defines how this name is chosen, not redun.
    entrypoint : str
        The name of the entrypoint into the above config.
    url : str
        The URL to POST the result to.
    scratch_prefix : str
        The path where the packaged inputs can be written. This is dictated by the server, hence
        is a user input.
    dryrun : bool
        If false, actually perform the POST. If true, skip it.

    Other args are intended for the task itself.

    Returns
    -------
        str
            The execution ID we asked the proxy to use
        Dict
            The data package.
    """
    promise: Promise[Tuple[str, Dict]] = Promise()

    execution = Execution()

    input_path = get_execution_scratch_file(scratch_prefix, execution.id, SCRATCH_INPUT)
    with File(input_path).open("wb") as out:
        pickle_dump([task_args, task_kwargs], out)

    request_data = {
        "config_name": config_name,
        "entrypoint": entrypoint,
        "input_path": input_path,
        "execution_id": execution.id,
    }

    if not dryrun:
        response = requests.post(url, data=request_data)
        if not response.ok:
            response.raise_for_status()

    promise.do_resolve((execution.id, request_data))
    return promise


def launch_federated_task(
    federated_config_path: str,
    entrypoint: str,
    task_args: Optional[Tuple] = None,
    task_kwargs: Optional[Dict] = None,
    input_path: Optional[str] = None,
    execution_id: Optional[str] = None,
) -> str:
    """
    Launch the described federated task. Among other purposes, this is designed to make it easy
    to implement a REST server that can process the messages from `rest_federated_task`.

    As with the `launch` CLI verb, we briefly start a scheduler to help with sending off the work,
    but immediately shut it down.

    Inputs may be provided in explicit form via `task_args` and `task_kwargs`, or pre-packaged,
    but not both.

    Parameters
    ----------
    federated_config_path : str
        The path to the federated config file to use.
    entrypoint : str
        The name of the entrypoint into the above config.
    task_args : Optional[Tuple]
        Arguments to package and pass to the task.
    task_kwargs : Optional[Dict]
        Arguments to package and pass to the task.
    input_path : Optional[str]
        The path to already-packaged inputs, which must be suitable for use with the
        `redun launch` flag `--input`. This allows a server to forward inputs created by another
        application. This file is not opened, because we don't assume we have the code to
        unpickle the objects inside.
    execution_id : Optional[str]
        If provided, use this execution id. Otherwise, one will be generated for you.

    Returns
    -------
        str
            The execution id we asked the redun invocation to use.
    """
    execution = Execution(execution_id)

    config = Config()
    config.read_path(os.path.join(federated_config_path, "redun.ini"))

    federated_task_configs = config.get("federated_tasks")
    entrypoint_config = _get_federated_config(federated_task_configs, entrypoint)

    executor_name = entrypoint_config["executor"]
    task_name = f"{entrypoint_config['namespace']}.{entrypoint_config['task_name']}"

    executor = get_executor_from_config(config.get("executors", {}), executor_name)
    if not input_path:
        assert (
            task_args is not None and task_kwargs is not None
        ), "Must provide actual data - set () and {} if no arguments are desired."
        assert isinstance(task_args, tuple), f"Expected a tuple but got: {task_args}"
        assert isinstance(task_kwargs, dict), f"Expected a dict but got: {task_kwargs}"
        input_path = get_execution_scratch_file(
            executor.scratch_root(), execution.id, SCRATCH_INPUT
        )
        with File(input_path).open("wb") as out:
            pickle_dump([task_args, task_kwargs], out)

    script_command = [
        "redun",
        "-c",
        entrypoint_config["config_dir"],
        "run",
        "--input",
        input_path,
        "--execution-id",
        execution.id,
        entrypoint_config["load_module"],
        task_name,
    ]

    launch_script(config, executor=executor, script_command=script_command)

    return execution.id


def _get_federated_config(config: Config, entrypoint: str) -> Dict[str, Any]:
    """
    Helper. Extract the named federated config from the provided object. Raise an error
    if not found.
    """

    # Load the federated config and find the specified entrypoint within it.
    assert entrypoint in config, (  # type: ignore
        f"Could not find the entrypoint `{entrypoint}` "
        f"in the provided federated tasks. Found `{list(config.keys())}`"
    )

    entrypoint_config: Dict[str, Any] = dict(config[entrypoint])

    required_keys = {"namespace", "task_name", "load_module", "executor", "config_dir"}
    available_keys = set(entrypoint_config.keys())
    assert required_keys.issubset(available_keys), (
        f"Federated task entry `{entrypoint}` does not have the required keys, missing "
        f"`{required_keys.difference(available_keys)}`"
    )

    # The description is optional, but it shouldn't be in the hash. Just hide it from the
    # downstream subrun, so we don't have to deal with merging a user's `config_args`.
    entrypoint_config.pop("description", None)

    return entrypoint_config
