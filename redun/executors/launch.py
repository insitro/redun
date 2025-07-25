import shlex
from typing import List, Optional, Union, cast

from redun.config import Config
from redun.executors.base import Executor, get_executor_from_config
from redun.expression import TaskExpression
from redun.logging import logger
from redun.scheduler import Execution, Job, Scheduler
from redun.scripting import script_task
from redun.task import hash_args_eval
from redun.value import get_type_registry


def launch_script(
    config: Config,
    script_command: Union[List[str], str],
    executor: Optional[Executor] = None,
    executor_name: Optional[str] = None,
    task_options: Optional[dict] = None,
    execution_id: Optional[str] = None,
) -> None:
    """
    Submit the provided script command to the executor, then exit.

    Use a local scheduler with the default config. This means we won't record the entry point,
    but we have no intention of being around long enough to record the results, so there's not
    much point.

    WARNING: This won't actually work on all executor types, such as the local ones. To work,
    the executor needs to be "fire and forget" for `submit_script`.

    Parameters
    ----------
    config : Config
        Config object containing executor information.
    script_command : Union[List[str], str]
        The script command to run on the executor.
    executor : Optional[Executor]
        Optional Executor on which to run the script.  Must be set if executor_name is not set.
    executor_name : Optional[str]
        Name of the executor in the config.  Must be set if executor is not set.
    task_options : Optional[dict]
        Task options to pass to the script task.
    execution_id : Optional[str]
        If provided, use this execution id. This is only relevant for tagging.

    Returns
    -------
        None
    """

    if executor is None:
        assert executor_name is not None, "Must provide an executor by object or by name."
        executor = get_executor_from_config(config.get("executors", {}), executor_name)

    scheduler = Scheduler()
    execution = Execution(id=execution_id)
    executor.set_scheduler(scheduler)

    # Prepare command to execute within Executor.
    # Prepare command to execute within Executor.
    if isinstance(script_command, list):
        remote_run_command = shlex.join(script_command)
    else:
        remote_run_command = script_command

    task_options = task_options or {}
    task_options["executor"] = executor_name

    # Setup job for inner run command.
    run_expr = cast(TaskExpression, script_task.options(**task_options)(remote_run_command))

    logger.info(f"Run within Executor {executor_name}: {remote_run_command}")

    # Submit directly to executor and immediately exit.
    job = Job(script_task, run_expr, execution=execution)
    script_args = ()
    script_kwargs = {"command": remote_run_command}
    job.eval_hash, job.args_hash = hash_args_eval(
        get_type_registry(), script_task, script_args, script_kwargs
    )
    job.args = script_args, script_kwargs

    # Submit job to executor.
    executor.submit_script(job)
    executor.stop()  # stop the monitor thread.
