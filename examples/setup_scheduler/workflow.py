# Frequently used configurations could be provided through libraries.
from workflow_lib.executors import Account1Executor, Account2Executor

# Import a task from a workflow library.
from workflow_lib.utils import analysis, postprocess_data

from redun import Scheduler, task
from redun.config import Config
from redun.executors.base import Executor

redun_namespace = "redun.examples.setup"


@task()
def main(data: str = "foo") -> str:
    """
    Example of a project workflow that is built up using tasks from a
    workflow library.
    """
    analyzed_data = analysis(data)
    return postprocess_data(analyzed_data)


def setup_scheduler(config: Config, profile: str = "root") -> Scheduler:
    """
    Custom scheduler setup.
    """
    scheduler = Scheduler(config=config)

    # Example of defining executors programmatically.
    # Here, an executor configured for a specific AWS subaccount
    # (account1) is associated with a particular part of the
    # workflow library ("utils_executor").
    if profile == "account1":
        executor_class: Executor = Account1Executor
    elif profile == "account2":
        executor_class: Executor = Account2Executor
    else:
        raise ValueError("Unknown profile: {}".format(profile))

    scheduler.add_executor(executor_class("analysis_executor"))
    scheduler.add_executor(executor_class("postprocess_executor"))

    return scheduler
