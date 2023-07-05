from typing import Any, List

from redun import Scheduler, task
from redun.federated_tasks import federated_task, launch_federated_task, rest_federated_task
from redun.scheduler import Execution

redun_namespace = "redun.examples.federated_task"


@task()
def local_wrapper_typed(x: int) -> int:
    """This task is a thin wrapper for the federated task.

    Here, we have provided type hints to improve type checking when we call it. Redun will behave
    identically in either case.
    """
    return federated_task("entrypoint_name", x=x)


@task()
def local_wrapper_untyped(*args, **kwargs) -> Any:
    """This task is a thin wrapper for the federated task.

    Here, we provided an opaque type signature. Redun will behave identically in either case.
    """
    return federated_task("entrypoint_name", *args, **kwargs)


@task()
def main() -> List[int]:
    """We can call the federated task wrapper like any other ordinary redun task."""
    return [
        local_wrapper_typed(1),
        local_wrapper_untyped(4, y=10),
        federated_task("entrypoint_name", 8, 9),
    ]


if __name__ == "__main__":
    """Demonstrate how to use rest submission of a task."""
    # Run the rest task.
    scheduler = Scheduler()
    execution_id, rest_data = scheduler.run(
        rest_federated_task(
            config_name="published_config/.redun",
            entrypoint="entrypoint_name",
            url="fake",
            scratch_prefix="/tmp/redun",
            dryrun=True,
            x=8,
            y=9,
        )
    )

    print("We would have posted:", rest_data)

    # Use the trivial convention that the name of the config is the same as the path to it.
    rest_data["federated_config_path"] = rest_data.pop("config_name")
    launch_federated_task(**rest_data)

    # Try again where the inputs haven't been packed yet.
    rest_data["execution_id"] = Execution().id
    rest_data.pop("input_path")

    launch_federated_task(**rest_data, task_args=(7,), task_kwargs={"y": 10})

    # It doesn't matter if some are moved from args to kwargs
    rest_data["execution_id"] = Execution().id
    launch_federated_task(**rest_data, task_args=tuple(), task_kwargs={"x": 7, "y": 10})
