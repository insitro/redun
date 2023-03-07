from typing import Any, List

from redun import task
from redun.scheduler import federated_task

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
