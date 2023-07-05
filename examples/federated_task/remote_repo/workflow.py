from redun import task

redun_namespace = "remote_namespace"


@task()
def internal_task() -> int:
    return 1


@task()
def internal_task2(x: int) -> int:
    return 2 * x


@task(executor="implementation_executor")
def published_task(x: int, y: int = 0) -> int:
    """This task will be published, so that it is accessible outside this repository. It is
    a normal task and can invoke other tasks as usual. There are no special requirements
    to be published as a federated task, and there is no way to tell from the code here
    whether it has been published.

    However, if this task accepts or returns complex types, the caller will need to have
    enough code to understand them (i.e., can pickle and un-pickle them)."""
    return internal_task() + internal_task2(x) + y
