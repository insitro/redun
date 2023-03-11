import os

from redun import task

redun_namespace = "redun.examples.gcp_batch"


@task(executor="gcp_batch")
def hello_world(greet: str) -> str:

    batch_task_index = os.getenv("BATCH_TASK_INDEX")
    batch_task_count = os.getenv("BATCH_TASK_COUNT")

    return (
        f"{greet} world! This is task {batch_task_index}. "
        f"This job has a total of {batch_task_count} tasks."
    )


@task(executor="gcp_batch", script=True)
def hello_world_script(greet: str) -> str:
    return (
        "echo %s world! This is task ${BATCH_TASK_INDEX}. " % (greet)
        + "This job has a total of ${BATCH_TASK_COUNT} tasks."
    )


@task()
def main(greet: str = "Hello") -> list:

    return [
        "main",
        hello_world_script(greet),
        [hello_world(f"({i}) {greet}") for i in range(1, 4)],
    ]
