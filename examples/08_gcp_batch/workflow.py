from redun import task

redun_namespace = "redun.examples.gcp_batch"


@task(executor="gcp_batch")
def hello_world(greet: str) -> list:
    return [
        "hello_world",
        "echo %s world! This is task ${BATCH_TASK_INDEX}. " % (greet)
        + "This job has a total of ${BATCH_TASK_COUNT} tasks.",
    ]


@task(executor="gcp_batch", script=True)
def hello_world_script(greet: str) -> list:
    return [
        "hello_world_script",
        "echo %s world! This is task ${BATCH_TASK_INDEX}. " % (greet)
        + "This job has a total of ${BATCH_TASK_COUNT} tasks.",
    ]


@task(executor="gcp_batch")
def main(greet: str = "Hello") -> list:
    return ["main", hello_world(greet), hello_world_script(greet)]
