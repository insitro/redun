from redun import task

redun_namespace = "redun.examples.gcp_batch"


@task(executor="gcp_batch", script=True)
def main(greet: str = "Hello") -> str:
    return (
        "echo %s world! This is task ${BATCH_TASK_INDEX}. "
        + "This job has a total of ${BATCH_TASK_COUNT} tasks." % (greet)
    )
