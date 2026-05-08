from redun import task

redun_namespace = "redun.examples.custom_executor_config"


@task(executor="my_custom")
def process(data: str) -> str:
    return f"processed: {data}"


@task()
def main(data: str = "hello") -> str:
    """
    Example workflow using a custom executor registered via the config class key.

    The 'my_custom' executor is defined in .redun/redun.ini with:
        type = my_local
        class = my_executors.MyLocalExecutor

    No setup_scheduler function or explicit import is needed.
    """
    return process(data)
