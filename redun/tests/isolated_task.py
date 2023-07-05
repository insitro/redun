from redun import task


@task
def isolated_task(x):
    """This task is isolated in a module by itself. So, it cannot be used unless the module
    is actually imported."""
    return {
        "result": x,
    }
