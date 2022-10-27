import subprocess

from redun import task

redun_namespace = "redun.examples.aws_batch.task_lib2.utils"


@task(executor="batch", version="12")
def lib_task_on_batch(x: int):
    return ["lib_task_on_batch", subprocess.check_output(["uname", "-a"]), x]
