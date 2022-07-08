import subprocess

from redun import task

redun_namespace = "redun.examples.k8s.task_lib.utils"


@task(executor="k8s", version="12")
def lib_task_on_k8s(x: int):
    return ["lib_task_on_k8s", subprocess.check_output(["uname", "-a"]), x]
