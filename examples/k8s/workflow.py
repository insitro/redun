import sys
import os
import subprocess
import time
from typing import Dict

from redun import File, script, task


redun_namespace = "redun.examples.k8s"
   

@task(executor="k8s")
def subtask(x):
    time.sleep(100)
    return x
    
@task(script=True, executor="k8s")
def sleepy_script_on_k8s():
    return "sleep 60"

@task(executor='k8s')
def task_on_k8s() -> list:
    return [
        'task_on_k8s',
        print("hello stdout"),
        print("hello stderr", file=sys.stderr),
    ]

@task()
def script_on_k8s() -> File:
   # The outer task is just for preparing the script and its arguments.
    return script(  # This inner task will run as a bash script inside the container.
        f"ls /", 
        executor="k8s",
        outputs=File("-"),
    )

@task(executor='k8s')
def failed_task_on_k8s() -> list:
    raise RuntimeError


@task
def main(n=10):
    return [subtask(i) for i in range(n)]

#     return [
#         'main',
#         sleepy_script_on_k8s(),
#         #task_on_k8s(),
#         #script_on_k8s(),
#         #failed_task_on_k8s(),
#     ]
