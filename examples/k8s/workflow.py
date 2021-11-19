import os
import subprocess
from typing import Dict

from redun import File, script, task


redun_namespace = "redun.examples.k8s"

#@task(executor='k8s')
# def task_on_k8s() -> list:
#      return [
#         'task_on_k8s',
#         subprocess.check_output(['uname', '-a']),
#     ]


@task(executor='batch')
def task_on_batch() -> list:
     return [
        'task_on_batch',
        subprocess.check_output(['uname', '-a']),
    ]


@task()
def main() -> list:
    # This is the top-level task of the workflow. Here, we are invoking the
    # different examples of running tasks on different executors. All of their
    # results will be combined into one nested list as shown below.
    return [
        'main',
        #task_on_k8s(),
        task_on_batch(),
    ]
