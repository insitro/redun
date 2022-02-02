import sys
import os
import subprocess
import time
from typing import Dict
import random

from redun import File, script, task


redun_namespace = "redun.examples.k8s"
   


@task(script=True, executor="k8s")
def script_false_on_k8s():
    return "false"


@task(script=True, executor="k8s")
def script_true_on_k8s():
    return "true"


@task(executor="k8s")
def subtask_sysexit1(x):
    sys.exit(1)


@task(executor="k8s")
def subtask_sysexit0(x):
    sys.exit(0)

@task(executor="k8s")
def subtask_random_sleep(x):
    t = random.uniform(10,60)
    time.sleep(t)
    coin = random.getrandbits(1)
    print(coin)
    return coin

@task(executor="k8s")
def subtask(x):
    time.sleep(100)
    coin = random.getrandbits(1)
    print(coin)
    return coin
    
@task(executor="batch")
def batch_subtask(x):
    time.sleep(100000)
    coin = random.getrandbits(1)
    print(coin)
    return coin

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
def main(n: int=10):
    #return [subtask_random_sleep(i) for i in range(n)]
    return [subtask(i) for i in range(n)]
    #return [script_fail_on_k8s() for i in range(n)]
    #return [batch_subtask(i) for i in range(n)]

#     return [
#         'main',
#         sleepy_script_on_k8s(),
#         #task_on_k8s(),
#         #script_on_k8s(),
#         #failed_task_on_k8s(),
#     ]
