import subprocess

from redun import task

from task_lib2.utils import lib_task_on_batch


redun_namespace = "redun.examples.aws_batch"


@task()
def task_on_default(x: int):
    return [
        'task_on_default',
        subprocess.check_output(['uname', '-a']),
        x
    ]


@task(executor='batch', version="12")
def task_on_batch(x: int):
    return [
        'task_on_batch',
        subprocess.check_output(['uname', '-a']),
        task_on_default(x + 5),
        x
    ]

@task(executor='batch_debug', interactive=True)
def task_on_batch_debug(x: int):
    import pdb; pdb.set_trace()
    return [
        'task_on_batch_debug',
        subprocess.check_output(['uname', '-a']),
        task_on_default(x + 5),
        x
    ]

@task()
def main(y: int=10):
    return [
        'main',
        subprocess.check_output(['uname', '-a']),
        task_on_batch(y),
        lib_task_on_batch(y),
        #task_on_batch_debug(y),
    ]
