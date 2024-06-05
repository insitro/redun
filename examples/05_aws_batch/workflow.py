import subprocess
from typing import Dict

# This is an example of import redun tasks from a library.
from task_lib.utils import lib_task_on_batch

from redun import File, script, task

redun_namespace = "redun.examples.aws_batch"


@task()
def task_on_default(x: int) -> list:
    # Since we don't specify an `executor` task option, this task will run on
    # the 'default' executor, i.e. a local thread.

    # In this example, we run the command `uname -a` to get the OS and hostname
    # information in order to provde the task is running locally. For example, if
    # you are running this code on MacOSX, you would see the OS name 'Darwin'.
    return ["task_on_default", subprocess.check_output(["uname", "-a"]), x]


@task(executor="batch", memory=0.5, vcpus=1, batch_tags={"stage": "2", "level": "high"})
def task_on_batch(x: int) -> list:
    # As you can see in the task options, this task will run on the 'batch' executor.
    # We have configured .5Gb of memory and 1 vCPU.

    # This workflow python code will be packaged up in a tarfile, uploaded to S3, and
    # then downloaded into the container on AWS Batch. When the container starts, this
    # task will be called.

    # Again, we use the command `uname -a` to prove we are running the container.
    # Most likely you will see the OS name 'Linux' and the hostname of an EC2 instance.

    # One other thing to point out in this example, we are free to call other tasks that
    # don't run in batch, such as `task_on_default`. It might seem impossible for a Batch
    # task to invoke a task to run back on the local machine, however, recall that
    # calling a task initially returns a lazy expression. The lazy expression is the
    # return value from the batch task, and the local scheduler will do a follow up evaluation
    # to run `task_on_default` locally.

    return [
        "task_on_batch",
        subprocess.check_output(["uname", "-a"]),
        task_on_default(x + 5),
        x,
    ]


@task(executor="batch_debug")
def task_on_batch_debug(x: int) -> list:
    # This task will run within a local Docker container, but overall will follow
    # the same execution steps as a container running on AWS Batch. This can be
    # useful for debugging a workflow locally. For example, you should be able to
    # see the stdout in your terminal and start the Python debugger.
    return [
        "task_on_batch_debug",
        subprocess.check_output(["uname", "-a"]),
        task_on_default(x + 5),
        x,
    ]


@task()
def count_colors_by_script(data: File, output_path: str) -> Dict[str, File]:
    """
    Count colors using a multi-line script.
    """
    # Here, we use the same script as in 04_script, but now we do File staging
    # to and from S3.
    output = File(output_path + "color-counts.tsv")
    log_file = File(output_path + "color-counts.log")

    return script(
        """
        echo 'sorting colors...' >> log.txt
        cut -f3 data | sort > colors.sorted

        echo 'counting colors...' >> log.txt
        uniq -c colors.sorted | sort -nr > color-counts.txt
        """,
        executor="batch",
        inputs=[data.stage("data")],
        outputs={
            "colors-counts": output.stage("color-counts.txt"),
            "log": log_file.stage("log.txt"),
        },
    )


@task()
def main(output_path: str, y: int = 10) -> list:
    # This is the top-level task of the workflow. Here, we are invoking the
    # different examples of running tasks on different executors. All of their
    # results will be combined into one nested list as shown below.

    # Prepare input data on S3 for a batch job.
    # Copy the local file to an s3 location.
    data = File("data.tsv")
    s3_data = data.copy_to(File(f"{output_path}/data.tsv"))

    return [
        "main",
        subprocess.check_output(["uname", "-a"]),
        task_on_batch(y),
        lib_task_on_batch(y),
        task_on_batch_debug(y),
        count_colors_by_script(s3_data, output_path),
    ]
