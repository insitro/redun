import os
import subprocess
from typing import Dict

from redun import File, script, task

redun_namespace = "redun.examples.docker"

# Path used for Docker volume mounts. We will copy data into and out of this
# path.
DATA_DIR = os.path.abspath("data")


@task()
def task_on_default(x: int) -> list:
    # Since we don't specify an `executor` task option, this task will run on
    # the 'default' executor, i.e. a local thread.

    # In this example, we run the command `uname -a` to get the OS and hostname
    # information in order to provde the task is running locally. For example, if
    # you are running this code on MacOSX, you would see the OS name 'Darwin'.
    return ["task_on_default", subprocess.check_output(["uname", "-a"]), x]


@task(executor="docker", memory=0.5, vcpus=1, volumes=[(DATA_DIR, DATA_DIR)])
def task_on_docker(x: int) -> list:
    # As you can see in the task options, this task will run on the 'docker' executor.
    # We have configured .5Gb of memory and 1 vCPU.

    # We also volume mount using `volumes` the DATA_DIR to the same (host, container) paths.

    # Again, we use the command `uname -a` to prove we are running the container.
    # Most likely you will see the OS name 'Linux'.

    # One other thing to point out in this example, we are free to call other tasks that
    # don't run in docker, such as `task_on_default`.

    return ["task_on_docker", subprocess.check_output(["uname", "-a"]), task_on_default(x + 5), x]


@task()
def count_colors_by_script(data: File, output_path: str) -> Dict[str, File]:
    """
    Count colors using a multi-line script.
    """
    # Here, we use the same script as in 04_script, but now we do File staging
    # to and from a scratch directory.
    output = File(os.path.join(output_path, "color-counts.tsv"))
    log_file = File(os.path.join(output_path, "color-counts.log"))

    return script(
        """
        echo 'sorting colors...' >> log.txt
        cut -f3 data | sort > colors.sorted

        echo 'counting colors...' >> log.txt
        uniq -c colors.sorted | sort -nr > color-counts.txt
        """,
        executor="docker",
        volumes=[(DATA_DIR, DATA_DIR)],
        inputs=[data.stage("data")],
        outputs={
            "colors-counts": output.stage("color-counts.txt"),
            "log": log_file.stage("log.txt"),
        },
    )


@task()
def main(output_path: str = DATA_DIR, y: int = 10) -> list:
    # This is the top-level task of the workflow. Here, we are invoking the
    # different examples of running tasks on different executors. All of their
    # results will be combined into one nested list as shown below.

    output_path = os.path.abspath(output_path)

    # Prepare input data in our scratch dir which will be volume mounted by the
    # docker containers.
    data = File("data.tsv")
    s3_data = data.copy_to(File(f"{output_path}/data.tsv"))

    return [
        "main",
        subprocess.check_output(["uname", "-a"]),
        task_on_docker(y),
        count_colors_by_script(s3_data, output_path),
    ]
