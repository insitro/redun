import os

from redun import File, script, task

redun_namespace = "redun.examples.aws_batch_array_jobs_script"


@task
def my_script(i: int, output_path: str) -> File:
    return script(
        f"echo {i} > output",
        executor="batch",
        outputs=File(output_path).stage("output"),
    )


@task
def main(output_prefix: str, n: int = 10) -> list[int]:
    # The add and mults in the list comprehension below will automatically run
    # in parallel. Further, the AWSBatchExecutor will automatically group
    # jobs together into a Array Jobs for faster submission. This design
    # allows submitting 1000s to 10,000s of jobs within seconds.
    return [my_script(i, os.path.join(output_prefix, str(i))) for i in range(n)]
