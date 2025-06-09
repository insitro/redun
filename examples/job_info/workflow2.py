from redun import File, task
from redun.scheduler import JobInfo


redun_namespace = "redun.examples.job_info2"


@task
def step1(x: int, y: int, job_info: JobInfo = JobInfo()) -> File:
    z = x + y
    file = File(f"data/{job_info.eval_hash}/output")
    file.write(str(z))
    return file


@task
def step2(w: int, file: File) -> int:
    z = int(file.read())
    return z * w


@task
def main(x: int = 1, y: int = 2, w: int = 3) -> int:
    file = step1(x, y)
    output = step2(w, file)
    return output
