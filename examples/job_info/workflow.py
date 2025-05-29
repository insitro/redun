from redun import task
from redun.scheduler import JobInfo

redun_namespace = "redun.examples.job_info"


@task
def add(a: int, b: int, job_info: JobInfo = JobInfo()) -> int:
    print("add job_info", job_info.execution_id, job_info.job_id, job_info.eval_hash)
    return a + b


@task
def main(job_info: JobInfo = JobInfo()) -> int:
    print("main job_info", job_info.execution_id, job_info.job_id, job_info.eval_hash)
    return add(1, 3)
