from redun import task
from redun.scheduler import subrun

redun_namespace = "redun.examples.subrun"


@task(executor="process")
def process_record(record):
    return record + 2


@task(version="2")
def root_on_batch(records):
    return [process_record(record) for record in records]


@task()
def main():
    records = list(range(10))
    return subrun(
        root_on_batch(records),
        executor="batch",
        # Not providing a config uses the local scheduler config (replacing config_dir with ".")
        vcpus=2,
        memory=4,
        cache=False,
    )
