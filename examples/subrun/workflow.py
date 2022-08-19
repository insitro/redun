import os
import subprocess
import time
from typing import List

from redun import task
from redun.scheduler import subrun

redun_namespace = "redun.examples.subrun"


@task(executor="process")
def process_record(record) -> dict:
    """
    Report our machine (uname) and process id (pid).

    This task will be run across multiple processes per batch container.
    """
    time.sleep(10)
    return {
        "name": "process_record",
        "uname": subprocess.check_output(['uname', '-a']),
        "pid": os.getpid(),
        "value": record + 2,
    }


@task
def process_group(record_group: List[int]) -> dict:
    """
    This task runs in batch within a subscheduler due to the subrun() call below.
    """
    values = [process_record(record) for record in record_group]
    return {
        "name": "root_on_batch",
        "uname": subprocess.check_output(['uname', '-a']),
        "pid": os.getpid(),
        "value": values
    }


@task
def main() -> dict:
    """
    This task run locally and launches several batch containers using subrun().
    """
    record_groups = [
        list(range(10)),
        list(range(10, 20)),
    ]
    results = []

    for record_group in record_groups:
        results.append(subrun(
            process_group(record_group),

            # Run the subscheduler on a the 'batch' executor. We can also
            # specify executor options here.
            executor="batch",
            vcpus=4,
            memory=4,

            # Start a new Execution for subscheduler.
            new_execution=True,

            # Use the following config for the subscheduler.
            # Here, db_uri is a sqlite file within the container (which will be
            # discarded). If you want to retain the cache of the subscheduler
            # use a db_uri for a central db such as postgres in RDS.
            config={
                "backend": {
                    "db_uri": "sqlite:///redun.db",
                },
            },
        ))

    return {
        "name": "main",
        "uname": subprocess.check_output(['uname', '-a']),
        "results": results,
    }
