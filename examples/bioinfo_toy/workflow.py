"""
Toy example of sequential and parallel tasks for a mock bioinformatics pipeline.

To run the single sample workflow use:

  redun run workflow.py run_sample

To run the multiple sample workflow use:

  redun run workflow.py run_samples

To process more samples use:

  redun run workflow.py run_samples --n 10

After running the above commands, try 'upgrading' the calling algorithm to do

  @task()
  def run_gatk(align: str) -> str:
      calls = "calls2({})".format(align)
      return calls

Running the workflow again will only rerun the `run_gatk()` tasks and will cache
the `run_bwa()` tasks.
"""

from typing import List

from redun import task

redun_namespace = "redun.examples.bioinfo"


@task()
def run_bwa(reads: str) -> str:
    align = "align({})".format(reads)
    return align


@task()
def run_gatk(align: str) -> str:
    calls = "calls({})".format(align)
    return calls


@task()
def run_sample(reads: str = "read-data") -> str:
    align = run_bwa(reads)
    calls = run_gatk(align)
    return calls


@task()
def run_samples(n: int = 3) -> List[str]:
    reads_set = [f"read-data-{i}" for i in range(n)]

    calls_set = []
    for reads in reads_set:
        align = run_bwa(reads)
        calls = run_gatk(align)
        calls_set.append(calls)
    return calls_set
