# Suppressing provenance recording

Sometimes, you may want to use redun for its parallelism and distributed compute, but recording the provenance for certain substeps of a workflow might be unnecessary. It might even be too expensive to record provenance for substeps that has very fine grain parallelism.

In such cases, you can use the task option `prov=False`, which will turn off provenance recording for the subtree of task calls. To run this example, use the following command:

```
redun run workflow.py main
```

You should see output similar to this:

```
[redun] redun :: version 0.7.3
[redun] config dir: /Users/rasmus/projects/redun/examples/no_prov/.redun
[redun] Start Execution d861cd0b-3de6-408e-a564-fa4253a6e47b:  redun run workflow.py main
[redun] Run    Job 5b532ca4:  redun.examples.no_prov.main() on default
[redun] Run    Job ff4f4f46:  redun.examples.no_prov.add4(a=1, b=2, c=3, d=4) on default
[redun] Run    Job 6d0949a6:  redun.examples.no_prov.add4_no_prov(a=5, b=6, c=7, d=8) on default
[redun] Run    Job b6d20d15:  redun.examples.no_prov.add4(a=5, b=6, c=7, d=8) on default
[redun] Run    Job 318c5c00:  redun.no_prov(expr=QuotedExpression(TaskExpression('redun.examples.no_prov.add4', (9, 10, 11, 12), {}))) on default
[redun] Run    Job dd38c07c:  redun.no_prov(expr=QuotedExpression(SimpleExpression('add', (SimpleExpression('add', (TaskExpression('redun.examples.no_prov.add', (13, 14), {}), 15), {}), 16), {}))) on default
[redun] Run    Job 76a50e71:  redun.examples.no_prov.add(a=1, b=2) on default
[redun] Run    Job 7347247c:  redun.examples.no_prov.add(a=3, b=4) on default
[redun] Run    Job b599756f:  redun.examples.no_prov.add(a=5, b=6) on default
[redun] Run    Job ef8ce6a8:  redun.examples.no_prov.add(a=7, b=8) on default
[redun] Run    Job 08443e70:  redun.examples.no_prov.add4(a=9, b=10, c=11, d=12) on default
[redun] Run    Job a3243c17:  redun.examples.no_prov.add(a=13, b=14) on default
[redun] Run    Job 5fb89dbf:  redun.examples.no_prov.add(a=9, b=10) on default
[redun] Run    Job a7175e7b:  redun.examples.no_prov.add(a=11, b=12) on default
[redun] Run    Job ee35ce17:  redun.examples.no_prov.add(a=3, b=7) on default
[redun] Run    Job 16f39bcf:  redun.examples.no_prov.add(a=11, b=15) on default
[redun] Run    Job e3da6754:  redun.examples.no_prov.add(a=1, b=58) on default
[redun] Run    Job 7eafdd5e:  redun.examples.no_prov.add(a=19, b=23) on default
[redun] Run    Job 52eddd2a:  redun.examples.no_prov.add(a=1, b=10) on default
[redun] Run    Job 02593acf:  redun.examples.no_prov.add(a=1, b=26) on default
[redun] Run    Job 71dd4144:  redun.examples.no_prov.add(a=1, b=26) on default
[redun] Run    Job 19a14330:  redun.examples.no_prov.add(a=1, b=42) on default
[redun]
[redun] | JOB STATUS 2021/09/29 09:52:07
[redun] | TASK                                PENDING RUNNING  FAILED  CACHED    DONE   TOTAL
[redun] |
[redun] | ALL                                       0       0       0       0      22      22
[redun] | redun.examples.no_prov.add                0       0       0       0      15      15
[redun] | redun.examples.no_prov.add4               0       0       0       0       3       3
[redun] | redun.examples.no_prov.add4_no_prov       0       0       0       0       1       1
[redun] | redun.examples.no_prov.main               0       0       0       0       1       1
[redun] | redun.no_prov                             0       0       0       0       2       2
[redun]
[redun] Execution duration: 0.71 seconds
{'no_options_expr': 59,
 'no_prov': 43,
 'no_prov_options': 27,
 'no_prov_task': 27,
 'prov': 11}
```

Next, we can verify whether provenance was suppressed for the most recent execution (denoted `-`) by using:

```
redun log -
```

which should show something like:

```
Exec d861cd0b-3de6-408e-a564-fa4253a6e47b [ DONE ] 2021-09-29 09:52:06:  run workflow.py main (git_commit=14fc18c9702d6a8c79ff02115ca40000f3e73c48, git_origin_url=git@github.com:insitro/redun.git, project=redun.examples.no_prov, redun.version=0.7.3, user
Duration: 0:00:00.69

Jobs: 14 (DONE: 14, CACHED: 0, FAILED: 0)
--------------------------------------------------------------------------------
Job 5b532ca4 [ DONE ] 2021-09-29 09:52:06:  redun.examples.no_prov.main()
  Job ff4f4f46 [ DONE ] 2021-09-29 09:52:06:  redun.examples.no_prov.add4(1, 2, 3, 4)
    Job 76a50e71 [ DONE ] 2021-09-29 09:52:07:  redun.examples.no_prov.add(1, 2)
    Job 7347247c [ DONE ] 2021-09-29 09:52:07:  redun.examples.no_prov.add(3, 4)
    Job ee35ce17 [ DONE ] 2021-09-29 09:52:07:  redun.examples.no_prov.add(3, 7)
  Job 6d0949a6 [ DONE ] 2021-09-29 09:52:06:  redun.examples.no_prov.add4_no_prov(5, 6, 7, 8)
  Job b6d20d15 [ DONE ] 2021-09-29 09:52:06:  redun.examples.no_prov.add4(5, 6, 7, 8)
  Job 318c5c00 [ DONE ] 2021-09-29 09:52:06:  redun.no_prov(QuotedExpression(TaskExpression('redun.examples.no_prov.add4', (9, 10, 11, 12), {})))
  Job dd38c07c [ DONE ] 2021-09-29 09:52:06:  redun.no_prov(QuotedExpression(SimpleExpression('add', (SimpleExpression('add', (TaskExpression('redun.examples.no_prov.add', (13, 14), {}), 15), {}), 16), {})))
  Job e3da6754 [ DONE ] 2021-09-29 09:52:07:  redun.examples.no_prov.add(1, 58)
  Job 52eddd2a [ DONE ] 2021-09-29 09:52:07:  redun.examples.no_prov.add(1, 10)
  Job 02593acf [ DONE ] 2021-09-29 09:52:07:  redun.examples.no_prov.add(1, 26)
  Job 71dd4144 [ DONE ] 2021-09-29 09:52:07:  redun.examples.no_prov.add(1, 26)
  Job 19a14330 [ DONE ] 2021-09-29 09:52:07:  redun.examples.no_prov.add(1, 42)
```

Notice, we only see the nested `add()` calls in the first call to `add4`. All the follow up calls have the provenance suppressed.
