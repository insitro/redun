# Debugging

redun plays well with [pdb](https://docs.python.org/3/library/pdb.html) for setting breakpoints and stepping through workflows.

```sh
redun run workflow.py main
```

You also automatically enter postmortem debugging when an exception is raised by using the `--pdb` option:

```sh
redun run --pdb workflow.py will_boom
```

You should see something like:

```
[redun] redun :: version 0.7.3
[redun] config dir: /Users/rasmus/projects/redun/examples/debugging/.redun
[redun] Tasks will require namespace soon. Either set namespace in the `@task` decorator or with the module-level variable `redun_namespace`.
tasks needing namespace: functools.py:flatten, tools.py:render_template
[redun] Start Execution a48a901b-b617-497f-869b-4db402149fed:  redun run --pdb workflow.py will_boom
[redun] Run    Job 74be3d2c:  redun.examples.debugging.will_boom(num=10, denom=0) on default
[redun] Run    Job 4137f24a:  redun.examples.debugging.boom(num=10, denom=0) on default
[redun] *** Reject Job 4137f24a:  redun.examples.debugging.boom(num=10, denom=0):
[redun] *** Reject Job 74be3d2c:  redun.examples.debugging.will_boom(num=10, denom=0):
[redun]
[redun] | JOB STATUS 2021/09/27 06:13:16
[redun] | TASK                               PENDING RUNNING  FAILED  CACHED    DONE   TOTAL
[redun] |
[redun] | ALL                                      0       0       2       0       0       2
[redun] | redun.examples.debugging.boom            0       0       1       0       0       1
[redun] | redun.examples.debugging.will_boom       0       0       1       0       0       1
[redun]
[redun] Execution duration: 0.12 seconds
[redun] *** Execution failed. Traceback (most recent task last):
[redun]   Job 74be3d2c: File "/Users/rasmus/projects/redun/examples/debugging/workflow.py", line 29, in will_boom
[redun]     def will_boom(num: float = 10, denom: float = 0) -> float:
[redun]     denom = 0
[redun]     num   = 10
[redun]   Job 4137f24a: File "/Users/rasmus/projects/redun/examples/debugging/workflow.py", line 24, in boom
[redun]     def boom(num: float, denom: float) -> float:
[redun]     denom = 0
[redun]     num   = 10
[redun]   File "/Users/rasmus/projects/redun/examples/debugging/workflow.py", line 25, in boom
[redun]     return num / denom
[redun] ZeroDivisionError: division by zero
Uncaught exception. Entering postmortem debugging.
```

You rerun a specific job using `--rerun <jobid>`. Let's rerun just the `boom()` task with the exact same arguments. Note, your job id will differ:

```
redun run --pdb workflow.py --rerun 4137f24a

[redun] redun :: version 0.7.3
[redun] config dir: /Users/rasmus/projects/redun/examples/debugging/.redun
[redun] Tasks will require namespace soon. Either set namespace in the `@task` decorator or with the module-level variable `redun_namespace`.
tasks needing namespace: functools.py:flatten, tools.py:render_template
[redun] Start Execution 7477dfc4-a714-4d46-9ed2-9c192e29933a:  redun run --pdb workflow.py --rerun 4137f24a
[redun] Run    Job 0e7c0ab6:  redun.examples.debugging.boom(num=10, denom=0) on default
[redun] *** Reject Job 0e7c0ab6:  redun.examples.debugging.boom(num=10, denom=0):
[redun]
[redun] | JOB STATUS 2021/09/27 06:15:21
[redun] | TASK                          PENDING RUNNING  FAILED  CACHED    DONE   TOTAL
[redun] |
[redun] | ALL                                 0       0       1       0       0       1
[redun] | redun.examples.debugging.boom       0       0       1       0       0       1
[redun]
[redun] Execution duration: 0.02 seconds
[redun] *** Execution failed. Traceback (most recent task last):
[redun]   Job 0e7c0ab6: File "/Users/rasmus/projects/redun/examples/debugging/workflow.py", line 24, in boom
[redun]     def boom(num: float, denom: float) -> float:
[redun]     denom = 0
[redun]     num   = 10
[redun]   File "/Users/rasmus/projects/redun/examples/debugging/workflow.py", line 25, in boom
[redun]     return num / denom
[redun] ZeroDivisionError: division by zero
Uncaught exception. Entering postmortem debugging.
```

Great, we reproduced the bug with a specific call. We can even override one of the arguments to test our ideas. Let's see if changing `denom` avoids the bug:

```
redun run --pdb workflow.py --rerun 4137f24a --denom 10
[redun] redun :: version 0.7.3
[redun] config dir: /Users/rasmus/projects/redun/examples/debugging/.redun
[redun] Tasks will require namespace soon. Either set namespace in the `@task` decorator or with the module-level variable `redun_namespace`.
tasks needing namespace: functools.py:flatten, tools.py:render_template
[redun] Start Execution 6603b21b-5e6f-4592-9537-0d4a76ec586c:  redun run --pdb workflow.py --rerun 4055eee3 --denom 10
[redun] Run    Job 4415df83:  redun.examples.debugging.boom(num=10, denom=10.0) on default
[redun]
[redun] | JOB STATUS 2021/09/27 06:17:30
[redun] | TASK                          PENDING RUNNING  FAILED  CACHED    DONE   TOTAL
[redun] |
[redun] | ALL                                 0       0       0       0       1       1
[redun] | redun.examples.debugging.boom       0       0       0       0       1       1
[redun]
[redun] Execution duration: 0.05 seconds
1.0
```

Yes, that works. Perhaps that gives us ideas about what code changes to make next.
