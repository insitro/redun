# First redun workflow: Hello, World!

redun workflows look very similar to typical Python programs, and yet they are able to support features such as automatic caching, parallelism, remote execution, incremental execution, and data provenance. To get familiar with these features, let's take a look at our first example workflow [hello_world.py](hello_world.py):

```py
# hello_world.py
from redun import task

redun_namespace = "redun.examples.hello_world"

@task()
def get_planet() -> str:
    return "World"

@task()
def greeter(greet: str, thing: str) -> str:
    return "{}, {}!".format(greet, thing)

@task()
def main(greet: str="Hello") -> str:
    return greeter(greet, get_planet())
```

We can execute this workflow using the `redun` command-line program (see [INSTALL](../../README.md) for installing redun if not installed already):

```sh 
redun run hello_world.py main
```

This command runs the `main` task in the script `hello_world.py`, which should produce output something like:

```
[redun] redun :: version 0.4.15
[redun] config dir: /Users/rasmus/projects/redun/examples/hello_world/.redun
[redun] Start Execution d1419df0-f954-4a33-8f5b-ff401856c137:  redun run hello_world.py main
[redun] Run    Job c5a729dc:  redun.examples.hello_world.main(greet='Hello') on default
[redun] Run    Job b1b9e4ae:  redun.examples.hello_world.get_planet() on default
[redun] Run    Job 0e6c132a:  redun.examples.hello_world.greeter(greet='Hello', thing='World') on default
[redun]
[redun] | JOB STATUS 2021/06/18 07:23:15
[redun] | TASK                                  PENDING RUNNING  FAILED  CACHED    DONE   TOTAL
[redun] |
[redun] | ALL                                         0       0       0       0       3       3
[redun] | redun.examples.hello_world.get_planet       0       0       0       0       1       1
[redun] | redun.examples.hello_world.greeter          0       0       0       0       1       1
[redun] | redun.examples.hello_world.main             0       0       0       0       1       1
[redun]
'Hello, World!'
```

Here, we see that the redun scheduler ran three Jobs for our tasks `main`, `get_planet`, and  `greeter` with arguments such as `greet='Hello'` and `thing='World'`. The final output of the workflow is written to standard output, `'Hello, World!'`. In later examples, we will see workflows that create additional outputs, such as files. You may also notice that the task names have an additional prefix, `redun.examples.hello_world`. This is called a *task namespace*, and can be used to distinguish tasks executed in different workflows, which is useful in situations where logs are aggregated across workflows (we'll discuss this more later).

Since this is a toy example, it may not be immediately clear that each task actually ran in its own thread. In later examples, we will see how redun uses threads, processes, remote batch jobs, etc to perform automatic parallelism and distributed compute. However before we get to that, we'll first look at caching and argument parsing.

## Caching and argument parsing

The first features we will demonstrate will be caching and automatic command line argument parsing. To see caching in action, let's try running the same workflow again:

```
redun run hello_world.py main

[redun] redun :: version 0.4.15
[redun] config dir: /Users/rasmus/projects/redun/examples/hello_world/.redun
[redun] Start Execution 67fa7eb7-c31d-485e-a6c2-4196367a23c1:  redun run hello_world.py main
[redun] Cached Job fe387985:  redun.examples.hello_world.main(greet='Hello') (eval_hash=047bd6b3)
[redun] Cached Job 836e44d9:  redun.examples.hello_world.get_planet() (eval_hash=bd8f8a95)
[redun] Cached Job 5ede15ae:  redun.examples.hello_world.greeter(greet='Hello', thing='World') (eval_hash=14704bdc)
[redun]
[redun] | JOB STATUS 2021/06/18 07:24:08
[redun] | TASK                                  PENDING RUNNING  FAILED  CACHED    DONE   TOTAL
[redun] |
[redun] | ALL                                         0       0       0       3       0       3
[redun] | redun.examples.hello_world.get_planet       0       0       0       1       0       1
[redun] | redun.examples.hello_world.greeter          0       0       0       1       0       1
[redun] | redun.examples.hello_world.main             0       0       0       1       0       1
[redun]
'Hello, World!'
```

As expected, we get the same output `'Hello, World!'`, but we also see that all three tasks were `Cached`. When running a workflow, the redun scheduler by default creates a local sqlite database, `.redun/redun.db`, where it stores information about previous executions, including a cache of all task return values. When calling the same task with the same arguments as a past execution, the redun scheduler can skip rerunning a task and instead replay its return value from the cache. This allows redun to quickly resume an execution that may have been halted or altered half-way through.

Let's use the `help` option to get more help information about the `main` task:

```
redun run hello_world.py main help

[redun] redun :: version 0.4.15
[redun] config dir: /Users/rasmus/projects/redun/examples/hello_world/.redun
usage: redun.examples.hello_world.main [-h] [--greet GREET] {help,info} ...

positional arguments:
  {help,info}
    help         Show help for calling a task.
    info         Show task information.

optional arguments:
  -h, --help     show this help message and exit
  --greet GREET  (default: Hello)
```

Using the `help` command, we can see that the arguments of the `main()` function have been used to automatically create command line options, such as `--greet`. Also, the default value, `Hello`, has become the default option value. The redun command-line will also use the type annotations to perform automatic argument parsing (e.g. parsing `int` arguments). Let's try calling the workflow again, but with a different greeting:

```
redun run hello_world.py main --greet Hi

[redun] redun :: version 0.4.15
[redun] config dir: /Users/rasmus/projects/redun/examples/hello_world/.redun
[redun] Start Execution c734fdf9-e511-4598-b9f5-e64f7786afb4:  redun run hello_world.py main --greet Hi
[redun] Run    Job 9fecc9f7:  redun.examples.hello_world.main(greet='Hi') on default
[redun] Cached Job b4d9ca5c:  redun.examples.hello_world.get_planet() (eval_hash=bd8f8a95)
[redun] Run    Job 5615b803:  redun.examples.hello_world.greeter(greet='Hi', thing='World') on default
[redun]
[redun] | JOB STATUS 2021/06/18 07:30:52
[redun] | TASK                                  PENDING RUNNING  FAILED  CACHED    DONE   TOTAL
[redun] |
[redun] | ALL                                         0       0       0       1       2       3
[redun] | redun.examples.hello_world.get_planet       0       0       0       1       0       1
[redun] | redun.examples.hello_world.greeter          0       0       0       0       1       1
[redun] | redun.examples.hello_world.main             0       0       0       0       1       1
[redun]
'Hi, World!'
```

Now, we get a different output, `'Hi, World!'`, as we expected. However, look closely at the task executions. We see that only two of the tasks ran, `main` and `greeter`, but `get_planet` was still cached. redun hashes task arguments in order to determine whether they are the same arguments as used in past executions. Since `main` and `greeter` have new arguments never seen before, they must rerun in order to see what they would return, and since `get_planet` has the same arguments (i.e. no arguments) it does not need to be run again.

Let's use the `help` command without any task given before it to see what other tasks are available.

```
redun run hello_world.py help

[redun] redun :: version 0.4.15
[redun] config dir: /Users/rasmus/projects/redun/examples/hello_world/.redun
Tasks available:
   redun.examples.hello_world.get_planet
   redun.examples.hello_world.greeter
   redun.examples.hello_world.main
```

In redun, we can run any task as the top-level task, there is nothing special about `main`. For example, we can do the following to test the `greeter` task specifically (note: including the namespace is optional for the `run` command):

```
redun run hello_world.py greeter --greet Yo --thing Venus

[redun] redun :: version 0.4.15
[redun] config dir: /Users/rasmus/projects/redun/examples/hello_world/.redun
[redun] Start Execution 22a447a2-c7a6-4915-a4ef-955a400c8282:  redun run hello_world.py greeter --greet Yo --thing Venus
[redun] Run    Job 0ee0cc71:  redun.examples.hello_world.greeter(greet='Yo', thing='Venus') on default
[redun]
[redun] | JOB STATUS 2021/06/18 09:25:13
[redun] | TASK                               PENDING RUNNING  FAILED  CACHED    DONE   TOTAL
[redun] |
[redun] | ALL                                      0       0       0       0       1       1
[redun] | redun.examples.hello_world.greeter       0       0       0       0       1       1
[redun]
'Yo, Venus!'
```

Running lower-level tasks can be a convenient way to test subsets of a workflow during development.

## Code reactivity

The redun workflow not only reacts to changes in arguments, but also changes in code. Using a text editor, let us alter the `get_planet` task to return something new. Make the following change to the `get_planet` task in [hello_world.py](hello_world.py):

```py
@task()
def get_planet() -> str:
    return 'Mars'
```

Let us now rerun the workflow.

```
redun run hello_world.py main

[redun] redun :: version 0.4.15
[redun] config dir: /Users/rasmus/projects/redun/examples/hello_world/.redun
[redun] Start Execution 4b20320e-0c87-483c-afbe-44136cd09337:  redun run hello_world.py main
[redun] Cached Job 91278c9a:  redun.examples.hello_world.main(greet='Hello') (eval_hash=047bd6b3)
[redun] Run    Job 982f7074:  redun.examples.hello_world.get_planet() on default
[redun] Run    Job 8b1ff5c3:  redun.examples.hello_world.greeter(greet='Hello', thing='Mars') on default
[redun]
[redun] | JOB STATUS 2021/06/18 07:40:51
[redun] | TASK                                  PENDING RUNNING  FAILED  CACHED    DONE   TOTAL
[redun] |
[redun] | ALL                                         0       0       0       1       2       3
[redun] | redun.examples.hello_world.get_planet       0       0       0       0       1       1
[redun] | redun.examples.hello_world.greeter          0       0       0       0       1       1
[redun] | redun.examples.hello_world.main             0       0       0       1       0       1
[redun]
'Hello, Mars!'
```

We get the expected output, `'Hello, Mars!'`, and we see that the `get_planet` task has run. In addition to hashing task arguments, the redun scheduler also hashes the source code of each task and compares that hash with past executions. In fact, it is the hash of the task source code and its arguments that are used as the "cache key". Notice also that `greeter` re-ran as well. Although, we did not change the source of `greeter`, it must rerun because it is receiving a new argument from its upstream task `get_planet`. This is an example of the redun scheduler helping us to automatically determine which subsets of the workflow to run, typically referred to as [incremental computation](https://en.wikipedia.org/wiki/Incremental_computing).

This is a more subtle point, but also notice that redun correctly determined that `get_planet` needed to be rerun even though it is called from `main`. A more naive implementation of caching might have accidentally overlooked rerunning `get_planet`, if it assumed `main` is cached and therefore nothing needs to rerun. After all, `main`'s code and arguments have not changed. The full details are discussed in the [design doc](), but the short answer is that redun replays the child tasks calls, not just the final return value of `main`, in order to provide a fully correct implementation of incremental compute.

Lastly, sometimes we would like to control the versioning of tasks ourselves instead of it being driven by source code hashing. Perhaps we want to add comments or refactor our code, without being forced to rerun it. redun allows this as well using manual [Task versioning]().

## Workflow failures

Let's make one more change to our workflow. Using your text editor, introduce the following bug in the `get_planet` task:

```py
@task()
def get_planet():
    x = 1 / 0
    return 'Mars'
```

When we run this workflow, we will see the `get_planet` and `main` tasks will fail.

```
redun run hello_world.py main

[redun] redun :: version 0.4.15
[redun] config dir: /Users/rasmus/projects/redun/examples/hello_world/.redun
[redun] Start Execution 686ff4f0-38e9-4dd4-8155-35a4e5fac008:  redun run hello_world.py main
[redun] Cached Job bb320628:  redun.examples.hello_world.main(greet='Hello') (eval_hash=047bd6b3)
[redun] Run    Job 5b632c7e:  redun.examples.hello_world.get_planet() on default
[redun] *** Reject Job 5b632c7e:  redun.examples.hello_world.get_planet():
[redun] *** Reject Job bb320628:  redun.examples.hello_world.main(greet='Hello'):
[redun]
[redun] | JOB STATUS 2021/06/18 07:44:20
[redun] | TASK                                  PENDING RUNNING  FAILED  CACHED    DONE   TOTAL
[redun] |
[redun] | ALL                                         1       0       2       0       0       3
[redun] | redun.examples.hello_world.get_planet       0       0       1       0       0       1
[redun] | redun.examples.hello_world.greeter          1       0       0       0       0       1
[redun] | redun.examples.hello_world.main             0       0       1       0       0       1
[redun]
[redun] *** Execution failed. Traceback (most recent task last):
[redun]   Job bb320628: File "/Users/rasmus/projects/redun/examples/hello_world/hello_world.py", line 19, in main
[redun]     def main(greet: str='Hello'):
[redun]     greet = 'Hello'
[redun]   Job 5b632c7e: File "/Users/rasmus/projects/redun/examples/hello_world/hello_world.py", line 8, in get_planet
[redun]     def get_planet():
[redun]   File "/Users/rasmus/projects/redun/examples/hello_world/hello_world.py", line 9, in get_planet
[redun]     x = 1 / 0
[redun] ZeroDivisionError: division by zero
```

Let's take a look at this output. First, we see that a `ZeroDivisionError` exception has been raised. We see that it occurred in `get_planet` and that this eventually lead to the whole workflow coming to a halt. We see a "traceback" (sometimes referred to as a "call stack"), but it is not the typical Python traceback. Instead of showing frames in a call stack, it is showing a stack of redun Jobs. As we previously discussed, these Jobs each run in their own thread, but more generally they could be running on different processes or machines. Regardless, redun is reconstructing a "virtual call stack" that corresponds to the logical workflow. By treating a workflow as if it was just one large program which only happens to run distributed, redun aims to provide a simplified debugging and development experience.

## Data provenance for debugging

In a large workflow, say with hundreds or thousands of Jobs, understanding where a failure is and why it occurred can be a significant challenge. Let's use redun's data provenance features to find this bug and compare it to previous executions that succeeded. This time, we use a new command called `redun log`, which works similar to `git log`. It will use the sqlite database, `.redun/redun.db`, to display a list of all past workflow executions:

```
redun log

Recent executions:

Exec 686ff4f0 [FAILED] 2021-06-18 07:44:20:  run hello_world.py main
Exec 4b20320e [ DONE ] 2021-06-18 07:40:50:  run hello_world.py main
Exec 22a447a2 [ DONE ] 2021-06-18 07:40:30:  run hello_world.py greeter --greet Yo --thing Venus
Exec c734fdf9 [ DONE ] 2021-06-18 07:30:52:  run hello_world.py main --greet Hi
Exec 67fa7eb7 [ DONE ] 2021-06-18 07:24:08:  run hello_world.py main
Exec d1419df0 [ DONE ] 2021-06-18 07:23:15:  run hello_world.py main
```

Here we see our six past executions (reverse chronological) and the command line arguments we used for each one. We can also see the timestamps and execution status (`DONE` and `FAILED`).

Similar to `git log`, we can use hashes and ids (as well as their prefixes) to get more specific information. For example, let us look at the very first workflow execution `d1419df0` (note, your Execution id will be different):

```
redun log d1419df0

Exec d1419df0-f954-4a33-8f5b-ff401856c137 [ DONE ] 2021-06-18 07:23:15:  run hello_world.py main
Duration: 0:00:00.21

Jobs: 3 (DONE: 3, CACHED: 0, FAILED: 0)
--------------------------------------------------------------------------------
Job c5a729dc [ DONE ] 2021-06-18 07:23:15:  redun.examples.hello_world.main(greet='Hello')
  Job b1b9e4ae [ DONE ] 2021-06-18 07:23:15:  redun.examples.hello_world.get_planet()
  Job 0e6c132a [ DONE ] 2021-06-18 07:23:15:  redun.examples.hello_world.greeter('Hello', 'World')
```

Let's take some time to understand this output. First, we see the full Execution id is `d1419df0-f954-4a33-8f5b-ff401856c137`, it finished successfully (denoted `DONE`), it was started at `2021-06-18 07:23:15`, and we see the original command line arguments. Second, we see that this execution had three Jobs that were executed in a job tree: `main` is the root Job that then called two child Jobs `get_planet` and `greeter`. We can see that all three jobs ran and were successful (`DONE`).

Let's compare this to the second execution we ran, `67fa7eb7`. Again, your id will differ so use the `redun log` output to locate the equivalent id.

```
redun log 67fa7eb7

Exec 67fa7eb7-c31d-485e-a6c2-4196367a23c1 [ DONE ] 2021-06-18 07:24:08:  run hello_world.py main
Duration: 0:00:00.07

Jobs: 3 (DONE: 0, CACHED: 3, FAILED: 0)
--------------------------------------------------------------------------------
Job fe387985 [CACHED] 2021-06-18 07:24:08:  redun.examples.hello_world.main(greet='Hello')
  Job 836e44d9 [CACHED] 2021-06-18 07:24:08:  redun.examples.hello_world.get_planet()
  Job 5ede15ae [CACHED] 2021-06-18 07:24:08:  redun.examples.hello_world.greeter('Hello', 'World')
```

Now we see a similar job tree, except all three Jobs are `CACHED`. This corresponds to when we reran the workflow with no changes (no new arguments or code changes).

Let's dig in and look at some data provenance related to a specific Job. Let's pick the `greeter` Job which in the example above has the id prefix `5ede15ae`:

```
redun log 5ede15ae

Job 5ede15ae-49ce-439b-9997-1ddc0dcbb9fb [CACHED] 2021-06-18 07:24:08:  redun.examples.hello_world.greeter('Hello', 'World')
Traceback: Exec 67fa7eb7 > Job fe387985 main > Job 5ede15ae greeter

  CallNode c51a681dedc0f527cbb0516947113df9d2daf633 redun.examples.hello_world.greeter
    Args:   'Hello', 'World'
    Result: 'Hello, World!'

  Task 4bf69a2969a13a81ecdf097e53f1f33ee4e2e063 redun.examples.hello_world.greeter

    def greeter(greet: str, thing: str) -> str:
        return '{}, {}!'.format(greet, thing)


  Upstream dataflow:

    result = 'Hello, World!'

    result <-- <c51a681d> greeter(greet, thing)
      greet = <d5d4dead> 'Hello'
      thing = <58dc3a20> 'World'

    greet <-- argument of <a0694d83> main(greet)
          <-- origin

    thing <-- <0070aa6a> get_planet()
          <-- origin
```

First, we see the full Job id `5ede15ae-49ce-439b-9997-1ddc0dcbb9fb`. We also see a brief traceback of the hierarchy of Jobs from the top-level execution to this Job. Second, we see what is called a `CallNode`, which represents the call of a task with a particular hash and particular arguments. We see the arguments `'Hello'` and `'World'` as well as the result `'Hello, World!'`. Third, we see the task for this Job, which is `greeter`. We see the task hash, `4bf69a2969a13a81ecdf097e53f1f33ee4e2e063`, as well as the original source code at the time of execution. Lastly, we see something call `Upstream dataflow`. This shows a trace of dataflow from the result of this job backwards through the workflow. We will see more extensive dataflows in later examples and discuss how to interpret them. For now, we can briefly see the values of `greet` and `thing` and we can see those values came from upstream tasks `main` and `get_planet`, respectively.

Now, we're ready to investigate our bug. Let's look at the execution list again:

```
redun log

Recent executions:

Exec 686ff4f0 [FAILED] 2021-06-18 07:44:20:  run hello_world.py main
Exec 4b20320e [ DONE ] 2021-06-18 07:40:50:  run hello_world.py main
Exec 22a447a2 [ DONE ] 2021-06-18 07:40:30:  run hello_world.py greeter --greet Yo --thing Venus
Exec c734fdf9 [ DONE ] 2021-06-18 07:30:52:  run hello_world.py main --greet Hi
Exec 67fa7eb7 [ DONE ] 2021-06-18 07:24:08:  run hello_world.py main
Exec d1419df0 [ DONE ] 2021-06-18 07:23:15:  run hello_world.py main
```

Let's look at the failing execution, `686ff4f0`. Note it is common to look at the most recent execution, which can be done with the abbreviation `redun log -`. Let's try that out here:

```
redun log -

Exec 686ff4f0-38e9-4dd4-8155-35a4e5fac008 [FAILED] 2021-06-18 07:44:20:  run hello_world.py main
Duration: 0:00:00.00

Jobs: 2 (DONE: 0, CACHED: 0, FAILED: 2)
--------------------------------------------------------------------------------
Job bb320628 [FAILED] 2021-06-18 07:44:20:  redun.examples.hello_world.main(greet='Hello')
  Job 5b632c7e [FAILED] 2021-06-18 07:44:20:  redun.examples.hello_world.get_planet()
```

Now, we see the failing jobs. When a Job fails, it typically causes the parent Job to fail as well, similar to a raised exception unwinding a call stack. In later examples, we'll see powerful ways of performing exception handling in workflows. For now, we can conclude that the root cause of our bug is likely the lowest-level Job `5b632c7e`, which can inspect directly:

```
redun log 5b632c7e

Job 5b632c7e-1024-4903-8364-c5cea1315f48 [FAILED] 2021-06-18 07:44:20:  redun.examples.hello_world.get_planet()
Traceback: Exec 686ff4f0 > Job bb320628 main > Job 5b632c7e get_planet

  CallNode cab04cb81708d20befff6ef69e097c462772d276 redun.examples.hello_world.get_planet
    Args:
    Raised: ZeroDivisionError('division by zero')

  Task 23c8f856f23d1cc77ad384716469f06b4748229a redun.examples.hello_world.get_planet

    def get_planet() -> str:
        x = 1 / 0
        return 'Mars'


  Upstream dataflow:

    result = ErrorValue(ZeroDivisionError('division by zero'))

    result <-- <cab04cb8> get_planet()
           <-- origin

  Traceback:
    File "/Users/rasmus/projects/redun/examples/hello_world/hello_world.py", line 9, in get_planet
      x = 1 / 0
  ZeroDivisionError: division by zero
```

Here again, we see the data provenance of a single Job. This time, we can see it failed and raised `ZeroDivisionError('division by zero')`. We also see a recording of the Python traceback which contains useful file and line number information. Looking at the task source, we see the problematic line:

```py
x = 1 / 0
```

Using this information, we could go back to our source code to fix our workflow (i.e. remove the divide by zero line).


## Conclusion

Although this particular workflow is very simplistic, we were able to review several important features of redun. We saw that redun can automatically perform caching on a per task basis. We saw the ability to get automatic argument parsing for any task in our workflow, which allows us to avoid writing tedious argument parsers ourselves. Lastly, we saw how the data provenance is recorded for every execution and that the `redun log` command can be used to quickly navigate past executions.

This only the beginning of what redun provides. In the later examples, we see how redun can provide reactivity and provenance for files, provide automatic parallelism, and schedule work on remote compute environments such as batch clusters.


## Bonus round

It may have been strange to describe data provenance as a way to help with debugging. Typically, data provenance is thought of as a way to provide long term documentation of how data was produced (e.g. what code, inputs, and parameters were used). This has many benefits such as scientific reproducibility as well as data sharing and discovery. Even though many people recognize these benefits, it is also typical that getting adoption of data provenance is a struggle for software teams. Why?

One explanation may be that data provenance requires overhead to the workflow developer, perhaps in the form of extra logging statements or tooling. Hopefully, automatic recording, as done by redun, could help with keeping that overhead low. But also data provenance typically isn't useful to the original developer. Usually it's some future person or team that will benefit, and that might be too abstract of a benefit when deadlines are looming. What if we made data provenance useful in the short term to the actual developer writing the workflow?

There is a related concept called *observability* that promotes detailed logging of system state to help with debugging. It is especially useful in highly distributed systems such as modern web services. In some sense, observability is just short term data provenance. If we recognize them as the same thing (just different extremes along time scales), we should be able to use data provenance to provide the same debugging benefits to developers as observability provides, and thus drive the adoption for the long term provenance we desire. Hopefully, this toy example gives you an idea of what could be possible.
