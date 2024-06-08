# Context example

Sometimes users have parameters or configuration that they want to pass to a task that is deeply nested inside a workflow. Typically, this might require adding the parameter to every task along the path from the top-level task (e.g. `main`) to the intended task. This has two negative consequences: (1) a lot of code change and bloat is added to all the intermediate tasks, (2) when the parameter is changed between executions, we may unnecessarily rerun intermediate that otherwise do not care about the parameter.

To address this need, redun provides a feature called **Context** that allows propagating values directly to deep parts of a workflow, while still providing correct behavior for workflow caching and reactivity (e.g. rerunning tasks when arguments change). You can think of this like passing values through an "underground pipeline" that skips pass all the intermediate tasks and resurfaces in a low-level task. This is inspired by [React’s Context feature](https://reactjs.org/docs/context.html) as well as the functional programming concept called the [Reader Monad](https://hackage.haskell.org/package/mtl-2.2.2/docs/Control-Monad-Reader.html).

See the docs for more information about [Context](https://insitro.github.io/redun/context.html).

## Running the workflow

To run these examples, you can use the usual commands:


```sh
redun run workflow.py main
```

Which should produce the following output:

```
[redun] redun :: version 0.20.0
[redun] config dir: /Users/rasmus/projects/redun-dev/examples/context/.redun
[redun] Start Execution 779b18e8-4746-48ce-a878-35e52fd2a3f3:  redun run --no-cache workflow.py main
[redun] Run    Job 3c46b3fe:  redun.examples.context.main(size=10) on default
[redun] Run    Job d6a6c15a:  redun.examples.context.process_values(values=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]) on default
[redun] Run    Job 1414e5d3:  redun.examples.context.my_tool(value=0, flag=True, param=None) on process
[redun] Run    Job a07e67ea:  redun.examples.context.my_tool(value=1, flag=True, param=None) on process
[redun] Run    Job 2162a278:  redun.examples.context.my_tool(value=2, flag=True, param=None) on process
[redun] Run    Job a4ce81d0:  redun.examples.context.my_tool(value=3, flag=True, param=None) on process
[redun] Run    Job fef839bc:  redun.examples.context.my_tool(value=4, flag=True, param=None) on process
[redun] Run    Job 1596c170:  redun.examples.context.my_tool(value=5, flag=True, param=None) on process
[redun] Run    Job cc9b0a4e:  redun.examples.context.my_tool(value=6, flag=True, param=None) on process
[redun] Run    Job ed4406e4:  redun.examples.context.my_tool(value=7, flag=True, param=None) on process
[redun] Run    Job 45504f7c:  redun.examples.context.my_tool(value=8, flag=True, param=None) on process
[redun] Run    Job cb1f6644:  redun.examples.context.my_tool(value=9, flag=True, param=None) on process
[redun]
[redun] | JOB STATUS 2024/06/07 06:52:21
[redun] | TASK                                  PENDING RUNNING  FAILED  CACHED    DONE   TOTAL
[redun] |
[redun] | ALL                                         0       0       0       0      12      12
[redun] | redun.examples.context.main                 0       0       0       0       1       1
[redun] | redun.examples.context.my_tool              0       0       0       0      10      10
[redun] | redun.examples.context.process_values       0       0       0       0       1       1
[redun]
[redun]
[redun] Execution duration: 8.48 seconds
{'values': ['Ran my_tool with flag: True and param: None',
            'Ran my_tool with flag: True and param: None',
            'Ran my_tool with flag: True and param: None',
            'Ran my_tool with flag: True and param: None',
            'Ran my_tool with flag: True and param: None',
            'Ran my_tool with flag: True and param: None',
            'Ran my_tool with flag: True and param: None',
            'Ran my_tool with flag: True and param: None',
            'Ran my_tool with flag: True and param: None',
            'Ran my_tool with flag: True and param: None']}
```

Notice how `my_tool` received the value for `flag` and ran on the executor `process`.

Also see the second example using:

```sh
redun run workflow2.py main
```

Lastly, you can override context from the command line using:

```sh
redun run --context my_tool.param=10 workflow.py main
```

Which should produce output like:

```
[redun] redun :: version 0.20.0
[redun] config dir: /Users/rasmus/projects/redun-dev/examples/context/.redun
[redun] Start Execution 58397c0d-e843-4356-8f58-3138f0ea97e2:  redun run --context my_tool.param=10 workflow.py main
[redun] Cached Job 82123597:  redun.examples.context.main(size=10) (eval_hash=6c56e9e8, call_hash=None)
[redun] Cached Job da70f13c:  redun.examples.context.process_values(values=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]) (eval_hash=2d51dfa5, call_hash=None)
[redun] Run    Job 7bc65e0e:  redun.examples.context.my_tool(value=0, flag=True, param=10) on process
[redun] Run    Job ed46bd16:  redun.examples.context.my_tool(value=1, flag=True, param=10) on process
[redun] Run    Job b93df8b9:  redun.examples.context.my_tool(value=2, flag=True, param=10) on process
[redun] Run    Job afc23e2e:  redun.examples.context.my_tool(value=3, flag=True, param=10) on process
[redun] Run    Job 1c19b9c9:  redun.examples.context.my_tool(value=4, flag=True, param=10) on process
[redun] Run    Job 122153ce:  redun.examples.context.my_tool(value=5, flag=True, param=10) on process
[redun] Run    Job 956727a1:  redun.examples.context.my_tool(value=6, flag=True, param=10) on process
[redun] Run    Job 48e5aaf2:  redun.examples.context.my_tool(value=7, flag=True, param=10) on process
[redun] Run    Job 6bb8f93b:  redun.examples.context.my_tool(value=8, flag=True, param=10) on process
[redun] Run    Job 8e65c476:  redun.examples.context.my_tool(value=9, flag=True, param=10) on process
[redun]
[redun] | JOB STATUS 2024/06/07 06:50:16
[redun] | TASK                                  PENDING RUNNING  FAILED  CACHED    DONE   TOTAL
[redun] |
[redun] | ALL                                         0       0       0       2      10      12
[redun] | redun.examples.context.main                 0       0       0       1       0       1
[redun] | redun.examples.context.my_tool              0       0       0       0      10      10
[redun] | redun.examples.context.process_values       0       0       0       1       0       1
[redun]
[redun]
[redun] Execution duration: 9.10 seconds
{'values': ['Ran my_tool with flag: True and param: 10',
            'Ran my_tool with flag: True and param: 10',
            'Ran my_tool with flag: True and param: 10',
            'Ran my_tool with flag: True and param: 10',
            'Ran my_tool with flag: True and param: 10',
            'Ran my_tool with flag: True and param: 10',
            'Ran my_tool with flag: True and param: 10',
            'Ran my_tool with flag: True and param: 10',
            'Ran my_tool with flag: True and param: 10',
            'Ran my_tool with flag: True and param: 10']}
```
