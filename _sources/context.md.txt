# Context

Sometimes users have parameters or configuration that they want to pass to a task that is deeply nested inside a workflow. Typically, this might require adding the parameter to every task along the path from the top-level task (e.g. `main`) to the intended task. This has two negative consequences: (1) a lot of code change and bloat is added to all the intermediate tasks, (2) when the parameter is changed between executions, we may unnecessarily rerun intermediate that otherwise do not care about the parameter.

To address this need, redun provides a feature called **Context** that allows propagating values directly to deep parts of a workflow, while still providing correct behavior for workflow caching and reactivity (e.g. rerunning tasks when arguments change). You can think of this like passing values through an "underground pipeline" that skips pass all the intermediate tasks and resurfaces in a low-level task. This is inspired by [React’s Context feature](https://react.dev/learn/passing-data-deeply-with-context) as well as the functional programming concept called the [Reader Monad](https://hackage.haskell.org/package/mtl-2.2.2/docs/Control-Monad-Reader.html).

## Context example

Consider the following example of using Context in a bioinformatics workflow. First, context variables can be defined using JSON in the redun configuration file:

```ini
# .redun/redun.ini
context = 
  {
    “bwa”: {
      "executor": "batch",
      “memory”: 10,
      “platform”: “illumina”
    }
  }
```

These values can be fetched from anywhere in the workflow using the `get_context(context_path)` task, where `context_path` is a dot separated path into the JSON (e.g. `"bwa.memory" or "bwa.platform"). Now let's see how this can be used in the workflow:

```py
# workflow.py

@task(executor=get_executor("batch"), memory=get_context(“bwa.memory”))
def run_read_aligner(
    input_file: File, 
    platform: str = get_context(“bwa.platform”),
) -> File:
    # At runtime, `platform` will be set to “illumina”.
    # Notice, the task options `executor` and `memory` can also be parameterized from the context.
    # ...

@task
def align_sample(input_file: File) -> File:
    # Notice we don’t need to specify `platform`. 
    # It will be set by the default value, which reads from the context at runtime.
    bam = run_read_aligner(input_file)
    bam2 = postprocess_bam(bam)
    return bam2

@task
def main(input_files: List[File]) -> File:
    # This top-level task calls align_sample() which calls run_read_aligner().
    # The `memory` and `platform` values don’t need to be passed as arguments
    # to intermediate tasks like align_sample().
    output_files = [align_sample(input_file) for input_file in input_files]
    return output_file
```

In the above workflow, parameters are configured in the configuration file `.redun/redun.ini` and are accessible in low-level tasks like `run_read_aligner()`. In particular, this workflow makes use of context to define the default argument of `platform` as well as how much memory to use for the job.

See the [context example](https://github.com/insitro/redun/tree/main/examples/context), for a working example.

## Defining the context

The context can be defined using several mechanisms:

- Context can be defined within the configuration file using [`context`](config.md#context).
- Context can be defined in a separate JSON file using [`context_file`](config.md#context_file).
- Context can be defined from the command-line using `--context` and `--context-file`. See `redun run -h` for more.
- Context can be updated within the workflow using [`Task.update_context(context)`](https://insitro.github.io/redun/redun/redun.html#redun.Task.update_context).
  - See [context/workflow2.py](https://github.com/insitro/redun/tree/main/examples/context/workflow2.py) for an example.
