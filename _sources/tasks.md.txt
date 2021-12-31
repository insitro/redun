---
tocpdeth: 3
---

# Tasks

Tasks are the basic unit of work in a redun workflow. For a review of their basic features see [Tasks as pure functions](design.md#tasks-as-pure-functions), [Task hashing](design.md#task-hashing), and [Task naming](design.md#task-naming).

## Task options

Tasks can be configured in many ways to adjust how they are executed and cached. For example, the [`executor`](#executor) task option defines which [executor](executors.md) system (local, AWS Batch, etc) the task should execute on, and the [`cache`](#cache) task option defines whether the cache can be used to fast forward through the task.

The most common way to configure a task is with the [`@task`](redun/redun.md#redun.task.task) decorator. For example, this task runs on AWS Batch with 4Gb of memory and without using caching:

```py
@task(executor="batch", memory=4, cache=False)
def process_dataset(dataset: list, col: str, value: Any) -> list:
    dataset2 = [
        row
        for row in dataset
        if row[col] == value
    ]
    return dataset2
```

Tasks options can also be overridden at call-time using [`Task.options()`](redun/redun.md#redun.task.Task). For example, we could dynamically set the memory required for a AWS Batch task based on the dataset size.

```py
@task()
def main(input: File) -> list:
    dataset = read_input(input)

    # Dynamically change memory depending on size of dataset.
    memory = len(dataset) / 1000
    dataset2 = process_dataset.options(memory=memory)(dataset, "color", "red")
    return dataset2
```

Lastly, several task options, such as [`image`](config.md#image) or [`memory`](config.md#memory), can be inherited by the [executor configuration](config.md#executors). 

### `cache`

A bool (default: true) that defines whether the cache can be used to fast forward through the task's execution. If set to `False`, the task will run every time the task is called in a workflow.

### `check_valid`

A string (default: `"full"`) that defines whether cached results are checked for validity in the entire subtree
beneath this task (`"full"`) or whether just this task's results need to be valid (`"shallow"`). This can be used to dramatically speed up resuming large workflows.

### `config_args`

A list of strings defining config args for this task.
Config args will be ignored when hashing a task meaning that changes in config args' values will not cause tasks to re-evaluate.
The values in the `config_args` list should match arg or kwarg names in the task definition.
For example, consider:

```py
@task(config_args=["memory"])
def task1(arg1: int, memory: int) -> int:
    return task2.options(memory=memory)(arg1)
```

The `memory` arg in `task1` will be ignored when hashing a task and changing that value will not trigger re-evaluation of the task.

### `executor`

A string (default: `default`) that defines which executor the task executes on. See [Executors](executors.md) for more information.

### `hash_includes`

A list of data to include in the task hash. Changing the hash will invalidate cached execution results.

### `limits`

A list or dict of resources needed for this task to execute. See the [Limits](config.md#limits) feature for more information.

### `load_module`

The name of a python module that can be loaded to recreate this task. For example, this is part of how a remote worker is instructed to initialize itself, in preparation for running a task. 

### `name`

A string (default: `func.__name__`) naming the task. Task name must use only alphanumeric characters and underscore '_'.

Every task in a workflow needs a unique name. By default this is automatically inferred from the function's name (e.g. `func.__name__`). However, the user may use the `name` task option to explicitly set a task's name. See [Task naming](design.md#task-naming) for more.

### `namespace`

A string defining the namespace for a task. Task namespace must use only alphanumeric characters, underscore '_', and dot '.', and may not start with a dot.

Ideally, tasks should be uniquely identified across workflows by their fullname, which is a combination of their namespace and name. Typically, the namespace is set for the entire python module using the `redun_namespace` global variable (See [Task naming](design.md#task-naming) for more). However, the namespace can also be configured explicitly using this task option.

### `nout`

An optional int (default: `None`) that can be used for tasks that return a tuple or list of length `nout`. Typically, tasks return lazy results with unknown length (`nout=None`). This prevents users from iterating or destructuring results like they could with a normal tuple or list. Using `nout`, a user can iterate and destructure results.

```py
@task(nout=2)
def make_pair() -> Tuple[str, int]:
    return ("Bob", 20)

@task()
def main() -> str:
    # Destructuring works because make_pair defines nout=2.
    name, age = make_pair()

    # For-loop iteration also works because nout is defined.
    results = []
    for item in make_pair():
        results.append(item)

    return [name, age, results]
```

### `script`

A bool (default: `False`) that defines whether this task is a [script task](design.md#script-tasks).

### `version`

An optional string (default: None) defining the version of the task. If defined, the task version helps to determine the hash for a task. When not given, the task hash will depend on the task's source code. Therefore, an explicit version allows the user to safely refactor a task without causing unnecessary re-execution of their pipeline.

See [Task hashing](design.md#task-hashing) for more.
