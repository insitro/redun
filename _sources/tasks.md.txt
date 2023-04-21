---
tocpdeth: 3
---

# Tasks

Tasks are the basic unit of work in a redun workflow. 

## Task naming

Every task has a name and namespace that uniquely identifies it within and across workflows. Task names and namespaces are also used as part of [task hashing](#task-hashing) to ensure unique hashes. Typically, the name of a task can be automatically inferred from the function's name (e.g. `func.__name__`) and the namespace can be set once at the top of a module (`redun_namespace`). For example, in this workflow

```py
# workflow.py
from redun import task

redun_namespace = "my_workflow"


@task()
def my_task():
    # ... task definition
```

the task `my_task()` has name `my_task` and namespace `my_workflow`. From the command line, you can run `my_task` as

```sh
redun run workflow.py my_task
```

or with its fully qualified name:

```sh
redun run workflow.py my_workflow.my_task
```

If needed, task name and workflow can be defined directly in the `@task` decorator:

```py
@task(name="cool_task", namespace="acme")
def my_task():
    # ... task definition
```

Namespaces are especially important for distinguishing tasks when call graphs recorded from different workflows are combined into one database.

## Tasks as pure functions

In order for redun to safely cache results and execute tasks in parallel, a few restrictions must be followed when writing a task. Primarily, tasks must be written such that they are "effectively [pure functions](https://en.wikipedia.org/wiki/Pure_function)". That is, given the same arguments, a task should always return (effectively) the same result. redun does not try to enforce strict purity, so minor side-effects such logging, or caching, are not an issue. A task can even be stochastic as long as the user is ok with using cached results from previous executions.

Tasks are allowed to call other tasks, just like one would write a function to call other functions. In fact, this is the encouraged way of building up a workflow program. Tasks are also allowed to call plain Python functions, as long as the net result of the task is effectively pure. You may also call tasks from plain Python functions as well. In fact, this often occurs when using functions such as `map()`.

```py
def small_helper(item):
    # Small plain Python code...
    return item2

@task()
def process_item(item):
    # Process an item...
    result = small_helper(item)
    return result

@task()
def process_items(items):
    return list(map(process_item, items))
```

One point of caution about using plain Python functions is that redun cannot hash the contents of a plain Python function, such as `small_helper` above. So changes to that function will not trigger re-execution. It becomes the responsibility of the user to force re-execution if needed, perhaps by bumping the version of the calling task or explicitly indicating the dependency to Redun (see [Task Hashing](#task-hashing) for more).

See [Redun tasks are not general python](#redun-tasks-are-not-general-python) below, for more.

## Task hashing

As with general `Value`s, redun relies on hashing tasks to identify whether two tasks are the same. Among other things, this allows us to identify if a past computation used the "same" task, and hence might be a suitable cache result.

For Tasks, redun provides multiple ways to determine a hash. By default, Tasks are hashed by their name, namespace, and source code. You can inspect this process yourself with `Task.source` and `Task.hash` attributes:

```py
@task()
def step1(a, b):
    return a + b

print(step1.source)
# """def step1(a, b):
#     return a + b
# """

print(step1.hash)
# '3f50b2a534c0bf3f1a977afbe1d89ba04501a6f0'
```

Hashing based on the source of a Task function is very convenient for iterative development. Simply by changing the code of a Task, redun will detect that it will need to re-execute it.

Sometimes, a user may want to refactor a Task without triggering re-execution. This can be done by manually versioning tasks, and only bumping the version when a meaningful change is made to the Task. Any string can be used as a task version. No order is assumed with versions, redun only checks task version equality.

```py
@task(version='1')
def step1(a, b):
    return a + b
```

Users may also want to tether a task hash to other objects. This can be done with the `hash_includes` argument in the task decorator, which is especially useful to force re-execution of a task if its plain Python helper functions change. Note that this will override versioning.
```py
def helper(a, b):
  return a * b

@task(hash_includes=[helper])
def step1(a, b):
    return str(helper(a, b))
```


## Nested values

It is common in workflow frameworks to allow a task to have multiple outputs. This is useful when different outputs need to be routed to their own downstream tasks. redun aims to be pythonic, and in Python functions return one value. However, return values can be container types such as lists, tuples, dict, etc. In that spirit, redun gives special treatment to the built-in Python containers (list, tuple, dict, namedtuple, set, dataclass) in order to provide features like multiple outputs and inputs. In redun, we call a value built up from possibly multiple nested container types, a *nested value*. Take the following code for example:

```py
@task()
def run_calculation(data):
    # Perform calculation...
    return {
        'output1': output1,
        'output_list': [output_a, output_b, output_c]
    }

@task()
def step2(item):
    # Process item...

@task()
def step3(items):
    # Process items...

@task()
def main():
    data = # Get data from somewhere...
    outputs = run_calculation(data)
    result2 = step2(outputs['output1'])
    result3 = step3(outputs['output_list'][:2])
    return [result2, result3]
```

From the lazy-evaluation section, we know that `run_calculation(data)` returns an Expression, which we usually have to pass to another task in order to obtain a resolved concrete value (i.e. "look inside"). However, redun allows lazy attribute access for nested values (such as dict). For example, the above `outputs['output1']` returns another Expression:

```py
print(outputs)
# TaskExpression('run_calculation', (data,), {})

print(outputs['output1'])
# SimpleExpression(
#   'getitem',
#   (
#     TaskExpression('run_calculation', (data,), {}),
#     'output1'
#   ),
#   {}
# )
```

By the time this expression is passed to `step2()` it will evaluate to the concrete value `output1` from `run_calculation()`. redun implements a number of builtin simple expressions, such as 'getitem' (`expr[key]`), 'getattr' (`expr.attr`), and 'call' (`expr(arg1, arg2)`).

Also, multiple lazy calls can be chained such as `outputs['output_list'][:2]`. This lazy-evaluation technique allows us to define dataflows even in the case of multiple outputs.

In the `main()` task, we return a nested value that contains Expression, `result2` and `result3`. redun will recurse into nested values to detect additional Expressions that must be resolved before `main()` can fully resolve.

Expressions can also be nested in arguments:

```py
@task()
def task1(x):
    return x + 1

@task()
def adder(values):
    return sum(values)

@task()
def main():
    values = [task1(i) for i in range(10)]
    return adder(values)
```

Notice that `values` in `main()` is actually a list of Expressions. The redun scheduler knows to wait on the evaluation of all expressions within a nested value before using it as an argument to a task (`adder`).


## Recursion

Another example of the generality of our call graph recording technique is the ability to define workflows with recursion, which many workflow engines cannot express. Take the following example:

```py
@task()
def add(a: int, b: int):
    return a + b


@task()
def fib(n: int):
    if n <= 1:
        return 1
    return add(fib(n - 1), fib(n - 2))
```

This computes the n<sup>th</sup> [Fibonacci number](https://en.wikipedia.org/wiki/Fibonacci_number) using recursion. redun doesn't prevent a task from calling itself, and in fact, due to the memoization, this calculation will be done in linear time instead of exponential.


## Task options

Tasks can be configured in many ways to adjust how they are executed and cached. For example, the [`executor`](#executor) task option defines which [executor](executors.md) system (local, AWS Batch, etc) the task should execute on, and the [`cache`] task option defines whether the cache can be used to fast-forward through the task (see [Scheduler](scheduler.md) for more information on caching).

The most common way to configure a task is with the [`@task`](redun.task.task) decorator. For example, this task runs on AWS Batch with 4Gb of memory and without using caching:

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

Tasks options can also be overridden at call-time using [`Task.options()`](redun.task.Task). For example, we could dynamically set the memory required for an AWS Batch task based on the dataset size.

```py
@task()
def main(input: File) -> list:
    dataset = read_input(input)

    # Dynamically change memory depending on size of dataset.
    memory = len(dataset) / 1000
    dataset2 = process_dataset.options(memory=memory)(dataset, "color", "red")
    return dataset2
```

Lastly, several task options, such as [`image`](config.md) or [`memory`](config.md), can be inherited by the [executor configuration](config.md#executors). 

### `allowed_cache_results`
Generally not a user-facing option, this is a `Optional[Set[CacheResult]]` specifying an upper bound on which kind of cache results are may be used (default: `None`, indicating that any are allowed). 

### `cache`
A bool (default: `true`) that defines whether the backend cache can be used to fast-forward through the task's execution. See [Scheduler](scheduler.md#Configuration-options) for more explanation.
A value of `true` is implemented by setting `cache_scope=CacheScope.BACKEND` and `false` by setting `cache_scope=CacheScope.CSE`.

### `cache_scope`

A `CacheScope` enum value (default: `CacheScope.BACKEND`) that indicates the upper bound on what scope a cache result may come from. See [Scheduler](scheduler.md#Configuration-options) for more explanation. 

* `NONE`: Disable both CSE and cache hits
* `CSE`: Only reuse computations from within this execution
* `BACKEND`: Allow cache hits from any evaluation known to the backend

### `check_valid`
An enum value `CacheCheckValid` (or a string that can be coerced, default: `"full"`) that defines whether the entire subtree
of results is checked for validity (`"full"`) or whether just this task's ultimate results need to be valid (`"shallow"`). This can be used to dramatically speed up resuming large workflows.
See [Scheduler](scheduler.md#Configuration-options) for more explanation.

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

Every task in a workflow needs a unique name. By default, this is automatically inferred from the function's name (e.g. `func.__name__`). However, the user may use the `name` task option to explicitly set a task's name. See [Task naming](#task-naming) for more.

### `namespace`

A string defining the namespace for a task. Task namespace must use only alphanumeric characters, underscore '_', and dot '.', and may not start with a dot.

Ideally, tasks should be uniquely identified across workflows by their fullname, which is a combination of their namespace and name. Typically, the namespace is set for the entire python module using the `redun_namespace` global variable (See [Task naming](#task-naming) for more). However, the namespace can also be configured explicitly using this task option.

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

See [Task hashing](#task-hashing) for more.


## Redun tasks are not general python

Although redun tasks are designed to allow rich and natural definition of tasks in Python,
tasks need to act as [pure functions](#tasks-as-pure-functions). This section discusses a few potential issues in more depth.

### Object references and uniqueness

As noted in (Hashing and equality)[values.md#Hashing-and-equality], redun does not distinguish
between references to a single underlying object and values that merely have the same hash.

```python
class MyObject:
  def __init__(self, data):
    self.data = data

@task
def task(x, y):
  return [x, y]

# Make two copies of an object with equal hashes
x = MyObject(1)
y = MyObject(1)

# Consider two ways of calling `task`
out1 = task(x, x)
out2 = task(x, y)

# To redun, these two are actually interchangeable, since the hash of the inputs and outputs
# for both are identical. It does not care that `x` and `y` are distinct objects.
# This means that if there was a cached value from `task(x, x)`, this might be returned as 
# the result from `task(x, y)`, even though this might seem like a bug. 
```

Warning: Redun tasks should never rely on the identity of objects, as redun makes no effort to 
define its behavior under changes that do not impact hashing. See [Mutating arguments](#mutating-arguments),
below, for an example of how relying on object identity can produce unexpected behavior.

```python
@task
def unreliable_task(x, y) -> bool:
  # This task could give completely wrong results if the value is cached. Moreover, 
  # if it is included in a more complex workflow, you may find that its output varies arbitrarily,
  # depending upon what parts of the workflow was cached.
  return id(x) == id(y)
```

### External state and non-deterministic functions

Python code often makes use stateful imperative code, in the form of module variables, class
members, etc. These generally need to be avoided, because Redun is designed to leverage the 
deterministic behavior of "effectively" pure functions to efficiently schedule and cache tasks
within workflows. Functions with random or otherwise non-deterministic behavior are likewise 
problematic. 

Functions that are intentionally random can be used, but you need to decide what kind of cache
behavior you intend. Consider the following task:

```python
@task
def rand():
    return random.random()
```

Most likely, the user does not intend this to be cached globally, because we would simply 
generate a single random number and then replay it forever. However, if the cache is disabled,
(or limited in scope) redun will behave consistently, running the task's implementation every time 
and recording all the output values.

The following function is simple enough in ordinary python (if you remove the decorator): it
returns an increasing value as you call it. If caching is enabled (it is by default), this will 
always return `1`. However, unlike functions that are merely random, disabling the cache
leaves us with undefined behavior. Most non-local executors use a fresh python context for each
task, `call_count` would be reset every time. The local executors might share this state, but
then you are left with race conditions and potential thread safety issues.

```python
call_count = 0
@task
def counter():
    call_count += 1
    return call_count
```

Note that redun's unit tests sometimes use this pattern when checking the behavior of the
scheduler. This is a subtle trick that only works because redun can't detect the external state.

### Mutating arguments

Pure functions are generally not allowed to mutate their arguments and redun likewise relies
on this assumption. Many functional programming languages have deep enforcement of immutability 
because this is such a crucial concept, but python does not. Therefore, it is up to the user
not to mutate task arguments. Here, we give a brief exploration of what goes wrong.

```python
class Data:
    int x
    int y
    
@task
def unsafe_set_x(data: Data, x: int) -> Data:
    data.x = x
    return data
    
@task
def unsafe_set_y(data: Data, y: int) -> Data:
    data.y = y
    return data

@task
def compare(d1: Data, d2: Data) -> bool:
    return d1.x < d2.y

@task
def incorrect_mutation() -> Data:
  data = Data()
  unsafe_set_x(data, 1)
  unsafe_set_y(data, 2)
  # If these were regular python functions, this would return `Data(x=1, y=2)` as desired.
  # However, since we discarded the outputs from `unsafe_set_x` and `unsafe_set_y`, redun won't 
  # even bother running them! Instead, this task would construct `data` and return it.
  return data

@task
def incorrect_mutation2() -> bool:
  data = Data()
  
  # This is a purely linear chain of mutations, where we've been careful to use the resulting
  # output to enforce ordering. You can get away with this, although it's very fragile.
  data1 = unsafe_set_x(data, 1)
  data2 = unsafe_set_y(data1, 2)
  
  # To understand why this is fragile, ask whether `data` and `data2` refer to the same
  # object? In ordinary python, they would. However, in redun, they may or may not,
  # it will depend on caching and the executors involved. Either of the steps above might 
  # return us a new object with the result. Hence, of the two mutations above, `data` might have
  # either, both, or none. 
  return compare(data, data2)
  
@task
def safe_set_x(data: Data, x: int) -> Data:
    # A safe approach is to make a deep copy and then modify and return the copy. 
    result = copy.deepcopy(data)
    data.x = x
    return data
```

Redun cannot detect mutation to input arguments and cannot reason about it. As discussed above,
redun does not define its behavior with regards to object identity, which means that most 
input mutation is likewise undefined. Hence, as we show above, the behavior of tasks that mutate 
their arguments is fragile, and this is best avoided altogether.

## Advanced topics

### Tasks as first class values

As we saw in the recursion section, redun can handle complex dataflows, such as recursion. It also can implement higher order tasks (tasks that receive or return other tasks) in order to provide very reusable workflow components.

As an example, let's say we had a pipeline of steps:

```py
@task()
def step1(x):
    # ...

@task()
def step2(x):
    # ...

@task()
def step3(x):
    # ...

@task()
def pipeline(x):
    x = step1(x)
    x = step2(x)
    x = step3(x)
    return x
```

Next, let's say we have several possible tasks for step2 and we wanted to swap them in and out in different situations. To do that, we could make step2 an input to the pipeline.

```py
@task()
def step1(x):
    # ...

@task()
def step2a(x):
    # ...

@task()
def step2b(x):
    # ...

@task()
def step3(x):
    # ...

@task()
def pipeline(step2, x):
    x = step1(x)
    x = step2(x)
    x = step3(x)
    return x

@task()
def main(x):
    return [
        pipeline(step2a, x),
        pipeline(step2b, x),
    ]
```

Tasks can also be used as return values.

```py
# def step1, step2a, step2b, step3, pipeline

@task()
def pick_step2(x):
    if x < 0:
        return step2a
    else:
        return step2b

@task()
def main(x):
    step2 = pick_step(x)
    return pipeline(step2, x)
```


### Task wrapping

We provide a mechanism for wrapping tasks, which is a powerful mechanism for altering
the run-time behavior of a task. Conceptually inspired by `@functools.wraps`, which makes it easier 
to create decorators that enclose functions. As a motivating example, consider a task that needs
to operate in parallel processes on the worker that executes the task. We can partition the 
implementation into a generic wrapper that launches parallel processes and an inner task
that operates within that context.  

Specifically, this helps us create a decorator that accepts a task and wraps it. The task
passed in is moved into an inner namespace, hiding it. Then a new task is created from the
wrapper implementation that assumes its identity; the wrapper is given access to both the run-time
arguments and the hidden task definition. Since `Tasks` may be wrapped repeatedly,
the hiding step is recursive.

A simple usage example is a new decorator `@doubled_task`, which simply runs the original
task and multiplies by two.

```python
def doubled_task() -> Callable[[Func], Task[Func]]:

    # The name of this inner function is used to create the nested namespace,
    # so idiomatically, use the same name as the decorator with a leading underscore.
    @wraps_task()
    def _doubled_task(inner_task: Task) -> Callable[[Task[Func]], Func]:

        # The behavior when the task is run. Note we have both the task and the
        # runtime args.
        def do_doubling(*task_args, **task_kwargs) -> Any:
            return 2 * inner_task.func(*task_args, **task_kwargs)

        return do_doubling
    return _doubled_task

# Create the task implicitly
@doubled_task()
def value_task(x: int):
    return 1 + x

# Or we can do it manually
@doubled_task()
@task(name="renamed")
def value_task2(x: int):
    return 1 + x

# We can keep going
@doubled_task()
@doubled_task()
def value_task3(x: int):
    return 1 + x
```

This mechanism might be used to alter the runtime behavior, such as by retrying the task
if it fails or running it in parallel. We suggest that users prefer other approaches for
defining their tasks, and only use this technique when access to the `Task` object itself is 
needed.


There is an additional subtlety if the wrapper itself accepts arguments. These must be passed
along to the wrapper so they are visible to the scheduler. Needing to do this manually is
the cost of the extra powers we have. 

```python
# An example of arguments consumed by the wrapper
def wrapper_with_args(wrapper_arg: int) -> Callable[[Func], Task[Func]]:

    # WARNING: Be sure to pass the extra data for hashing so it participates in the cache 
    # evaluation
    @wraps_task(wrapper_extra_hash_data=[wrapper_arg])
    def _wrapper_with_args(inner_task: Task) -> Callable[[Task[Func]], Func]:

        def do_wrapper_with_args(*task_args, **task_kwargs) -> Any:
            return wrapper_arg * inner_task.func(*task_args, **task_kwargs)

        return do_wrapper_with_args
    return _wrapper_with_args
```

### Subrun

Redun provides the `subrun` scheduler task, which is able to start a nested scheduler within
the body of a task.

To see why this is useful, imagine
a task graph `main -> f -> g`, where you would like both of `f, g` to occur on one AWS Batch
node, and `main` to occur locally. This is not possible without subrun, because the redun
`batch` executor uses a CLI command called `oneshot` to launch a single task at a time on the
batch node, without actually launching a scheduler. Hence, that if the task was implemented like
this, then the `f` oneshot would return the unevaluated expression `g`, which the original 
scheduler would deploy to another execution.

```py
@task(exectutor="batch")
def g(x):
  return "value" + x

@task(exectutor="batch")
def f():
  return g("str")

@task
def main():
    # Results in two batch executions, one for `f`, then another for `g`.
    return f()
```

We can accomplish the desired outcome by using subrun to push the entire execution onto one worker:

```python
@task
def g(x):
  return "value" + x

@task
def f():
  return g("str")

@task
def main():
    # Only invokes batch once because it starts a scheduler that handles the resolution of
    # both `f` and `g`
    return subrun(f(), executor="batch")
```

See the example `examples/subrun` for more information.

### Federated task

Generally redun requires access to the code implementing a task in order to execute that task.
Federated tasks provide a mechanism to invoke a task without having local access to the code.

The typical scenario is that we have two python repositories, a `local` one we are currently working in,
and a `remote` one containing a task we wish to call, but do not wish to import.
Broadly, we're going to accomplish this by publishing the `remote` repository as a docker image
containing both redun and the package, and we'll provide configuration data explaining how to do so.

We can create `federated_tasks` in the config file that indicates the task by name and how to
import it. It also defers to an executor that can make the code available, which will typically
be one based on Docker, but need not be. This data is termed an "entrypoint", and is identified
by name; there is no namespacing, but incorporating repository identifiers into the entrypoint
name and associated executors may be a useful strategy. This federated entrypoint is wrapped into
a task within the `local` repository and can be used like an ordinary task.

Furthermore, we might wish for these federated tasks and their executors to be provided by the
authors of the `remote` repository. Therefore, the configuration file can indicate `federated_imports`,
which are additional config files that are allowed to specify additional `federated_tasks` and
`executors`. These can be shared out-of-band, for example, by placing them on a shared file system.

In addition to primary federated tasks, we provide tools to support REST-based proxy.
See `redun.federated_tasks.rest_federated_task` and `redun.federated_tasks.launch_federated_task`.
The proxy has two main features. First, it is designed to help facilitate a fire-and-forget approach
to launching jobs (see [Running without a scheduler](design.md#Running-without-a-scheduler) ), 
which is useful in implementing a UI. Second, it can help arrange for permissions, such as 
facilitating AWS role switches.

See the example `examples/federated_task` for more information.

