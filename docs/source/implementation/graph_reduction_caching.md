---
tocpdeth: 3
---

# Graph reduction caching

redun's use of [graph reduction](https://en.wikipedia.org/wiki/Graph_reduction) and caching techniques
are central to the architecture.

## Consequences

redun uses graph reduction to execute a workflow. This provides a clear and established framework for deciding how various execution steps should occur and how their results should propagate. In contrast, other workflow engines, such as Airflow, view workflow execution as simply executing tasks in topological sort order along a directed acyclic graph (DAG). While simple, this approach leaves unaddressed questions about which kinds of caching or graph optimizations (e.g. Common Subexpression Elimination, task composition/fusion) are safe to perform.

Consider, for example, workflow caching, where we fast forward through tasks that have been previously executed. Viewing workflow execution as graph reduction, allows us to consider when we would prefer caching *single reductions* versus *ultimate reductions*, and the consequences of those options. It also allows us to think through how [Common Subexpression Elimination](https://en.wikipedia.org/wiki/Common_subexpression_elimination) (CSE) can be used to provide the benefits for pull-style workflow engines within a broad push-style approach.

Going forward, our decisions on caching and CSE will likely play a role in how we design subschedulers (`subrun`) and federate workflows.


## Design details

### Review of redun's reduction and caching process

Here, we review redun's workflow evaluation process, as well as how redun uses several
caching mechanisms to provide [incremental compute](https://en.wikipedia.org/wiki/Incremental_computing) commonly seen in other workflow engines.
The different caching mechnisms are used together in order to blend the best behaviors of
push- and pull-style workflow engines, as well as provide a balance between rigorous reactivity
(code and data) and user opt-in optimizations.

#### Graph reduction

redun represents a workflow as a large Expression Graph and uses
[graph reduction](https://en.wikipedia.org/wiki/Graph_reduction) to recursively evaluate the graph
until workflow completion. For example, consider the following workflow:

```py
@task
def add(a: int, b: int) -> int:
    return a + b

@task
def add4(a: int, b: int, c: int, d: int) -> int:
    return add(add(a, b), add(c, d))
```

Now, let's walk through what happens when the redun Scheduler, `scheduler`, is used to evaluate a workflow:

```py
result = scheduler.run(add4(1, 2, 3, 4))
```

When `add4` is called, it immediately returns an Expression that represents the task call, specifically:

```py
TaskExpression("add4", (1, 2, 3, 4))
```

This node represents the initial state of our Expression Graph. In practice, as we perform reductions on
the Expression Graph, it will expand and contract until it reduces down to a single concrete value (i.e.
anything other than an unevaluated Expression). In this example, the redun scheduler will notice that all four
arguments are already concrete and will then call the function `add4` with the arguments `(1, 2, 3, 4)`. As we can
see in the workflow above, `add4` itself returns more expressions, specifically a tree of expressions:

```py
TaskExpression(
  "add", (
     TaskExpression("add, (1, 2)),
     TaskExpression("add, (3, 4)),
  )
)
```

This expression tree will subsitute into a our Expression Graph, replacing the previous `add4` node. After subsitution,
the redun Scheduler will recursively evaluate this expression. Specifically, the following reductions will occur:

```
Let e1 = TaskExpression("add4", (1, 2, 3, 4))

Reduce TaskExpression("add4", (1, 2, 3, 4)) to TaskExpression(
  "add", (
     TaskExpression("add, (1, 2)),
     TaskExpression("add, (3, 4)),
  )
)

Substituting that result back into the graph gives us:

e2 = TaskExpression(
  "add", (
     TaskExpression("add, (1, 2)),
     TaskExpression("add, (3, 4)),
  )
)

Reduce TaskExpression("add, (1, 2)) to 3

Reduce TaskExpression("add, (3, 4)) to 7

Substituting those results back into e2 give us:

e3 = TaskExpression("add", (3, 7))

Reduce TaskExpression("add", (3, 7)) to 10

Substituting that result back into e3 gives us:

e4 = 10

We now have a single concrete value and the workflow concludes.
```

#### The redun Expression Graph language

Here, we define a full grammar describing the redun Expression Graph structure.

```
e = TaskExpression(task_name, (e, ...), {arg_key: e, ...}, options)
  | SchedulerExpression(task_name, (e, ...), {arg_key: e, ...}, options)
  | SimpleExpression(op_name, (e, ...), {e, ...})
  | nested_value
  | concrete_value

nested_value = [e, ...]
             | {e: e, ...}
             | (e, ...)
             | {e, ...}
             | named_tuple_cls(e, ...)
             | dataclass_cls(e, ...)

options = {key: concrete_value, ...}
  where key is an option name

named_tuple_cls = User defined NamedTuple
dataclass_cls = User defined dataclass

task_name = Task name string
arg_key = task argument name string

op_name = "add"
        | "mult"
        | "div"
        | ... name of any supported Python operator

concrete_value = Any other python value (e.g. 10, "hello", pd.DataFrame(...))
```

And these are the rules for evaluating an Expression Graph.

```
eval(TaskExpression(task_name, args, kwargs, options), p) => eval(f(*eval(args, p), **eval(kwargs, p)), j)
  where f = function associated with task t with name task_name.
        p = parent job of the TaskExpression or None if this the root Expression.
        j = job created for evaluating the TaskExpression
        j.options = t.options | options  # configuration to customize the execution environment of f
        options = configuration option overrides specific to this TaskExpression

eval(SchedulerExpression(task_name, args, kwargs, options), p) => eval(f(scheduler, p, s, *args, **kwargs))
  where scheduler = redun scheduler
        s = SchedulerExpression(task_name, args, kwargs)
        p = parent job of the SchedulerExpression
        options = configuration to customize the execution environment of f

eval(SimpleExpression(op_name, args, kwargs), p) = op(*args, **kwargs)
  where op = an operation associated with operation name `op_name`, such as
             `add`, `sub`, `__call__`, `__getitem__`, etc.
        p = parent job of the SimpleExpression

eval(ValueExpression(concrete_value), p) => concrete_value
   where p = parent job of the ValueExpression

eval([a, b, ...], p) => [eval(a, p), eval(b, p), ...]
eval({a: b, c: d, ...}, p) => {eval(a, p): eval(b, p), eval(c, p): eval(d, p), ...}
eval((a, b, ...), p) => (eval(a, p), eval(b, p), ...)
eval({a, b, ...}, p) => {eval(a, p), eval(b, p), ...}
eval(named_tuple_cls(a, b, ...), p) => named_tuple_cls(eval(a, p), eval(b, p), ...)
eval(dataclass_cls(a, b, ...), p) => dataclass_cls(eval(a, p), eval(b, p), ...)

eval(concrete_value, p) => concrete_value
```

Note:
- We only create new Job `j` for evaluating TaskExpressions. We do not create new Jobs for evaluating the other expression types. This design choice was made to record provenance at an appropriate level of detail.
- We propagate the parent jobs through the evaluation in order to facilitate CallGraph recording and to pass down environment like data to child Jobs.
- `j.options` is a merge of the task options `t.options` and the overrides `options` from the TaskExpression.
- When evaluating a `SchedulerExpression`, we pass the positional arguments `args` and keywords arguments `kwargs` unevaluated into `f`. This allows `f` to customize how to evaluate the arguments, if at all.


#### Speeding up graph reduction with caching

Let us now consider how caching could help us in speeding up reductions. Calling a task's function could be
an expensive operation, taking seconds to hours of compute time depending on the task. Therefore, prior
to calling any task's function, we would like to consult a cache to see if we have ever performed this reduction before,
and if so, "replay" the substitution. Notice, we could chose to cache either a *single reduction*, such as `e1 -> e2`
(from the example reduction process above), or the *ultimate reduction*, such as `e1 -> e4`.

Given, completely pure and immutable functions and values, it would be completely safe and fastest to always perform 
an ultimate reduction cache replay. However, in redun's use case, there are a few factors where it is safer to
replay single reductions most of the time, and only use ultimate reduction caching as an opt-in optimization.
To give some intuition, consider that we must handle using a cache across workflow executions, where the task
definitions (i.e. code) may have changed in between. We also have external values, such as `File`s, whose contents
are stored externally (such as a file system, or object store), and we must check whether their value has
changed since previous use. Overall, an ideal caching approach should produce the same results as if the workflow
ran with no caching. We use the terms "code reactivity" and "data reactivity" to describe a caching approach that
properly considers the possible ways tasks and external values can change between executions.

To account for task definition changes, we could define a hashing scheme for tasks and use the task hash as part
of the cache key for fetching cache results. redun by default hashes the source code of a task.
However, notice that the value of an ultimate reduction of `add4(1, 2, 3, 4)` depends on not just the
code within in the definition of top-level task `add4`, but also on the code in all other tasks (such as `add`) that
are called directly or indirectly. Ideally, when hashing as task, we could statically analyze the source code and
determine the full closure of tasks that could be called, and include all of their code in the top-level task's hash.
In fact, this is the approach of the [unison](https://www.unison-lang.org/) programming language.

However, given the very dynamic nature of Python and the challenges of discovering the right level of reactivity
to defined across user code, library code, and compiled code, we have chosen to not attempt static analysis. Instead, if
we only perform single reduction caching, we only need to consider the source code of the top-level task.
For example, when performing the reduction of `TaskExpression("add", (1, 2, 3, 4))`, the single reduction
result `TE("add", (TE("add", (1, 2)), TE("add", (3, 4))))` only depends on the code of `add4` and the
arguments `(1, 2, 3, 4)`. If there are code changes to child tasks, such as `add` we will consider them in the
later reductions. This is the approach taken by the [redo](https://redo.readthedocs.io/en/latest/) build system.

We also have an opportunity to inspect the hashes of each value being used as an argument or a result,
which allows us to implement proper external value reactivity. If a File (or any other external value) is used as
an argument to a task, we will naturally consider changes to its contents when constructing the cache key
`eval_hash` (see more below), since `eval_hash` includes the hash of all arguments. However, if a task returns an
external value as part of its result, a cached File reference might be stale, in that it refers to a version of a File
that might have changed due to external manipulation (e.g. user deleted or altered the file) since the previous execution.
For such values, redun defines a validity check (`value.is_valid()`) to determine if the recorded hash still matches
the current hash of a value. Non-external values, such as pure in-memory values like ints, strings, and DataFrames,
are always "valid" by this definition.

In summary, redun's caching process follows the following steps:

- Let `expr` represent a `TaskExpression(task_name, args, kwargs)` that we wish to reduce.
- Let `task` represent the currently defined task with name `task_name`.
- Determine a cache key `eval_hash` using the following hashing approach:

  ```py
  eval_hash = hash([
    hash(task),
    [hash(arg) for arg in args],
    {key: hash(arg) for key, arg in kwargs.items()}
  ])
  ```

- Check whether `eval_hash` corresponds to an ongoing reduction (see CSE below)
  - If yes, return the promise of the ongoing reduction.
- Check whether `eval_hash` exists in the single reduction cache table (`Evaluation`).
  - If no, treat this as a cache miss and perform the reduction by calling the task.
- Let `result` be the deserialized cached value.
- Check whether `result` is still valid to use by calling `result.is_valid()`.
  - If yes, replay `result` as the result of the reduction.
  - If no, treat this as a cache miss and perform the reduction by calling the task.


#### Review of push and pull workflow engines

Workflow engines can be categorized by whether their execution can be seen as "pushing" values as arguments into tasks
or "pulling" results from tasks. Here are a few examples:

- Push workflow engines:
  - [Airflow](https://github.com/apache/airflow)
  - [Prefect](https://www.prefect.io/)
  - [Flyte](https://flyte.org/)
  - [Nextflow](https://www.nextflow.io/)
  - [WDL](https://github.com/openwdl/wdl)
  - [redun](https://github.com/insitro/redun)

- Pull workflow engines:
  - [Make](https://www.gnu.org/software/make/)
  - [SnakeMake](https://snakemake.readthedocs.io/en/stable/)
  - [Luigi](https://github.com/spotify/luigi)
  - Dagster's notion of [software-defined assets](https://docs.dagster.io/guides/dagster/software-defined-assets)

Briefly, push-style workflow engines look the most similar to typical programming languages where one calls a
function (or task) with arguments and receives back a result
(see [Applicative programming language](https://en.wikipedia.org/wiki/Applicative_programming_language)).

In contrast, pull-style workflow engines typically take a workflow defined as a series of rules that describe how
to create a value from several dependent values. For example, a Makefile for compiling C programs uses rules like
the following:

```make
prog1.o: prog1.c
    gcc -c prog1.c

prog2.o: prog2.c
    gcc -c prog2.c

lib.o: lib.c
    gcc -c lib.c

prog1: prog1.o lib.o
    gcc -o prog1 prog1.o lib.o

prog2: prog2.o lib.o
    gcc -o prog2 prog2.o lib.o
```

The user then executes the workflow by asking for the last value, `make prog1` or `make prog2`, and the workflow engine
recurses backwards through the rules to construct a workflow DAG of commands to execute. Here, commands play the
role of tasks using our terminology.

One advantage of pull-style workflow engines, is that the rule-lookup process allows the workflow engine to
determine automatically if there is a commonly reused dependency, such as `lib.o` above, and to only build it once.
This could be achieved in a typical push-style workflow engine, but usually the user must think ahead about all
possible reuses and explicitly pass that dependency as an argument (possibly through many layers of task calls).

The disadvantage of pull-style workflow engines is that is can be very hard to introduce dynamic logic and dependencies.
Intuitively, this is because dynamic workflows requires seeing intermediate results and then deciding what task to
run next, a process often referred to as "data-dependent dynamic execution". This is natural to express in a
workflow syntax where steps are described in chronological order. Given that the rules of a pull-style workflow engine
are consulted backwards relative to execution flow, this can be challenging to express. It's not impossible though,
and workflow engines like [Luigi](https://luigi.readthedocs.io/en/stable/tasks.html?highlight=dynamic#dynamic-dependencies) and [SnakeMake](https://snakemake.readthedocs.io/en/v5.32.1/snakefiles/rules.html#data-dependent-conditional-execution)
have extra syntax for expressing some of these cases. In the field, there are many claims of supporting
"dynamic execution", but such claims often conflate multiple forms of dynamism, ranging from true
data-dependent dynamic execution to simply parameterizing workflow graph structure (as Airflow does) at workflow
graph construction-time (i.e. before seeing any data).


#### Unifying push and pull with Common Subexpression Elimination (CSE)

Given the individual advantages of push and pull workflow engines, can we develop a workflow engine that has the
benefits of both? redun achieves this by using [Common Subexpression Elimination (CSE)](https://en.wikipedia.org/wiki/Common_subexpression_elimination).

The basic idea behind CSE is to detect whether the exact same expression is being reduced multiple times within an
execution and replace them with a single reduction. For example, if we have

```py
result = add(add(1, 2), add(1, 2))
```

we could compute the reduction for `add(1, 2)` once, and reuse its result `x` multiple times such as:

```py
x = add(1, 2)
result = add(x, x)
```

CSE can be thought of as a special case of caching, but within a single workflow execution lifetime. There are, however,
two important situations to consider: 1) what level of caching should CSE use (single vs ultimate reduction) and 2)
how do we deal with pending reductions?

First, the goal of CSE is to collapse common computations as much as possible,
which implies using ultimate reduction caching. Using single reductions would cause unnecessary double traversals of
possibly large common expression subgraphs. However, is it safe to use ultimate reductions? Yes, in the case of CSEs,
we are within the same execution lifetime as the previous reduction, and therefore we can safely assume that neither
task nor external values have changed their hashes since.

Second, we must take care in implementing cache checking, such that opportunities for CSE are not missed simply
because of timing issues. For example, if the two reductions for `add(1, 2)` happened at similar times in parallel
threads, they would both have an initial cache miss and then would perform a reduction. Ideally, we would like to
maintain a set of cache keys for *pending reductions* as well, so that the second reduction attempt would see the
pending cache key and wait on the same promised result.

Overall, by using both CSE and typical result caching, we can implement pull-style logic embedded within push-style
workflows, such as in the example [02_compile](https://github.com/insitro/redun/blob/main/examples/02_compile/make.py).
We believe this gives users the best of both worlds while providing the familiar semantics of "caching".


#### Opt-in ultimate reduction caching

As described above, we default to single reduction caching due to its ability to safely check each task hash
and external value validity. However, there may be situations where the user is willing to ignore
changes to external values that appear in the child task calls, and would like to opt into
ultimate reduction caching as a faster way to replay large workflows. One common example of this is a workflow
that deals with many (say 1000s) of intermediate files and then one mid-level task summarizes the contents of those
files into one main result. The user may be fine skipping validity checking of those intermediate files as long
as we have a valid result from the mid-level task.

redun allows opting into such an approach using the task option `check_valid="shallow"`. Here is an example use:

```py
@task
def process_file(file: File) -> File:
    # Produce an intermediate file...

@task
def summarize_files(files: List[File]) -> File:
    # Summarize a large set of Files into main result.

@task(check_valid="shallow")
def process_files(files: List[File]) -> File:
    # Large fan out computation.
    intermediate_results = [process_file(file) for file in files]
    summary = summarize_files(intermediate_results)
    return summary
```

When consulting the cache for the call `process_files(files)`, since we have `check_valid="shallow"`, we will replay
the ultimate reduction, which will allow us to skip checking the cache of every lower level reduction, such as
`profile_file(file)`.

Although the user may be interested in skipping external value validity checks, they likely are still interested
in task change reactivity, and that was another benefit of the single reduction caching approach.
In redun, we have implemented an optimization that supports task reactivity even when ultimate reduction caching is in
use. Although we avoid performing static analysis of task source code, we rely on the past recorded CallGraphs to reveal
what child task *would be* called for a particular reduction. As part of the CallGraph recording process, we
consolidate the union of all tasks that are called beneath a CallNode. Although large workflows may have many 1000s
of CallNodes, they typically only have dozens of unique tasks and so this extra bookkeeping is not expensive to maintain.
We use the database table `CallSubtreeTask` to link a CallNode to such tasks.

To summarize, when performing ultimate reduction cache checking, we do the following process:

- Check whether `eval_hash` exists in the ultimate reduction cache table (`CallNode`).
  - If no, fallback to the single reduction cache checking process.
- Check whether all task hashes in `call_node.tasks` exist in the current TaskRegistry.
  - If no, fallback to the single reduction cache checking process.
- Let `result` be the deserialized cached value.
- Check whether `result` is still valid to use by calling `result.is_valid()`.
  - If yes, replay `result` as the result of the reduction.
  - If no, treat this as a cache miss and perform the reduction by calling the task.


#### Caching implementation

In terms of cache implementation, we have implemented the following database and in-memory data structures.

We have implemented the `Evaluation` table which provides a mapping from `eval_hash` (our cache key) to
`value_hash` which is the hash of a Value containing the single reduction results. We use this to perform single
reduction cache lookup.

We use the `CallNode` table to implement a mapping from `eval_hash` to `value_hash` which is a reference to the
ultimate reduction result. We use this to perform ultimate reduction cache lookup.

We an in-memory mapping `Scheduler._pending_jobs` which essentially maps an `eval_hash` to a running Job.
We use this to perform CSE detection.
