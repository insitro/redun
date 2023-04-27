---
tocpdeth: 3
---

# Scheduling redun workflows


Redun attempts to allow users to naturally express complex workflows,
while still allowing the scheduler to execute them correctly and efficiently. For many cases,
the default settings are both safe and effective, and will "just work" as expected. However,
caching and scheduling complex workflows is non-trivial, 
so redun offers users the ability to customize the behavior in certain ways. 
This section reviews the essential guarantees and properties of the redun scheduler, 
to assist users needing to customize or understand the execution behavior more closely.

For more background on the design of the caching behavior and the scheduler, we also provide further 
[implementation notes](implementation/graph_reduction_caching.md).

## Core principles of the scheduler

The redun scheduler operates on these principles:

1. Running a workflow with caching should produce the equivalent result as running without, up to 
the cache approximation preferences of the user.
2. The behavior of a workflow should not depend upon the computation time of the tasks involved, 
including when their computation time is effectively zero because of a cache hit.
3. Each expression is run exactly once in an execution.
4. When considering using a cached result from another execution, the scheduler should check 
for reactivity to changing code or to the "validity" of the cached values (up to the cache 
approximation preferences of the user). Within an execution, it does not do so.

Redun implements three interrelated but distinct ideas related to the efficient execution of
workflows:
1. **Unique expressions**: Within a single execution of a workflow, there are some situations 
where we identify that an entire expression is reused and should only be run once.
2. **Common Sub-expression Elimination (CSE)**: Within a single execution of a workflow, we look
for cases of a `Task` being rerun with identical inputs, allowing us to reuse the result.
3. **Caching**: We look for opportunities to reuse the results of a `Task` from a different 
execution. Additional checks are required to ensure this is appropriate.

Although CSE is a form of caching/memoization, as much a possible, we reserve the term "caching"
for cases where the result came from a different execution, whereas CSE is within a single 
execution. We explain these in detail in the following sections.

## Single vs Ultimate reductions

Before explaining the scheduler behaviors, we need to briefly introduce the concept of reductions.
For a longer exposition, see (Graph reduction caching)[implementation/graph_reduction_caching.md].
Consider the following snippet:

```python
@task
def deeply_nested():
    return "hello"

@task
def nested():
    return deeply_nested()

@task
def main():
    return nested()
```

When we execute `main()`, redun will recursively evaluate the result until we get to the grounded
result, `"hello"`. However, in a caching/memoization setting, there are actually two possible 
answers. First, a **single reduction** of `main()` is just the first round of evaluation,
which is the expression `nested()`. Second, the **ultimate reduction** of `main()` is the 
result from the complete recursive evaluation, `"hello"`.

As we will see below, redun uses both approaches. If we think about caching the value of `main()`,
it is attractive to think about the ultimate reduction, so that we can jump straight to the final
answer. However, redun sometimes uses single reductions, because this allows it to be more
careful.

## Unique evaluation of expressions

To introduce expression caching, consider the following basic Python code:

```python
def f(input):
  result = ...
  return result

input = ...
x = f(input)
list1 = [x, x]
list2 = [f(input), f(input)]
```

Assuming that `f` is deterministic, these two lists would be nearly the same. However, the first
would only invoke `f` once, and would contain two references to the same object; the second
would invoke `f` twice and would produce two distinct objects with the same value. To ensure that
redun behaves the same way, we ensure that each expression object is executed exactly once.

Consider the following example:
```python
def rand():
    return random.random()

def main():
  x = rand()
  return {"x1": x, "x2": x, "y": rand(), "z": rand()} 
```

The user clearly wants the random task to be called three time, not four, and produce a result like
this:
```python
{"x1": 0.183, "x2": 0.183, "y": 0.881, "z": 0.354} 
```

If we make this a redun task:

```python
@task(cache_scope=CacheScope.NONE)
def rand():
    return random.random()

@task
def main():
  x = rand()
  return {"x1": x, "x2": x, "y": rand(), "z": rand()} 
```
when the implementation of `main` is actually run, it generates a dictionary of lazy
expression objects:
```python
{"x1": <expression 1>, "x2": <expression 1>, "y": <expression 2>, "z": <expression 3>} 
```

The scheduler will recurse through the dictionary values, looking for expressions it needs
to run. All of these expressions have identical hashes, so they will all be deduplicated and run 
exactly once. The scheduler applies an ultimate reduction for the entire expression. This is
quite different from the original python code, for example, we might get this result:

{"x1": 0.183, "x2": 0.183, "y": 0.183, "z": 0.183} 

This deduplication is based on the hash of the entire expression object. There are some subtle
scoping rules involved. Rather than trying to explain them fully, we will simply note that they
are designed for the situation depicted in these examples, where an expression is reused in 
several places within the result returned from a task. 

This feature will likely change in the future so that redun produces the same output as the
original random python example.

## Common Sub-expression Elimination (CSE)

Within the scope of a single redun execution, redun is careful not to rerun a `Task` with the
same arguments.

This behavior is performed at the level of `Task`s, since it allows us to correctly handle cases 
like this: `expensive_task(add(1, 3)) + expensive_task(add(2,2))`. These expressions are 
obviously different, unique expression logic would not apply. However, once we start evaluating
the user code, we can determine that there are two identical calls to `expensive_task(4)`, which we
only need to do once! Redun is careful to check for CSE between jobs that are still in-flight.
The in this example, events would likely play out in this order:

1. Schedule `add(1, 3)` for execution
2. Schedule `add(2, 2)` for execution
3. Get result `add(1, 3) -> 4`
4. Schedule `expensive_task(4)` for execution
5. Get result `add(2, 2) -> 4`
6. Start scheduling `expensive_task(4)` again, but realize that it's already in-flight. We can simply wait for it!
7. Get result `expensive_task(4) -> expensive_value`, which we will use twice
8. Schedule `expensive_value + expensive_value`

Of course, CSE will also work if the previous run is already finished.

CSE applies an ultimate reduction. After all, we just computed the result ourselves, so we can
be aggressive about reusing it. Errors will be propagated through CSE.

## Task caching

Task caching refers to using the redun backend to find and replaying the result of a `Task`
from a prior execution. The redun database creates persistence for this cache, allowing caching 
to span multiple executions, processes, hosts, etc. In general, we need to be more careful than 
with CSE, that the cached value is appropriate to use.

Task caching operates at the granularity of a single
call to a `Task` with concrete arguments. Recall that the result of a `Task` might be a value,
or another expression that needs further evaluation. In its normal mode, caching uses single
reductions, stepping through the evaluation. See the (Results caching)[design.md#Result-caching] 
section, for more information on how this recursive checking works.

Consider the following example:
```python
@task
def inner(in):
    return ...

@task
def prep(in):
  return ...

@task
def main(in):
  return inner(in)

data = ...
out = main(prep(data))
```

To evaluate `out`, the following three task executions might be considered for caching:
* `prep(data)`, since arguments are prepared first
* `main(prep_result)`, where `prep_result` is defined as the output from `prep(data)` 
* `inner(prep_result)`

For CSE, we could simply assume that the code was identical for a task, but for caching, 
need to actually check that the code is identical, as defined by the 
(hash of the Task)[tasks.md#Task-hashing]. Since `Value` objects can represent state in addition 
to their natural values, we need to check that the output is actually valid before using a cache
result; see (Validity)[values.md#Validity].

The normal caching mode (so-called "full") is fully recursive (i.e., uses single reductions), 
hence the scheduler must visit every node in the entire call graph produced by an expression,
checking the Task hashes and output validity, even if every single step is cached.
Verifying the validity of the intermediate states is a conservative approach to ensure
that the state of the world after the workflow is identical, regardless of whether the cache was 
used.

For large graphs, this means a fully-cached result can still be expensive to compute, especially
if validity checking of the outputs is expensive. Therefore, redun offers an opt-in 
mode to approximate this computation with an ultimate reduction. Specifically, it skips
the intermediate values and only checks the validity of the final values.
This is called `shallow` validity checking. 

In shallow mode, reactivity to code more conservative: 
redun will rerun the task if there are code changes anywhere in the transitive closure of the
called tasks, that is, the task will be rerun if the code changes for the task itself, or any
task that is called downstream of it. Normally, changes to downstream code would only trigger
rerunning of the directly impacted tasks, when the recursive evaluation actually reached
that node in the call graph. Typically, if a shallow cache hit is not available, redun will
fall back to a single reduction, so any extra conservatism does not necessarily cause 
reevaluation of the entire call graph.

In shallow mode, we only check the validity of the final results, skipping over any 
intermediate results. This reduces the complexity from 
`O(#call_nodes + #intermediate_values + #output_values)` to 
`O(#distinct_tasks + #output_values)`. For deep expression trees, this savings can be significant, 
but the reduction in value reactivity may be undesirable (i.e., could lead to incorrect caching),
especially for tasks working with files. 

Unlike CSE, errors are not replayed from the cache. Although some errors might make sense to
replay, such as ones derived from deterministic code bugs, others, like out-of-memory errors, 
do not. Therefore, redun takes the conservative approach and discards errors retrieved from 
the cache and reruns the task.

## Configuring the scheduler

The scheduler and tasks provide a few different mechanisms for customizing the behavior of redun.

### Configuration options

The most common mechanism for customizing the behavior of the scheduler is to use task options 
`check_valid` and `cache`, to switch between full and shallow validity checking, or to turn off
caching, respectively. As an advanced option, the `cache_scope` option can disable CSE as well.
See (task options)[tasks.md#task-options].

The scheduler as a whole can be configured to disable caching, either with the `--no-cache` CLI 
flag, the `scheduler.run` argument, or with the configuration file.

Users cannot customize the reused expression logic. 

An important detail is that all of these settings only impact our use of **reading** from the 
cache. Regardless of these settings, the full updates to the backend get written.
For example, this implies that a run where the cache has been disabled is eligible to seed cache 
values for later runs. Additionally, a task that is run with full validity checking can be used
to populate the cache for later runs of the same task with shallow validity checking.

### Configuration playbook

We conclude our discussion of the scheduler behavior by summarizing some of the most common
situations where users might interact with these flags.

* In the vast majority of situations, the default `Task` options (i.e., with CSE enabled and caching 
with full validity checking) should work, and the scheduler should have cache enabled (the default).
* If a `Task` creates a lot of intermediate values or has a very deep call graph, so that cache hits
are expensive, consider approximating the cache with `check_valid="shallow"`. 
* If the task is calls code that redun cannot include in the Task hash, disable the cache with
`cache=False` to ensure that you don't reuse a value from a different execution with different code.
For example, redun does this for shell scripts. Leave CSE enabled, since this probably won't 
be an issue within an execution.
* Similarly, if you have a task that is sensitive to the environment, perhaps depending on the OS,
such that you can't reuse values across executions, disable the cache with `cache=False`, but 
leave CSE enabled.
* If you have a non-deterministic task and never want results to be reused, disable both caching
and CSE with `cache_scope=CacheScope.None'
* If you have a recursive task, be careful about disabling CSE with `cache_scope=CacheScope.None',
since it may be necessary for the workflow to actually terminate!
* Disabling the cache across the entire scheduler isn't useful very often, it's primarily a 
tool for debugging.

## Summary of evaluation algorithm

redun's evaluation process can be summarized with the following pseudo-code:

```
def evaluate(expression):
  1. If we've already evaluated this expression, return the pre-existing result
  2. Else, if the expression is a collection:
    1. Recurse: call evaluate on each item in the collection and return the result
  3. Else, if the expression is a concrete value, return it, we're done.
  4. Else, the expression must be a single call to a task:
    1. Recurse: Call evaluate on the arguments of the expression (to reduce to concrete values)
    2. If the cache has the result for this task+arguments, return it
    3. If not cached, actually evaluate the task by submitting to an executor
    4. Recurse: Call evaluate on the result (to reduce to concrete values)
    5. Return the final value
```

To provide some examples:
* Concrete values might be numbers `1.23`, strings `"hello"`, or simple objects
* A collection might be a list, tuple, or dictionary.
* A call to a task might be `foo(1)`
* An expression might be recursive: `foo(foo(1) + foo(2))`. The outer call to `foo` can't be resolved until the inner arguments have been reduced.
* An expression may be a collection of other expressions: `[foo(1), foo(2 + foo(3))]` or `{1: foo(1), 2: foo(2)}`

In order to comply with the principle that the execution duration does not impact the behavior
of a workflow, a particular Task execution is eligible to provide a cache result from the moment
that it is scheduled. While the evaluation is pending, the scheduler does not actually have the 
result yet, but it will merely wait until the already-scheduled computation finishes; it will
not run it twice merely because the result is not finished yet. Getting a cache hit from 
a still-pending task improves both performance and consistency of the scheduler. Likewise, 
each expression object is only evaluated once, regardless of whether it is finished running.
The scheduler implementation has to specially handle these behaviors.


## Scheduler tasks

"Scheduler tasks" provide a method for extending and altering the scheduler. When working
with scheduler tasks, be sure to carefully consult their documentation for any changes.

Scheduler tasks are not subject to CSE or caching, but can be involved in unique expression 
deduplication.

## Scheduler overhead

The scheduler implementation makes every effort to be performant, but the scheduler does
impose some costs, as compared to the equivalent python code. 

Redun adds several other run-time penalties, such as needing to hash values and source code,
check for value validity, and upload/download results and bookkeeping to/from the backend. 
These operations are memoized wherever possible and typically scale linearly with the size of 
the call graphs produced by workflows.

The redun scheduler also imposes non-trivial memory overhead, since 
it needs to maintain in-memory caches of expressions and tasks and their outputs.
However, redun tasks are usually designed not to directly handle large values with in-memory
objects, because of the combination of time, space, and network overheads, and the impact
on the size of the central database. Offloading large objects to opaque value types, like files,
reduces this penalty in practice.


