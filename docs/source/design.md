---
tocpdeth: 3
---

# Design overview

Here, we describe the high-level design features of redun.

## Motivation

The goal of redun is to provide the benefits of workflow engines for Python code in an easy and unintrusive way. Workflow engines can help run code faster by using parallel distributed execution, they can provide checkpointing for fast resuming of previously halted execution, they can reactively re-execute code based on changes in data or code, and can provide logging for data provenance.

While there are [A LOT](https://github.com/pditommaso/awesome-pipeline) of workflow engines available even for Python, redun differs by avoiding the need to restructure programs in terms of dataflow. In fact, we take the position that writing dataflows directly is unnecessarily restrictive, and by doing so we lose abstractions we have come to rely on in most modern high-level languages (control flow, recursion, higher order functions, etc). redun's key insight is that workflows can be expressed as lazy expressions, that are then evaluated by a scheduler which performs automatic parallelization, caching, and data provenance logging.

redun's key features are:

- Workflows are defined by lazy expressions that when evaluated emit dynamic directed acyclic graphs (DAGs), enabling complex data flows.
- Incremental computation that is reactive to both data changes as well as code changes.
- Workflow tasks can be executed on a variety of compute backend (threads, processes, AWS batch jobs, Spark jobs, etc). 
- Data changes are detected for in memory values as well as external data sources such as files and object stores using file hashing.
- Code changes are detected by hashing individual Python functions and comparing against historical call graph recordings.
- Past intermediate results are cached centrally and reused across workflows.
- Past call graphs can be used as a data lineage record and can be queried for debugging and auditing.

See the [influences](#influences) for more details on how these features relate to other tools.

## First example

redun augments Python to simulate a functional language with lazy evaluation. By writing code as pure functions without side-effects, redun can safely perform parallel execution and cache function results. By using [lazy evaluation](#lazy-evaluation-and-parallelism), we can execute a Python program quickly to automatically discover the dataflow between functions. Therefore, it is unnecessary to force the developer to specify the dataflow directly (e.g. Airflow's `set_upstream()` and `set_downstream()`).

redun does not try to impose a functional style on all code. Python supports multiple programming paradigms and a typical project might include libraries that makes use of all of them. Therefore, redun allows the user to annotate which functions are effectively pure using a `@task` decorator. Such functions are called Tasks and they are the main unit of execution in redun. Tasks have several special properties that make them a powerful building block for data pipelines.

Here, is a small hello world program for redun:

```py
# hello_world.py
from redun import task, Scheduler

redun_namespace = "hello_world"

@task()
def get_planet():
    return "World"

@task()
def greeter(greet: str, thing: str):
    return "{}, {}!".format(greet, thing)

@task()
def main(greet: str="Hello"):
    return greeter(greet, get_planet())

if __name__ == "__main__":
    scheduler = Scheduler()
    result = scheduler.run(main())
    print(result)
```

It could be executed as a regular Python script:

```sh
python hello_world.py
# Which prints:
Hello, World!
```

Or it can be executed with the `redun` command-line program:

```sh 
redun run hello_world.py main
# Which prints:
'Hello, World!'
```

There is nothing special about the `main()` task, we could call any task from the command line:

```sh
redun run hello_world.py greeter --greet Hello --thing "Mars"
# Which prints:
'Hello, Mars!'
```

## Key features

### Memoization and reactivity

The return value of tasks are memoized. When calling the same task with the same arguments, a previously cached result is use instead of re-executing the task. This allows us to quickly resume a past execution, by fast forwarding through old parts of a program and only executing new parts.

Consider the example above, which is also available in `examples/hello_world.py`. On first execution, every task executes as indicated by the "[redun] Run" log lines:

```
redun run hello_world.py main

[redun] Check cache main (eval_hash=8cc26021)...
[redun] Run main(args=(), kwargs={'greet': 'Hello'}) on default
[redun] Check cache get_planet (eval_hash=e9a65320)...
[redun] Run get_planet(args=(), kwargs={}) on default
[redun] Check cache greeter (eval_hash=6447887f)...
[redun] Run greeter(args=('Hello', 'World'), kwargs={}) on default
'Hello, World!'
```

If we run a second time, all of the tasks will be cached (memoized) and will be fast-forwarded:

```
redun run hello_world.py main

[redun] Check cache main (eval_hash=8cc26021)...
[redun] Check cache get_planet (eval_hash=e9a65320)...
[redun] Check cache greeter (eval_hash=6447887f)...
'Hello, World!'
```

However, if we run with a new input argument, redun will detect that some tasks (`main`, `greeter`) do need to be re-executed:

```
redun run hello_world.py main --greet Hi

[redun] Check cache main (eval_hash=ff5c0bea)...
[redun] Run main(args=(), kwargs={'greet': 'Hi'}) on default
[redun] Check cache get_planet (eval_hash=e9a65320)...
[redun] Check cache greeter (eval_hash=ce5daced)...
[redun] Run greeter(args=('Hi', 'World'), kwargs={}) on default
'Hi, World!'
```

Also, notice that `get_planet()` did not need to be run again, because it arguments `()` are unchanged.

redun not only reacts to new input data changes, but it can also react to new *code changes*. Say we updated `get_planet()` to

```py
@task()
def get_planet():
    return 'Venus'
```

If we run redun again, redun will detect that `get_planet()` needs to run again because it's code has changed:

```
redun run hello_world.py main

[redun] Check cache main (eval_hash=8cc26021)...
[redun] Check cache get_planet (eval_hash=f01c0932)...
[redun] Run get_planet(args=(), kwargs={}) on default
[redun] Check cache greeter (eval_hash=0e90d651)...
[redun] Run greeter(args=('Hello', 'Venus'), kwargs={}) on default
'Hello, Venus!'
```

How is this achieved? To make this memoization work, we need to know whether we have seen the same Task called with the same arguments. redun performs [hashing of Tasks](#task-hashing) (see below) and arguments, and uses those hashes as a cache key for fetching values from a result cache.


### Lazy evaluation and parallelism

Tasks are executed in a lazy and asynchronous manner. When a task is called, it returns immediately with an `Expression` object:

```py
from redun import task

@task()
def task1(x, y=2):
    return x + y

print(task1(10, y=3))
# TaskExpression('task1', (10,), {'y': 3})
```

This expression means, "apply Task 'task1' to arguments `10` and `y=3`". Expressions are evaluated to their concrete values by the redun scheduler:

```py
from redun import Scheduler

scheduler = Scheduler()
print(scheduler.run(task1(10, y=3)))
# 13
```

To build up a larger program, these Expression objects can be used as arguments to other Tasks, such as:

```py
print(task1(task1(10, y=3)))
# TaskExpression('task1', (TaskExpression('task1', (10,), {'y': 3}),), {})
```

This gives us a tree of expressions, and more generally they can be Directed Acyclic Graphs (DAGs).

The redun scheduler analyzes these expression DAGs in order to determine in which order tasks need to be executed and which are safe to execute in parallel. For example, take the following program:

```py
# Assume step1, step2a, step2b are Tasks.
# Assume data is already defined.

result1 = step1(data)
result2a = step2a(result1, 10, 'red')
result2b = step2b(result1, True)
result3 = step3(result2a, result2b)
```

redun would execute all four lines immediately, deferring executing the steps themselves, and builds up the following DAG of Tasks and Expressions:

```
data --> step1 --> result1 --> step2a --> result2a ---> step3 -- result3
                         \                          /
                          \--> step2b --> result2b /
```

redun will notice that `step2a` and `step2b` are safe to execute in parallel in separate threads (or processes, or batch jobs, etc).

Every argument of a task can be a concrete value (e.g. `10`, `'red'`, `True`, a DataFrame, etc) or a lazy Expression of a value (e.g. `result1`). Just before a task executes, any arguments that were Expressions are resolved and replaced with concrete values. A task can also return an Expression received from subtasks as if it were a concrete value.

Working with lazy expressions requires some changes to how programs are structured. Briefly, once you have an Expression, the only way to "look inside" it, is to pass it to another task.

```py
@task()
def step1(x):
    return x + 1

@task()
def adder(values):
    # Arguments, such as `values` here, are always guaranteed to be concrete values.
    return sum(values)

@task()
def main():
    results = [step1(1), step1(2), step1(3)]
    # `results` is now a list of Expressions.
    # To add them together we need to pass them to another task.
    total = adder(result)
    return total
```

For those who are familiar with [Promises](https://en.wikipedia.org/wiki/Futures_and_promises) from other languages such as javascript, redun tasks perform the equivalent Promise manipulations:

```js
// This call
result2a = step2a(result1, 10, 'red')
// is equivalent to
result2a = result1.then(result1 => step2a(result1, 10, 'red'))
```

```js
// This call
result3 = step3(result2a, result2b)
// is equivalent to
result3 = Promise.all([result2a, result2b]).then(([result2a, result2b]) => step3(result2a, result2b))
```

Our use of Expressions to build up Task dependency is also similar to [Concurrent Futures in Python](https://docs.python.org/dev/library/concurrent.futures.html) and [Dask Delayed](https://docs.dask.org/en/latest/delayed.html). 


### Tasks as pure functions

In order for redun to safely cache result and execute tasks in parallel, a few restrictions must be followed when writing a task. Primarily, tasks must be written such that they are "effectively [pure functions](https://en.wikipedia.org/wiki/Pure_function)". That is, given the same arguments, a task should always return (effectively) the same result. redun does not try to enforce strict purity, so minor side-effects such logging, or caching, are not an issue. A task can even be stochastic as long as the user is ok with using cached results from previous executions.

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

One point of caution about using plain Python functions is that redun cannot hash the contents of a plain Python function, such as `small_helper` above. So changes to that function will not trigger re-execution. It becomes the responsibility of the user to force re-execution if needed, perhaps by bumping the version of the calling task (`process_item`, see [Task Hashing](#task-hashing)).


### Task hashing

In order to correctly cache and reuse past Task results, we need to follow the following rule:

> We can use a cached value instead of re-executing a task, if that result was previously returned from the same task called with the same arguments.

To detect whether we've seen the same function or arguments, we use hashing. First, arguments are hashed by pickling and performing a sha512 hash of the bytes (the serialization and hashing can be overwritten by the user if needed).

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

Sometimes, a user may want to refactor a Task without triggering re-execution. This can be done by manually versioning tasks, and only bumping the version when a meaningful change is made to the Task. Any string can be used as a task version. No ordering is assumed with versions, redun only checks task version equality.

```py
@task(version='1')
def step1(a, b):
    return a + b
```


### Task naming

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


### Result caching

Fully satisfying the caching rule is actually harder than it first looks. First consider this example:

```py
@task(version='1')
def step1(x):
    return x + 1

@task(version='1')
def step2(x):
    return x * 2

@task(version='1')
def main(x):
    result1 = step1(x)
    result2 = step2(result1)
    return result2
```

When we execute `main(10)`, we get the result `(10 + 1) * 2 = 22`.

Now, say we updated `step1` to be:

```py
@task(version='2')
def step1(x):
    return x + 2
```

The correct answer to re-executing `main(10)` should be `((10 + 2) * 2) = 24`, however, overly simplistic result caching could miss the need to re-execute. After all, the code for `main` has not been changed (it's still version='1') and neither have its arguments, `10`. In order to handle such cases correctly, redun takes the following approach.

On the first execution of `scheduler.run(main(10))`:

- The scheduler considers `main(10)` as an Expression `TaskExpression('main', (10,), {})`.
- It first creates a cache key `(task=main, version='1', args=10)` and uses it to look up in a key-value store whether a previous result for this task call has been recorded. As this is our first execution, it will be a cache miss.
- The function `main(10)` is executed, and its initial result is an Expression tree: `TaskExpression('step2', (TaskExpression('task1', (10,) {}),), {})`.
- The *expression tree itself* is cached for the cache key `(task=main, version='1', args=10)`.
- The redun scheduler proceeds to evaluate the Expression tree, calling task `task1(10)` and then `task2(11)`.
- Each of their results are cached with cache keys indicating the task, task version/hash, and arguments.
- Ultimately the scheduler returns concrete value `22`.

For the second execution, with version='2' for step1, we have:

- The scheduler considers `main(10)` as an Expression `TaskExpression('main', (10,), {})`.
- It creates a cache key `(task=main, version='1', args=10)` and finds a cache hit, namely `TaskExpression('step2', (TaskExpression('task1', (10,) {}),), {})`.
- Due to having a cached result, the scheduler skips executing the function `main(10)`, and proceeds to evaluating the expression tree.
- When the scheduler evaluates `TaskExpression('task1', (10,) {}),)`, it notices that the cache key `(task=step1, version='2', args=10)` has not been seen before, and so it executes `task1(10)` to get the result `12`.
- The second sub-expression `TaskExpression('task2', (12,), {})` has also not been seen before, and so it executes as well.
- Ultimately the scheduler returns the concrete value `24`.

The end result of this process is that redun correctly skips re-execution of unchanged high level tasks, such as `main`, but properly re-executes updated deeply nested tasks, such as `task1`. It is quite common in large programs that the upper level tasks mostly perform routing of arguments and results, and the lower level tasks perform the actual work. Routing frequently is unaffected by the actual data or implementations of the lower level tasks, so being able to skip re-execution of high level tasks is an important optimization.


### File reactivity

A common use case for workflow engines is to read and write to files, either stored locally or remotely (object storage, S3, etc). Files can be a tricky data type to accommodate in a purely functional setting, because the contents of files can change outside of our control. Ideally, we would like our workflow engine to react to those changes. For example, we would want to re-execute our workflow when:

- An input file changes its contents, and now we want to re-execute the tasks that read that input file so we can regenerate our downstream results.
- An output file has changed or been deleted, and now we want to re-execute the task that wrote that file so that we can reproduce the output file.

These behaviors are what we typically get from a workflow engine like `make`. Changing a C source code file, gets detected by `make` and it recompiles the source file. Deleting the final compiled program, forces `make` to regenerate the program from earlier steps (source compiling, object linking, etc). In data science workflows, we would often like to re-execute a calculation when the input CSV changes, or if an output plot was deleted or altered.

redun achieves this behavior by wrapping file input/output (IO) in a special class called `redun.File`. Here, is an example usage:

```py
import csv

from redun import task, File

redun_namespace = "examples.files"

@task()
def process_data(input_file):
    with input_file.open() as infile:
        rows = list(csv.reader(infile))

    # result = calculation with rows...

    output_file = File('output.txt')
    with output.open('w') as out:
        out.write(result)
    return output_file

@task()
def main():
    input_file = File('input.csv')
    output_file = process_data(input_file)
    return output_file
```

The `File()` constructor takes a path as an argument (it can be a local file or any [fsspec](https://filesystem-spec.readthedocs.io/en/latest/) supported remote URL, such as S3 or GCP.) and provides familiar file IO methods such as `open()` and `close()`. `open()` gives a usual file stream that we can read or write from. The special logic happens when a File is returned from a task or passed as an argument. In those situations the File is hashed and the File reference is recorded in the call graph. By default, redun performs a fast pseudo hashing of a file by hashing the triple (file path, byte size, modification time) (a technique used by other workflow engines, such as [Nextflow](https://www.nextflow.io/)).

When a change is made to the input file `input.csv`, redun will notice that the File's hash has changed and will consider it a new value that requires `process_data()` to re-execute. This is very similar to the caching logic for any other task argument.

However, if `output.txt` is deleted or altered, redun does something special. When redun retrieves from the call graph a previously cached `output_file` result, it compares the recorded hash with a new hash computed from the current file in the filesystem. If the hashes don't match, the cached File is considered *invalid*, and it is considered a cache miss. This will force `process_data()` to re-execute.

We call File an example of an *External Value*, because Files can change in an external system, such as a file syetsm outside our control. Consequently, when we fetch a File from the cache, we must first check whether its still valid (i.e. cached hash matches current hash from filesystem).

redun naturally understands which Files are used for input vs output based on whether they are passed as arguments or returned as results, respectively. Note, it can lead to confusing behavior to pass a File as input to a Task, alter it, and then return it as a result. That would lead to the recorded call node to be immediately out of date (its input File hash doesn't match anymore). The user should be careful to avoid this pattern.


### Shell scripting

It is common, especially in bioinformatics, to use workflow engines to combine together unix programs into a computational pipeline. Typically, each program exchanges data with each other through input and output files. redun provides several features to ease the calling of programs through scripts and managing their input and output files.

#### Script tasks

A *script task* is a special task in redun whose definition is a shell script as opposed to a python function. Script tasks are defined by passing the task option `script=True` in the `@task()` decorator and returning a string containing a shell script.

```py
@task(script=True)
def task1(arg1, arg2):
    return f"""
    mkdir dir
    cd dir
    my-prog {arg1} {arg2}
    """
```

The user can use even use f-string interpolation to conveniently pass task arguments (e.g. `arg1` and `arg2`) to the script.

The return value of a script task is the standard output of the script. Here, is an example of a normal task, `grep_files()` calling a script task `grep()`:

```py
@task(script=True)
def grep(pattern: str, file: File):
    # Calls grep to find pattern in a file.
    return f"""
    grep {pattern} {file.path}
    """

@task()
def grep_files(pattern: str, files: List[File]) -> List[str]:
    # Calls grep to find a pattern on a list of files.
    matches = [
        grep(pattern, file)  # Result is stdout of `grep {pattern} {file.path}`
        for file in files
    ]
    return matches
```

Scripts are assumed to be `sh` scripts by default, but custom interpreters (e.g. `python`) can be specified using `#!` in the first line.

```py
@task(script=True)
def grep(pattern: str, file: File):
    # Calls grep to find pattern in a file.
    return f"""
    #!/usr/bin/python
    print('Hello, World!')
    """
```

Feel free to indent lines to match the surrounding code. redeun uses [dedent()](https://docs.python.org/3/library/textwrap.html#textwrap.dedent) to remove leading whitespace in scripts.


#### File staging

When defining script tasks that run on a remote execution environment (e.g. AWS Batch), it is common to have to copy input files from a cloud storage location (e.g. S3) to the compute node and similarly copy output files from the compute node back to cloud storage. We call this file staging and unstaging, respectively.

Manually performing file staging would look something like this:

```py
@task(script=True)
def align_dna_reads_to_genome(input_reads_file: File, output_align_path: str, sample_id: str):
    return f"""
        # Stage input file from cloud storage to compute node (local disk).
        aws s3 cp {input_reads_file.path} reads.fastq.gz

        # Run a unix program that processes input reads.fastq.gz and outputs sample.aln.bam
        run-bwamem \
            -R "@RG\\tID:{sample_id}\\tSM:{sample_id}\\tPL:illumina" \
            -o sample \
            -H \
            genome.fa \
            reads.fastq.gz

        # Unstage output file back to cloud storage.       
        aws s3 cp sample.aln.bam {output_align_path}
    """

@task()
def main():
    # Example of calling script task.
    input_reads_file = File('s3://bucket/a/b/c/100.fastq.gz')
    output_align_path = 's3://bucket/a/b/c/100.aln.bam'
    sample_id = '100'
    stdout = align_dna_reads_to_genome(input_reads_file, output_align_path, sample_id)
    # After execution, the file 's3://bucket/a/b/c/100.aln.bam' will be created.
    return stdout
```

The above code should work perfectly fine, however, manually writing input/output staging commands can be tedious. redun provides a utility function `script()` that can further simplify file staging.

```py
from redun import task, script, File

@task()
def align_dna_reads_to_genome(input_reads_file: File, output_align_path: str, sample_id: str):
    return script(f"""
        # Run a unix program that processes input reads.fastq.gz and outputs sample.aln.bam
        run-bwamem \
            -R "@RG\\tID:{sample_id}\\tSM:{sample_id}\\tPL:illumina" \
            -o sample \
            -H \
            genome.fa \
            reads.fastq.gz
        """,
        inputs=[input_reads_file.stage('reads.fastq.gz')],
        outputs=File(output_align_path).stage('sample.aln.bam'),
   )

@task()
def main():
    # Example of calling script task.
    input_reads_file = File('s3://bucket/a/b/c/100.fastq.gz')
    output_align_path = 's3://bucket/a/b/c/100.aln.bam'
    sample_id = '100'
    output_align_file = align_dna_reads_to_genome(input_reads_file, output_align_path, sample_id)
    return output_align_file
```

The function `script()` takes a shell script, plus two additional arguments `inputs` and `outputs` that specify `StagingFile`s. A `StagingFile` is a pair of local and remote `File`s. `script()` will copy inputs from remote file to local file, and outputs from local file to remote file. `StagingFile`s can be defined using the `File.stage()` method:

```py
File(remote_path).stage(local_path)
```

The final result of `script()` will be the same shape of `outputs`, with all `StagingFile`s replaced by their remote `File`s. This allows easy definition of multiple output files:

```py
from redun import task, script, File

@task()
def align_dna_reads_to_genome(
        input_reads1_file: File,
        input_reads2_file: File,
        output_align_path: str,
        output_metrics_path: str,
        sample_id: str
    ):
    return script(f"""
        # Run a unix program that processes input reads.fastq.gz and outputs sample.aln.bam
        run-bwamem \
            -R "@RG\\tID:{sample_id}\\tSM:{sample_id}\\tPL:illumina" \
            -o sample \
            -H \
            genome.fa \
            reads1.fastq.gz \
            reads2.fastq.gz

        # Collect some metrics about sample.aln.bam
        samtools depth -a sample.aln.bam > sample.depth_out.txt
        """,
        inputs=[
            input_reads1_file.stage('reads1.fastq.gz')
            input_reads2_file.stage('reads2.fastq.gz')
        ],
        outputs={
            'align': File(output_align_path).stage('sample.aln.bam'),
            'metrics': File(output_metrics_path).stage('sample.depth_out.txt'),
        }
   )

@task()
def main():
    # Example of calling script task.
    input_reads1_file = File('s3://bucket/a/b/c/100.1.fastq.gz')
    input_reads2_file = File('s3://bucket/a/b/c/100.2.fastq.gz')
    output_align_path = 's3://bucket/a/b/c/100.aln.bam'
    output_metrics_path = 's3://bucket/a/b/c/100.depth_out.txt'
    sample_id = '100'
    
    result = align_dna_reads_to_genome(
        input_reads1_file, input_reads2_file,
        output_align_path, output_metrics_path,
        sample_id,
    )
    # Returns
    # {
    #    'align': File('s3://bucket/a/b/c/100.aln.bam'),
    #    'metrics': File('s3://bucket/a/b/c/100.depth_out.txt'),
    # }

    # Use lazy key access to pass only the alignment to another task.
    result = post_process_align(result['align'])

    return result
```

## Working with Values

Values is the general term redun uses to refer to task arguments and results. Below, we describe some of the special treatment redun uses to enable expressive value dataflow.

### Nested values

It is common in workflow frameworks to allow a task to have multiple outputs. This is useful when different outputs need to be routed to their own downstream tasks. redun aims to be pythonic, and in Python functions return one value. However, return values can be container types such as lists, tuples, dict, etc. In that spirit, redun gives special treatment to the builtin Python containers (list, tuple, dict, namedtuple, set) in order to provide features like multiple outputs and inputs. In redun, we call a value built up from possibly multiple nested container types, a *nested value*. Take the following code for example:

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

From the lazy-evaluation section, we know that `run_calculation(data)` returns an Expression, which we usually have to pass to another task in order to obtained a resolved concrete value (i.e. "look inside"). However, redun allows lazy attribute access for nested values (such as dict). For example, the above `outputs['output1']` returns another Expression:

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

Also multiple lazy calls can be chained such as `outputs['output_list'][:2]`. This lazy-evaluation technique allows us to define dataflows even for the case of multiple outputs.

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


### Recursion

Another example of the generality of our call graph recording technique is the ability to define workflows with recursion, which many workflow engines cannot express. Take the follow example:

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

This computes the n<sup>th</sup> [Fibonacci number](https://en.wikipedia.org/wiki/Fibonacci_number) using recursion. redun doesn't prevent a task calling itself, and in fact due to the memoization, this calculation will be done in linear time instead of exponential.


### Tasks as first class values

As we saw in the recursion section, redun can handle complex dataflows, such as recursion. It also can implement  higher order tasks (tasks that receive or return other tasks) in order provide very reusable workflow components.

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

Next, let's say we have several possible tasks for step2 and we wanted to swap them in and out in different situations. To do that, we could make step2 an input to pipeline.

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

## Data provenance

### Call graphs

<img src="redun-call-graph.svg" width="100%"/>

Depicted above is an example call graph and job tree (left) for an execution of a workflow (right). When each task is called, a CallNode is recorded along with all the Values used as arguments and return value. As tasks (`main`) call child tasks, children CallNodes are recorded (`task1` and `task2`). "Horizontal" dataflow is also recorded between sibling tasks, such as `task1` and `task2`. Each node in the call graph is identified by a unique hash and each Job and Execution is identified by a unique UUID. This information is stored by default in the redun database `.redun/redun.db`.


### Querying call graphs

Every time redun executes a workflow, it records the execution as a CallGraph. The `redun log` command can be used to query past executions and walk through the CallGraph. Here we'll walk through the `examples/compile/` example. The workflow script should look something like this:

```py
import os
from typing import Dict, List

from redun import task, File


@task()
def compile(c_file: File):
    """
    Compile one C file into an object file.
    """
    os.system("gcc -c {c_file}".format(c_file=c_file.path))
    return File(c_file.path.replace('.c', '.o'))


@task()
def link(prog_path: str, o_files: List[File]):
    """
    Link several object files together into one program.
    """
    os.system("gcc -o {prog_path} {o_files}".format(
        prog_path=prog_path,
        o_files=' '.join(o_file.path for o_file in o_files),
    ))
    return File(prog_path)


@task()
def make_prog(prog_path: str, c_files: List[File]):
    """
    Compile one program from its source C files.
    """
    o_files = [
        compile(c_file)
        for c_file in c_files
    ]
    prog_file = link(prog_path, o_files)
    return prog_file


# Definition of programs and their source C files.
files = {
    'prog': [
        File('prog.c'),
        File('lib.c'),
    ],
    'prog2': [
        File('prog2.c'),
        File('lib.c'),
    ],
}


@task()
def make(files : Dict[str, List[File]]=files):
    """
    Top-level task for compiling all the programs in the project.
    """
    progs = [
        make_prog(prog_path, c_files)
        for prog_path, c_files in files.items()
    ]
    return progs
```

For reference, these source files have the following contents:

```c
// lib.c
char *get_message() {
    return "Hello, World!\n";
}
#include <stdio.h>


// prog.c
char *get_message();

int main(int argc, char** argv) {
    char *msg = get_message();
    printf("prog1: %s", msg);
}
#include <stdio.h>


// prog2.c
char *get_message();

int main(int argc, char** argv) {
    char *msg = get_message();
    printf("prog2: %s", msg);
}
```


Let's execute this workflow using `redun run`:

```
redun run make.py make

[redun] Check cache make (eval_hash=fe2a2a7d)...
[redun] Run make(args=(), kwargs={'files': {'prog': [File(path=prog.c, hash=574d39e642f407119c2661559cea39d491bd3dc1), File(path=lib.c, hash=a08d53bbaf62e480408552a2d3680e604f4cff50)], 'prog2': [File(path=prog2.c, hash=b262cdb5130...) on default
[redun] Check cache make_prog (eval_hash=e69b7ae9)...
[redun] Run make_prog(args=('prog', [File(path=prog.c, hash=574d39e642f407119c2661559cea39d491bd3dc1), File(path=lib.c, hash=a08d53bbaf62e480408552a2d3680e604f4cff50)]), kwargs={}) on default
[redun] Check cache make_prog (eval_hash=045f3cca)...
[redun] Run make_prog(args=('prog2', [File(path=prog2.c, hash=b262cdb5130e8984ed3bb0dba8c83e56173ed702), File(path=lib.c, hash=a08d53bbaf62e480408552a2d3680e604f4cff50)]), kwargs={}) on default
[redun] Check cache compile (eval_hash=2872aec1)...
[redun] Run compile(args=(File(path=prog.c, hash=574d39e642f407119c2661559cea39d491bd3dc1),), kwargs={}) on default
[redun] Check cache compile (eval_hash=600f6efe)...
[redun] Run compile(args=(File(path=lib.c, hash=a08d53bbaf62e480408552a2d3680e604f4cff50),), kwargs={}) on default
[redun] Check cache compile (eval_hash=ee7f4a1a)...
[redun] Run compile(args=(File(path=prog2.c, hash=b262cdb5130e8984ed3bb0dba8c83e56173ed702),), kwargs={}) on default
[redun] Check cache link (eval_hash=7d623fd2)...
[redun] Run link(args=('prog', [File(path=prog.o, hash=ea439e92541937ec0777210367ed8a05ec91dad0), File(path=lib.o, hash=ecf0db54c26fd406610d140494ba580740c610e2)]), kwargs={}) on default
[redun] Check cache link (eval_hash=af9f2dad)...
[redun] Run link(args=('prog2', [File(path=prog2.o, hash=60e8a5e97fe8b6064d0655c1eee16ff6dd650946), File(path=lib.o, hash=ecf0db54c26fd406610d140494ba580740c610e2)]), kwargs={}) on default
[File(path=prog, hash=ff7aea3193ee882d1dc69f284d18d3353c97bc11),
 File(path=prog2, hash=e0e9a6701a1cef13d13df62ca492472016e5edc4)]
```

This workflow should have compiled and linked two binaries `prog` and `prog2` (Note, you will need gcc installed for this example to work). We should now be able to execute one of the binaries:

```
./prog

prog1: Hello, World!
```

Now, let's edit one of the C files to have more exclamation marks!!!!!!

```diff
--- a/examples/compile/lib.c
+++ b/examples/compile/lib.c
@@ -1,3 +1,3 @@
 char *get_message() {
-    return "Hello, World!\n";
+    return "Hello, World!!!!!!!!\n";
 }
```

We rerun our workflow and redun will automatically detect that some (but not all) of the tasks need to re-execute because an input file (lib.c) has changed:

```
redun run make.py make

[redun] Check cache make (eval_hash=3bd939d5)...
[redun] Run make(args=(), kwargs={'files': {'prog': [File(path=prog.c, hash=574d39e642f407119c2661559cea39d491bd3dc1), File(path=lib.c, hash=e8ff8720b689411145e97dec7d0f8ad9b4ee4e38)], 'prog2': [File(path=prog2.c, hash=b262cdb5130...) on default
[redun] Check cache make_prog (eval_hash=a925dcaa)...
[redun] Run make_prog(args=('prog', [File(path=prog.c, hash=574d39e642f407119c2661559cea39d491bd3dc1), File(path=lib.c, hash=e8ff8720b689411145e97dec7d0f8ad9b4ee4e38)]), kwargs={}) on default
[redun] Check cache make_prog (eval_hash=8f6b3814)...
[redun] Run make_prog(args=('prog2', [File(path=prog2.c, hash=b262cdb5130e8984ed3bb0dba8c83e56173ed702), File(path=lib.c, hash=e8ff8720b689411145e97dec7d0f8ad9b4ee4e38)]), kwargs={}) on default
[redun] Check cache compile (eval_hash=2872aec1)...
[redun] Check cache compile (eval_hash=6d665ff5)...
[redun] Run compile(args=(File(path=lib.c, hash=e8ff8720b689411145e97dec7d0f8ad9b4ee4e38),), kwargs={}) on default
[redun] Check cache compile (eval_hash=ee7f4a1a)...
[redun] Check cache link (eval_hash=3fcc3117)...
[redun] Run link(args=('prog', [File(path=prog.o, hash=ea439e92541937ec0777210367ed8a05ec91dad0), File(path=lib.o, hash=6370c13da0a6ca01177f4b42b043d5cd860351d6)]), kwargs={}) on default
[redun] Check cache link (eval_hash=9b3784ce)...
[redun] Run link(args=('prog2', [File(path=prog2.o, hash=60e8a5e97fe8b6064d0655c1eee16ff6dd650946), File(path=lib.o, hash=6370c13da0a6ca01177f4b42b043d5cd860351d6)]), kwargs={}) on default
[File(path=prog, hash=6daae76807abc212f1085b43b76003821314309f),
 File(path=prog2, hash=62a3a615ed8e6f5265f76f3603df8f58790ba1d6)]
```

We should see our change reflected in the new binary:

```
./prog

prog1: Hello, World!!!!!!!!
```

Now, let's look at the call graphs recorded so far. We can list past executions like so:

```sh
redun log

Recent executions:
  Exec a5583da5-3787-4324-86c9-ad988753cd2e 2020-06-26 22:21:32:  args=run make.py make
  Exec 3bd1e113-d88f-4671-a996-4b2feb5aa58b 2020-06-26 22:15:03:  args=run make.py make
```

Each execution is given a unique id (UUID). To list more information about a specific execution, simply supply it to the `redun log` command:

```
redun log a5583da5-3787-4324-86c9-ad988753cd2e

Exec a5583da5-3787-4324-86c9-ad988753cd2e 2020-06-26 22:21:32:  args=run make.py make
  Job 1604f4c0-aca0-437b-b5ae-2741954fafbd 2020-06-26 22:21:32:  task: make, task_hash: 0d9dca7d, call_node: c2de7619, cached: False
    Job 7bc38754-8741-4302-ae81-08d043b345aa 2020-06-26 22:21:32:  task: make_prog, task_hash: 16aa763c, call_node: 88f47605, cached: False
      Job 1f4b4a76-a920-4b6a-8323-1df9876354e3 2020-06-26 22:21:32:  task: compile, task_hash: d2b031e4, call_node: fedc6cdc, cached: True
      Job bfa81ca0-1897-4e88-8be9-030941dae648 2020-06-26 22:21:32:  task: compile, task_hash: d2b031e4, call_node: 7872899b, cached: False
      Job 96681441-e186-42f7-9e99-663fc0d0d08c 2020-06-26 22:21:32:  task: link, task_hash: e73b3cd2, call_node: 1ddb7325, cached: False
    Job e0109842-8a69-4a4c-b2c6-9b97bfd35b43 2020-06-26 22:21:32:  task: make_prog, task_hash: 16aa763c, call_node: 4453e6cd, cached: False
      Job 94830f60-acaa-4b6b-9b22-f4572c64f85b 2020-06-26 22:21:32:  task: compile, task_hash: d2b031e4, call_node: 1dc10a1c, cached: True
      Job 7935a494-01f6-47ff-8657-b8877bfe1e84 2020-06-26 22:21:32:  task: link, task_hash: e73b3cd2, call_node: bab7987a, cached: False
```

Now, we can see the tree of Jobs and information about each one, such as the task, task_hash, CallNode hash, and whether the result was cached. To display information about any specific object (Job, Task, CallNode) simply use `redun log <id_or_hash_prefix>`. We can get more information about the link Task (`e73b3cd2`) like so:

```
redun log e73b3cd2

Task link e73b3cd20246b559ac2b9c2933efe953612cc3ab
    First job 88eb9fd6 2020-06-26 22:15:07.424194

    def link(prog_path: str, o_files: List[File]):
        """
        Link several object files together into one program.
        """
        os.system("gcc -o {prog_path} {o_files}".format(
            prog_path=prog_path,
            o_files=' '.join(o_file.path for o_file in o_files),
        ))
        return File(prog_path)
```

Here we see the source code of the task at the time it was run. We can also get more information about the CallNode (`bab7987a`):

```
redun log bab7987a

CallNode bab7987ae6157f5dea825730e8020470a41c8f0c task_name: link, task_hash: e73b3cd2
  Result: File(path=prog2, hash=62a3a615ed8e6f5265f76f3603df8f58790ba1d6)
  Parent CallNodes:
    CallNode 4453e6cd53c7f1c92dedebcd05e73a19b0ef08d9 task_name: make_prog, task_hash: 16aa763c
```

We can also query by file name. Which CallNode created file `prog`?

```
redun log prog

File(hash='6daae768', path='prog'):
  - Produced by CallNode(hash='1ddb7325', task_name='link', args=['prog', [File(path=prog.o, hash=ea439e92541937ec0777210367ed8a05ec91dad0), File(path=lib.o, has...])
  - During Job(id='96681441', start_time='2020-06-26 22:21:32', task_name='link')

  Task link e73b3cd20246b559ac2b9c2933efe953612cc3ab
      def link(prog_path: str, o_files: List[File]):
          """
          Link several object files together into one program.
          """
          os.system("gcc -o {prog_path} {o_files}".format(
              prog_path=prog_path,
              o_files=' '.join(o_file.path for o_file in o_files),
          ))
          return File(prog_path)
```

Which task read `prog.c`?

```
redun log prog.c

File(hash='574d39e6', path='prog.c'):
  - Consumed by CallNode(hash='fedc6cdc', task_name='compile', args=[File(path=prog.c, hash=574d39e642f407119c2661559cea39d491bd3dc1)])
  - During Job(id='1f4b4a76', start_time='2020-06-26 22:21:32', task_name='compile')

  Task compile d2b031e48e7f491a3d05a12f243f967517b25236
      def compile(c_file: File):
          """
          Compile one C file into an object file.
          """
          os.system("gcc -c {c_file}".format(c_file=c_file.path))
          return File(c_file.path.replace('.c', '.o'))
```

We can also walk the CallGraph using SQLAlechemy. The command `redun repl` gives us an interactive read-eval-print-loop (REPL). There is a builtin function `query()` that can retrieve any object by an id prefix:

```
redun repl

(redun) call_node = query('bab7987a')
(redun) call_node
CallNode(hash='bab7987a', task_name='link', args=['prog2', [File(path=prog2.o, hash=60e8a5e97fe8b6064d0655c1eee16ff6dd650946), File(path=lib.o, h...])

(redun) call_node.value
Value(hash='62a3a615', value=File(path=prog2, hash=62a3a615ed8e6f5265f76f3603df8f58790ba1d6))

(redun) call_node.arguments
[Argument(task_name='link', pos=0, value=prog2), Argument(task_name='link', pos=1, value=[File(path=prog2.o, hash=60e8a5e97fe8b6064d0655c1eee16ff6dd650946), File(path=lib.o, hash=6370c13da0a6ca01177f4b42b043d5cd860351d6)])]

(redun) call_node.parents
[CallNode(hash='4453e6cd', task_name='make_prog', args=['prog2', [File(path=prog2.c, hash=b262cdb5130e8984ed3bb0dba8c83e56173ed702), File(path=lib.c, h...])]

(redun) call_node.task
Task(hash='e73b3cd2', name='link')

(redun) print(call_node.task.source)
def link(prog_path: str, o_files: List[File]):
    """
    Link several object files together into one program.
    """
    os.system("gcc -o {prog_path} {o_files}".format(
        prog_path=prog_path,
        o_files=' '.join(o_file.path for o_file in o_files),
    ))
    return File(prog_path)

(redun) call_node.parents[0]
CallNode(hash='4453e6cd', task_name='make_prog', args=['prog2', [File(path=prog2.c, hash=b262cdb5130e8984ed3bb0dba8c83e56173ed702), File(path=lib.c, h...])

(redun) print(call_node.parents[0].task.source)
def make_prog(prog_path: str, c_files: List[File]):
    """
    Compile one program from its source C files.
    """
    o_files = [
        compile(c_file)
        for c_file in c_files
    ]
    prog_file = link(prog_path, o_files)
    return prog_file
```


### Syncing provenance data

The redun database (stored in `.redun/redun.db` by default) is designed to be retained for long term storage and analysis. To aid this, the redun CLI provides several commands to make backing up and syncing databases easy.

#### Repositories
Redun organizes data provenance into repositories ("repos" for short) similar to how git organizes code in repos. By default, all provenance is tracked in the default, nameless repository.

Additional named repositories may be added with `redun repo add <repo name> <config dir>`. The configuration for the repository may be local or on S3.

Available repositories can be enumerated with `redun repo list`.

Removing repositories via the command-line is not currently supported, but `redun repo remove <repo name>` will instruct you on how to edit your config file.

#### Running redun in other repositories

To run redun commands in a different database, `redun --repo <repo name> <command>` will work for most commands. For example, `redun --repo foo log --task` will print tasks present in the "foo" repository.

#### Synchronizing repositories

Just like git, `redun pull <repo>` and `redun push <repo>` will synchronize data to and from remote repositories.

You can also sync from remote to remote: `redun --repo foo push bar` will push all records from repository "foo" to "bar". The number of records pushed will be displayed.

#### Manual exports
First, the database can be exported to a JSON log format using the `redun export` command:

```
redun export

{"_version": 1, "_type": "Value", "value_hash": "60e8a5e97fe8b6064d0655c1eee16ff6dd650946", "type": "redun.File", "format": "application/python-pickle", "value": "gANjcmVkdW4uZmlsZQpGaWxlCnEAKYFxAX1xAihYBAAAAHBhdGhxA1gHAAAAcHJvZzIub3EEWAQAAABoYXNocQVYKAAAADYwZThhNWU5N2ZlOGI2MDY0ZDA2NTVjMWVlZTE2ZmY2ZGQ2NTA5NDZxBnViLg==", "subvalues": [], "file_path": "prog2.o"}
{"_version": 1, "_type": "Task", "task_hash": "e73b3cd20246b559ac2b9c2933efe953612cc3ab", "name": "link", "namespace": "", "source": "def link(prog_path: str, o_files: List[File]):\n    \"\"\"\n    Link several object files together into one program.\n    \"\"\"\n    print(\"linking\")\n    os.system(\"gcc -o {prog_path} {o_files}\".format(\n        prog_path=prog_path,\n        o_files=' '.join(o_file.path for o_file in o_files),\n    ))\n    return File(prog_path)\n"}
{"_version": 1, "_type": "CallNode", "call_hash": "1dc10a1cbf5696b00d1b6f3e9da2473ba088e033", "task_name": "compile", "task_hash": "d2b031e48e7f491a3d05a12f243f967517b25236", "args_hash": "df25928d0785108768a019cdb8f856c5013c97ec", "value_hash": "60e8a5e97fe8b6064d0655c1eee16ff6dd650946", "timestamp": "2020-06-27 05:15:07.464279", "args": {"0": {"arg_hash": "16ce4470a4242d1c246d1c15a00487d73ce24efd", "value_hash": "b262cdb5130e8984ed3bb0dba8c83e56173ed702", "upstream": []}}, "children": []}
{"_version": 1, "_type": "Job", "id": "94830f60-acaa-4b6b-9b22-f4572c64f85b", "start_time": "2020-06-26 22:21:32.266311", "end_time": "2020-06-26 22:21:32.283055", "task_hash": "d2b031e48e7f491a3d05a12f243f967517b25236", "cached": true, "call_hash": "1dc10a1cbf5696b00d1b6f3e9da2473ba088e033", "parent_id": "e0109842-8a69-4a4c-b2c6-9b97bfd35b43", "children": []}
{"_version": 1, "_type": "Execution", "id": "6393500c-313c-43d7-87d4-d6feaeefaaab", "args": "[\"/Users/rasmus/projects/redun.dev/.venv/bin/redun\", \"run\", \"make.py\", \"make\", \"--config\", \"12\"]", "job_id": "e060d21c-108e-4cfb-ad11-04e19ae4d460"}
```

These JSON logs can be imported into another redun database using `redun import`.

```
# Export logs.
redun export > logs.json

wc -l logs.json
# 99

# Create a new redun database.
redun --config redun2 init

# Second database should be empty initially.
redun --config redun2 export | wc -l
# 0

# Import logs into second database.
redun --config redun2 import < logs.json

# Second database is now populated.
redun --config redun2 export | wc -l
# 99
```

The import command is [idempotent](https://en.wikipedia.org/wiki/Idempotence), so you can safely import log files that might contain records that already exist in the database, and redun will correctly only import the new records and avoid double insertions. Since records are immutable, there is no need to perform any updates or deletes, only inserts. Every record has a unique id that is either a hash or UUID, and therefore reconciling matching records is trivial.

redun can also efficiently sync new records using the `push` command, which works similar to `git push`:

```sh
# Push to another repo.
redun repo add other_repo redun2
redun push other_repo
```

This efficient sync is done by performing a graph walk of the redun database, similar to the [approach used by git](https://matthew-brett.github.io/curious-git/git_push_algorithm.html).

It is envisioned that the `push` command can be used in a data science workflow, where a data scientist could complete their analysis by pushing both their code provenance (version control) and their data provenance to central databases to share with their team.

```sh
# Share your code provenance.
git push origin main

# Share your data provenance.
redun push prod
```

## Advanced topics

### Working with databases and APIs

It's common to use workflow engines to implement Extract Transform Load (ETL) pipelines which help move data between data stores such as databases, data lakes, and other services. Inevitably, we would need to work with ephemeral objects such as database connections and API clients that would have difficulties with caching and re-execution without providing special treatment. For example:

- Database connections or API clients might have internal state you don't want serialized, such as secrets or overly specific configuration information (IP addresses, port numbers).
- Connections would also have state such as socket file descriptors that won't be valid when deserialized later on.
- With files, we were able to double check if their current state was consistent with our cache by hashing them. With a database or API, it's typically not feasible to hash a whole database. Is there something else we could do?
- The redun cache contains cached results from all previous runs. Conveniently, that allows for fast reverting to old results if code or input data is changed back to old state. However, for a stateful system like a database, we likely can't just re-execute arbitrary tasks in any order. Similar to database migration frameworks (South, Alembic, etc), we may need to rollback past tasks before applying new ones.

redun provides solutions to several of these challenges using a concept called Handles.


### Handles for ephemeral and stateful values

We'll introduce the features of Handles in several steps. The first feature that Handles provides is control over the data used for serialization and a mechanism for recreating ephemeral state after deserialization. Let's say we wanted to pass a database connection between tasks in a safe way. We could use a Handle class to do the following:

```py
from redun import task, Handle

class DbHandle(Handle):
    def __init__(self, name, db_file):
        self.db_file = db_file
        self.instance = sqlite3.connect(db_file)

@task()
def extract(conn):
    data = conn.execute('select * from my_table').fetchmany()
    return data

@task()
def transform(data):
    # result = transform data in some way...
    return result

@task()
def load(conn, data):
    conn.executemany('insert into another_table values(?, ?, ?)', data)
    return conn

@task()
def main():
    # Two databases we want to move data between
    src_conn = DbHandle('src_conn', 'src_data.db')
    dest_conn = DbHandle('dest_conn', 'dest_data.db')

    data = extract(src_conn)
    data2 = transform(data)
    dest_conn2 = load(dest_conn, data2)
    return dest_conn2
```

First, we use the `DbHandle` class to wrap a sqlite database connection. When Handles are serialized, only the arguments to their constructor are serialized. Therefore, you can define as much internal state as you wish for a Handle and it will not get caught up in the cache. redun's default serialization is Python's pickle framework. pickle uses the `__new__()` constructor and `__setstate__` (or `__dict__`) to directly set the state of an object, thus avoiding calling the `__init__` initializer. Handles modify this behavior in order to execute `__init__` even when deserializing. Therefore, we can use the constructor to reestablish ephemeral state such as the database connection.

Since Handles are often used to wrap another object, such as a connection, Handles provide a convenient mechanism for reducing boiler plate code. If a Handle defines a `self.instance` attribute, all attribute access to the Handle is automatically proxied to the `self.instance`. Above the `conn.execute()` and `conn.executemany()` calls make use of the attribute proxy.

#### State tracking

Now let's take a closer look at the `load()` task. It uses a handle, `dest_conn`, as an input and output in order to perform a write. Ideally, we would like to represent that `dest_conn` is updated by the `load` task, so that we can correctly determine which tasks need to be re-executed when input data or code changes.

As we stated before, hashing an entire database just to represent the database state is often not feasible, either because there is too much data to rehash frequently, or the database state contains too much ancillary data to hash consistently. As an alternative to hashing the whole database, we could just hash the sequence of operations (i.e. Tasks) we have applied to database Handle. This is the strategy redun takes.

Specifically, as a handle passes into or out of a Task, the redun scheduler, duplicates the Handle, and incorporates into its hash information specific to the task call, such as the hash of the corresponding call node. Therefore, in the example above, if we look at these specific lines:

```py
@task()
def load(conn_i, data):
    conn.executemany('insert into another_table values(?, ?, ?)', data)
    return conn_i

@task()
def main():
    # SNIP...
    dest_conn2 = load(dest_conn, data2)
    # SNIP ...
```

the handles `dest_conn`, `conn_i`, and `dest_conn2` are all distinct and have their own hashes. redun also records the lineage of such handles into a Handle Graph which represents the state transitions the handle has gone through:

```
dest_conn* --> conn_i* --> dest_conn2*
```

redun also records whether each handle state is currently valid or not, which we indicate above using `*` for valid handles. When re-executing a workflow, if a previously cached handle state is still valid we can safely fast forward through the corresponding task.

Now, lets consider what happens if we added an additional load step and re-executed this workflow:

```py
@task()
def load(conn_i, data):
    # Write data into conn_i...
    return conn_i

@task()
def load2(conn_ii, data):
    # Write date into conn_ii...
    return conn_ii

@task()
def main():
    # Two databases we want to move data between
    src_conn = DbHandle('src_conn', 'src_data.db')
    dest_conn = DbHandle('dest_conn', 'dest_data.db')

    data = extract(src_conn)
    data2 = transform(data)
    data3 = transform2(data)
    dest_conn2 = load(dest_conn, data2)
    dest_conn3 = load2(dest_conn2, data3)
    return dest_conn3
```

redun would notice that it can forward through `extract`, `transform`, and `load`, and would only execute `transform2`, and `load2`. The Handle Graph would be updated with:

```
dest_conn* --> conn_i* --> dest_conn2* --> conn_ii* --> dest_conn3*
```

Now consider what happens if we decided to change the code of task `load2()`. redun would fast forward through much of the workflow but would re-execute `load2()`. In doing so it would branch the Handle Graph, since `load2()` would contribute a different call node hash to the `dest_conn3`. Let's consider this new Handle `dest_conn3b`, and the Handle Graph would be:

```
dest_conn* --> conn_i* --> dest_conn2* --> conn_ii* --> dest_conn3
                                                  \
                                                   \--> dest_conn3b*
```

The old handle state `dest_conn3` is marked as invalid. What's important about this bookkeeping is that if we revert the change to `load2()`, it is not appropriate to just fast revert to dest_conn3. We must re-execute `load2()` again to update the database to be at state `dest_conn3` (instead of `dest_conn3b`).

#### More advance Handle uses

*This is currently explained very briefly*

Handles can be forked into instances that can be processed in parallel (parallel loading into a database). Forking might also be done to denote that there is no dependency between the tasks and limit the scope of rollbacks (i.e. Handle invalidation).

```py
from redun import merge_handles

# SNIP ...

@task()
def main():
    conn = DbHandle('conn', 'data.db')
    conn2a = load_a(conn)
    conn2b = load_b(conn)
    conn3 = merge_handles([conn2a, conn2b])
    conn4 = load_c(conn3)
    return conn4
```

Handles can also be merged to denote that the previous parallel steps must complete before continuing to the next step. The Handle Graph for the above code would be:

```
conn* --> conn_i* ---> conn2a* ----> conn3* --> conn_iii* --> conn4*
    \                            /
     \--> conn_ii* --> conn2b* -/
```

Using the above Handle Graph, redun can determine that updating the code for `load_b()` should trigger re-execution for `load_b()` and `load_c()`, but not `load_a()`.

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

## Implementation notes

### Evaluation process

redun's evaluation process can be summarized with the following pseudo code:

```py
# Top-level evaluation of an expression.
evaluate(expr)

def evaluate(expr):
   """
   Evaluate an expression.
   """
   if expr is a TaskExpression:
       return evaluate_task(expr.task, expr.args)
   elif expr is a SimpleExpression:
       return eval_simple_expression(expr.func, expr.args)
   elif expr is a nested value:
       return map_nested_value(evaluate, expr)
   else:
       # expr is a concrete value, return it as is.
       return expr

def evaluate_task(task, args):
    """
    Evaluate a Task.
    """
    # Evaluate arguments.
    eval_args = evaluate(args)
    preprocessed_args = preprocess_args(eval_args)

    # Use cache to fast-forward if possible.
    args_hash = hash_arguments(preprocess_args)
    eval_hash = hash_eval(task.hash, args_hash)
    postprocessed_result = get_cache(eval_hash, task.hash, args_hash, check_valid=task.check_valid)
    if postprocessed_result is Nothing:
        perform_rollbacks(preprocess_args)

        # Evaluate task.
        eval_result = executor_submit(task, preprocessed_args)
        postprocessed_result = postprocess_result(eval_result)
        set_cache(eval_hash, postprocessed_result)

    # Evaluate result.
    # This is needed because lazy evaluation will return an expression
    # that needs further evaluation.
    result = evaluate(postprocessed_result)

    child_call_hashes = [
        call_node.call_hash
        for call_node in CallNodes created in evaluate(postprocess_result)
    ]
    call_node = record_call_node(
        task,
        eval_args,
        result,
        child_call_hashes,
    )
    return result

def get_cache(eval_hash, task_hash, args_hash, check_valid):
    """
    Consult key-value store for cached results.
    """
    if check_valid == 'full':
        cache_result = backend.get_eval_cache(eval_hash)
    elif check_valid == 'shallow':
        cache_result = backend.get_cache(task_hash, args_hash)
    else:
        raise NotImplementedError()

    if is_valid_value(cache_result):
        return cache_result
    else:
        return Nothing
```


### Content addressable hashing summary

redun uses hashing to create deterministic ids for many records in the redun data model. The design of this hashing scheme has several properties:

- Every hash is unique across all records.
  - While a cryptographically secure hash function can be assumed to not produce hash collisions, we can still have collisions on pre-image data if we are not careful. For example, a CallNode could have the same hash as a file that contains a pickle of a CallNode, if the hashing schema is not carefully designed.
  - To achieve this property, every pre-image is prefixed with a record type, such as 'Task', 'CallNode', etc.
  - It is also easy to have pre-image collisions if arguments to a hash are not well escaped (e.g. if tab is used as an argument delimiter, you need to always escape tabs in string arguments). We solve this systematically by defining a consistent way of efficiently serializing structures using bittorrent encoding format (bencode).
- When one parental record refers to a child record, the child record's hash is included in the parent's hash.
  - This forms a [Merkle Tree](https://en.wikipedia.org/wiki/Merkle_tree) that gives unique hashes to nodes in the resulting DAG.
- Hashes are represented by hex strings instead of bytes to aide easy debugging and display.
  - If greater hashing efficiency is desired, hashes could be kept in their binary byte representation.
- Hashes are truncated to 40 hex bytes for easier display.
  - If the reduced hash space is a concern this truncation could be removed.

Here, is a summary of how they are computed and related to each other:

```py
# Basic functions.
blob_hash(bytes) = hex(sha512(bytes))[:40]
hash_struct(struct) = blob_hash(bencode(struct))

value_hash(value) = hash_struct(['Value', blob_hash(serialize(value))])
# alt: value_hash(value) = blob_hash(serialize(value))

task_hash(task) = {
  hash_struct(['Task', task.fullname, 'version', task.version]) if task.version defined,
  hash_struct(['Task', task.fullname, 'source', task.source]) otherwise
}

args_hash(args, kwargs) = hash_struct([
    'TaskArguments',
    [value_hash(arg) for arg in args],
    {key: value_hash(arg) for key, arg in kwargs.items()},
])

call_hash(job) = hash_struct([
    'CallNode',
    task_hash(job.task),
    args_hash(job.args, job.kwargs),
    value_hash(job.result),  # result_hash
    [
       call_hash(child)
       for child in job.child_jobs
    ]  # child_call_hashes
])

arg_hash(value, i, key, job) = hash_struct([
    'Argument', call_hash(job), str(i), str(key), value_hash(value)
])

eval_hash(job) = hash_struct(['Eval', task_hash(job.task), args_hash(job.args, job.kwargs)])

file_hash(file) = {
    hash_struct(['File', 'local', file.path, file.size, str(file.mtime)]) if file.proto == 'local',
    hash_struct(['File', 's3', file.path, file.etag]) if file.proto =='s3'
}

fileset_hash(fileset) = hash_struct(['FileSet'] + sorted(file.hash for file in fileset))

dir_hash(dir) = hash_struct(['Dir'] + sorted(file.hash for file in fileset))

handle_hash(handle) = {
    hash_struct(['Handle', handle.fullname, 'call_hash', handle.key, handle.call_hash]) if handle.call_hash,
    hash_struct(['Handle', handle.fullname, 'init', handle.key, handle.args, handle.kwargs]) otherwise
}
```


### Connections to functional programming

redun's use of task decorators and lazy-evaluation can be thought of as a [monad](https://en.wikipedia.org/wiki/Monad_(functional_programming)). The redun monad consists of three components:

- A type constructor `M`:
  - redun uses `Expression` to wrap user values (type `a`).
- A type converter `unit :: a -> M a`:
  - redun uses `ExpressionValue(a)` to lift a user value into the monad. This is conceptually done automatically for the user.
- A combinator `bind :: M a -> (a -> M b) -> M b`:
  - The `@task()` decorator plays the role of `bind` although with the arguments reversed.

Let's look at the following example:

```py
@task()
def task1(x: int):
    return 2 * x

@task()
def task2(y: int):
    return task1(y + 1)
```

- The undecorated function `task2` is a monadic function and has type `a -> M b`, where `a` and `b` are `int`. The return type is `M b` because `task(y + 1)` will return an `Expression` (`M`) wrapping an int (`b`).
- We can think of decorator `@task` as performing currying.
  - `task2` has type `a -> M b`.
  - `task(task2)` transforms the function to have type `M a -> M b`, since decorated tasks can accept `Expressions` as arguments. For arguments that are concrete user values (type `a`), redun automatically wraps the arguments with `ValueExpression`, at least conceptually.
  - If `task(task2)(ValueExpression(10))` is viewed as currying, we could write this as `task(task2, ValueExpression(10))` and `task` has type `(a -> M b) -> M a -> M b`. This is very close to `bind` except with arguments reversed.

By using monads to wrap the computation, redun can analyze the dataflow between tasks and perform proper parallelization and scheduling transparently. By design, redun tries to leverage the benefits of monads and functional programming concepts, but in a style familiar to Python developers.

This section from the [monad Wikipedia page](https://en.wikipedia.org/wiki/Monad_(functional_programming)) provides a good summary of why the monad is a natural fit for defining a workflow:

> The value of the monad pattern goes beyond merely condensing code and providing a link to mathematical reasoning. Whatever language or default programming paradigm a developer uses, following the monad pattern brings many of the benefits of purely functional programming. By reifying a specific kind of computation, a monad not only encapsulates the tedious details of that computational pattern, but it does so in a declarative way, improving the code's clarity. As monadic values explicitly represent not only computed values, but computed effects, a monadic expression can be substituted with its value in referentially transparent positions, much like pure expressions can be, allowing for many techniques and optimizations based on rewriting.[5]

In our use, "reifying" means wrapping the computation into an expression tree that our scheduler can analyze for deciding which tasks can be executed safely in parallel, which tasks can be skipped due to caching, etc. Generating the expression tree is what makes redun *declarative*, even though we do execute imperative code to generate the expression tree.


### redun: a language within

redun can be thought of as implementing a asynchronous functional programming language within Python, which is an imperative multi-paradigm language, a general pattern called [metaprogramming](https://en.wikipedia.org/wiki/Metaprogramming). This strategy for implementing a workflow engine allows us to use a very popular programming language, Python (used in data science, ML, web development, ETLs), but adapt it to have language features amenable to distributed computing. The following features map between Python and redun-the-language:

- `Task`s are the functions of redun.
- Tasks are [first-class values](https://en.wikipedia.org/wiki/First-class_function) in redun and therefore can be passed as arguments and returned as results from redun functions.
- Task input arguments and outputs are the values of redun.
  - In fact, the base class for classes like `File`, `Handle`, and `Task` is `Value`.
- The `Scheduler` is the interpreter and runtime of redun.
- `Expression` implements expressions in the redun language. The `Scheduler` recursively evaluates `Expression`s into concrete values.
- The `TaskRegistry` and `TypeRegistry` are the environment (described in [Scope](https://en.wikipedia.org/wiki/Scope_(computer_science)#Overview)) for redun.
- Plain Python functions that take `Expression`s are like [macros](https://en.wikipedia.org/wiki/Macro_(computer_science)#Syntactic_macros) since they are evaluated at "compile-time" (i.e. workflow construction-time).
- `scheduler_task`s are the equivalent of [fexpr](https://en.wikipedia.org/wiki/Fexpr) and can be used to implement [special forms](https://groups.csail.mit.edu/mac/ftpdir/scheme-7.4/doc-html/scheme_3.html) in redun.
  - One could implement if-expressions (`cond`), try-except (`catch`), and `seq`.
- `PartialTask` allows the creation of closures for redun, since they do not evaluate their arguments until all other arguments are supplied.
  - `PartialTask`s can be used to implement `delay` and `force`.


## Influences

Development of redun has been inspired by many projects. Here is a brief review of our understanding of how redun's ideas relate to similar concepts seen elsewhere.

- Bioinformatic workflow languages: [WDL](https://github.com/openwdl/wdl), [Nextflow](https://www.nextflow.io/), [Snakemake](https://snakemake.readthedocs.io/en/stable/), [Reflow](https://github.com/grailbio/reflow)
  - Workflow languages such as these give special treatment to file and code change reactivity, which is especially helpful in scientific workflows that typically go through quick interactive development. redun's `File` and task hashing were inspired by these languages.
  - Bioinformatic workflow languages are most commonly used to wrap unix programs that expect their input and output files to be local. Accordingly, these languages typically provide a specific syntax to help copy files from cloud storage to local disk and back, a process sometimes called staging or localization. This behavior inspired redun's `File(remote_path).stage(local_path)` syntax. The need to make frequent calls to unix programs inspired the `script()` task.
  - They also all define a Domain Specific Language (DSL) in order to enforce pure functions or provide dedicated syntax for task dependency. redun differs by relying on a host language, Python. Using Python makes it difficult to enforce some of the same constraints (e.g. function purity), but it does allow redun workflows to more easily integrate with the whole world of Python data science libraries (e.g. Pandas, NumPy, pytorch, rdkit, sqlalchemy, etc) without layers of wrapping (e.g. driver scripts, data serializing/deserializing, Docker images).
- Data engineering workflow languages: [Airflow](https://airflow.apache.org/), [Luigi](https://github.com/spotify/luigi), [Prefect](https://www.prefect.io/)
  - These workflow languages have had a more general focus beyond interacting with files. They are often used for ETLs (extract-transform-load) where data is exchanged with databases or web services. redun does not assume all dataflow is through files and so can used for interacting with databases and APIs as well.
  - Airflow v2, Prefect, and [Dask delayed](https://docs.dask.org/en/latest/delayed.html) have adopted defining tasks as decorated Python functions, which when invoked return a Promise / Future / Delayed that can be passed as an argument to another task call. The workflow engine can then inspect the presence of these lazy objects in task call arguments in order to infer the edges of the workflow DAG. This allows very natural looking workflow definitions. redun uses the same approach but returns Expressions instead, which differ from other lazy values in several ways. First, the evaluation of an Expression can change depending on what larger Expression it belongs to, such `cond()` or `catch()`. Second, Expressions are free to return more Expressions for evaluation. Third, redun will evaluate Expressions inside of Nested Values (list, dict, set, tuple, NamedTuple) by default.
- Expression hashing: [Unison](https://www.unisonweb.org/), [gg](https://github.com/StanfordSNR/gg)
  - In Unison, every expression is hashable and the hash can be used as a cache key for replaying reductions / evaluations from a cache. This is similar to how redun represents a workflow as a graph of Expressions, each of which is hashed in order to consult a central cache (Value Store).
  - In gg, a workflow is defined as a graph of thunks (redun's TaskExpression) which is evaluated using graph reduction. Each reduction is performed on a remote execution system such as AWS Lambda. redun is very similar, but also allows evaluating each Expression on a different Executor (e.g. threads, processes, Batch jobs, Spark jobs, etc).
- Functional programming languages: [Haskell](https://www.haskell.org/) and [Lisp](https://en.wikipedia.org/wiki/Lisp_(programming_language))
  - redun's Scheduler can be seen as an interpreter for a [functional programming language](#redun-a-language-within). Many of the builtin tasks take their name from similar functions in Haskell and Lisp: `map_`, `flat_map`, `seq`, `catch`, `cond`, `delay`, `force`, `@scheduler_task` (a.k.a. [special forms](https://www.cs.cmu.edu/Groups/AI/html/cltl/clm/node59.html) and [fexpr](https://en.wikipedia.org/wiki/Fexpr)).
  - redun is like Lisp in that higher level constructs like `catch` and `cond` are defined using lower-level features like `@scheduler_task`. This allows users to extend the workflow language similar to extending Lisp using macros.
- Build systems: [redo](https://redo.readthedocs.io/en/latest/), [Build systems a la Carte](https://dl.acm.org/doi/10.1145/3236774)
  - redo shows how the build steps ("tasks" in redun's terminology) can be arbitrary code that does not require static analysis of its dependencies (child task calls). Instead, when running a workflow for the first time you have to run everything anyways, so just run it. However, if one carefully records what was run (i.e. the data provenance Call Graph in redun) then you can consult the recording in future executions to decide what needs to be incrementally re-executed.
- Observability, distributed tracing: [Spans](https://opentelemetry.lightstep.com/spans/)
  - Span trees play a similar role to redun's Call Graph. Span attributes are equivalent to redun's tag system.
  - redun's Call Graphs should provide similar observability and debugging capabilities as span trees. One difference is that Call Graphs are intended to be retained long term in order to provide data provenance. In some ways, observability is like short-term data provenance.
  - redun's Call Graph gives more attention to dataflow between Jobs than Spans typically do.
- Source control: [git](https://git-scm.com/)
  - git provided several inspirations to redun. First, it is easy to get started with git, because users can simply run `git init` to create a new local repo in the hidden directory `.git/`. Central configuration and coordination is not needed. As users change their code, they record each change using commands like `git add` and `git commit`. This can be seen as a form of provenance but for code, and the [commit graph](https://krishnabiradar.com/blogs/deconstructing-a-git-commit/) is git's data structure for storing this provenance information. The commit graph is a [Merkle tree](https://en.wikipedia.org/wiki/Merkle_tree) that uses hashes to give unique ids to each node in the graph and this enables efficient peer-to-peer syncing of commit graphs between repos using the `git push` and `git pull` commands. git provides many additional tools, such as `git log` and `git diff`, for inspecting commit graphs to help with the code development process.
  - To relate this to redun, redun also creates a local repo in `.redun/` on the first workflow run or with a `redun init` command. New data provenance is recorded each time users use the `redun run` command. These recordings are stored in the Call Graph data structure, which is also a Merkle tree. Users can sync Call Graphs efficiently using the `redun push` and `redun pull` commands. redun also provides tools for using Call Graphs in the development process, such as exploring past executions (`redun log`), figuring out the difference between two executions (`redun diff`), and interactively exploring Call Graphs and data (`redun repl`).
- ML frameworks: [TensorFlow](https://www.tensorflow.org/), [PyTorch](https://pytorch.org/)
  - ML frameworks can be thought of as workflow engines for tensors. They face similar issues for reifying computation so that they can automatically parallelize, distribute, and differentiate the compute described by a network. TensorFlow's original Graph API required the network to be constructed upfront as a static directed acyclic graph (DAG), while TensorFlow and PyTorch's eager execution APIs allow in-line compute graph definition during forward pass. In comparison, redun operates between these two extremes. In redun, task calls are always lazy, which produces an upfront graph of compute (an expression graph). However, tasks can return additional expressions, thus allowing the user to interleave static DAGs with arbitrary Python code. This allows redun to express very dynamic compute without the downsides of fully eager execution: constant access to a central scheduler, difficult to define checkpoints, data provenance gaps, etc.
  - Redun's data provenance graph is also related to the tape-based automatic differentiation (often called autograd) in [Pytorch](https://pytorch.org/tutorials/beginner/former_torchies/autograd_tutorial.html) and [TensorFlow](https://www.tensorflow.org/api_docs/python/tf/GradientTape). Specifically, redun performs call graph recording, which tracks the lineage of task executions and emitted values, similar to the recoding of mathematical operators applied to the input during the forward pass of compute graphs in PyTorch/TensorFlow. The recorded redun call graph can then be recursively queried to examine the lineage of an output value, analogous to how ML frameworks recurse over the gradient tape to calculate derivatives of a compute graph's execution.
- Database migration frameworks: [Alembic](https://alembic.sqlalchemy.org/), [Django migrations](https://docs.djangoproject.com/en/3.2/topics/migrations/)
  - Migration frameworks are used to alter databases (both schemas and data) in a controlled manner. Frameworks, such as Alembic and Django migrations, encourage users to represent their migrations as atomic scripts arranged into a dependency DAG. The frameworks then provide command-line tools for upgrading and downgrading the database to a specific version node within the migration DAG. Such tools then execute the necessary migrations in topological sort order. In order to not apply the same migration twice, these frameworks often maintain a metadata table within the database. In a way, these frameworks are workflow engines with the same usual features of task dependency DAGs and caching logic.
  - One difference though, is that migration frameworks also encourage users to define an inverse for each "task", that is each upgrade migration has a corresponding downgrade. When given a desired migration node version, these CLI tools may apply a combination of upgrade and downgrade migrations to move the database along a path in the migration DAG.
  - redun has an experimental feature called `Handle` for defining interactions with stateful services such as databases. redun must record similar metadata about the past states of the Handle and it is envisioned that inverse tasks, or rollbacks, could be defined and executed as needed.
- Causal hashing: [Koji](https://arxiv.org/abs/1901.01908)
  - The Koji paper describes a way of using causal hashing to unify both file and web service use within a single workflow engine system. redun also aims for similar unification and provides a special Value type, Handle, for interacting with external services, such as databases and APIs. Handles solve the problem of giving a unique id to the state of an external system. When the external data source is a simple file, we can hash the file to give its state a unique id. However, when the external data source is a database, it is likely unreasonable to hash a whole database or web service. Instead, a causal hash can be defined as hashing the series of Call Nodes a Handle has passed through. This often gives an equally unique id for the state of an external service, while being more efficient.
