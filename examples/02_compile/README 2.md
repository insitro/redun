# Working with Files: A workflow for compiling

In the [previous example](../01_hello_world/README.md), we saw some of redun's basic features (e.g. caching, argument parsing, incremental compute, and data provenance) on a simple workflow. Here, we will start to look at a slightly larger workflow that involves file reading and writing. Consider the workflow [make.py](make.py), which shows a simple way of compiling two C programs and a common library:

```py
import os
from typing import Dict, List

from redun import task, File


redun_namespace = "redun.examples.compile"


@task()
def compile(c_file: File) -> File:
    """
    Compile one C file into an object file.
    """
    os.system("gcc -c {c_file}".format(c_file=c_file.path))
    return File(c_file.path.replace('.c', '.o'))


@task()
def link(prog_path: str, o_files: List[File]) -> File:
    """
    Link several object files together into one program.
    """
    os.system("gcc -o {prog_path} {o_files}".format(
        prog_path=prog_path,
        o_files=' '.join(o_file.path for o_file in o_files),
    ))
    return File(prog_path)


@task()
def make_prog(prog_path: str, c_files: List[File]) -> File:
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
def make(files: Dict[str, List[File]]=files) -> List[File]:
    """
    Top-level task for compiling all the programs in the project.
    """
    progs = [
        make_prog(prog_path, c_files)
        for prog_path, c_files in files.items()
    ]
    return progs
```

## Running the workflow

As we did in the first example, we can run the workflow using the `redun run` command:

```
redun run make.py make

[redun] redun :: version 0.4.15
[redun] config dir: /Users/rasmus/projects/redun/examples/compile/.redun
[redun] Upgrading db from version -1.0 to 2.0...
[redun] Start Execution 69c40fe5-c081-4ca6-b232-e56a0a679d42:  redun run make.py make
[redun] Run    Job 72bdb973:  redun.examples.compile.make(files={'prog': [File(path=prog.c, hash=dfa3aba7), File(path=lib.c, hash=a2e6cbd9)], 'prog2': [File(path=prog2.c, hash=c748e4c7), File(path=lib.c, hash=a2e6cbd9)]}) on default
[redun] Run    Job 096be12b:  redun.examples.compile.make_prog(prog_path='prog', c_files=[File(path=prog.c, hash=dfa3aba7), File(path=lib.c, hash=a2e6cbd9)]) on default
[redun] Run    Job 32ed5cf8:  redun.examples.compile.make_prog(prog_path='prog2', c_files=[File(path=prog2.c, hash=c748e4c7), File(path=lib.c, hash=a2e6cbd9)]) on default
[redun] Run    Job dfdd2ee2:  redun.examples.compile.compile(c_file=File(path=prog.c, hash=dfa3aba7)) on default
[redun] Run    Job 225f924d:  redun.examples.compile.compile(c_file=File(path=lib.c, hash=a2e6cbd9)) on default
[redun] Run    Job 3f9ea7ae:  redun.examples.compile.compile(c_file=File(path=prog2.c, hash=c748e4c7)) on default
[redun] Run    Job a8b21ec0:  redun.examples.compile.link(prog_path='prog', o_files=[File(path=prog.o, hash=4934098e), File(path=lib.o, hash=7caa7f9c)]) on default
[redun] Run    Job 5707a358:  redun.examples.compile.link(prog_path='prog2', o_files=[File(path=prog2.o, hash=cd0b6b7e), File(path=lib.o, hash=7caa7f9c)]) on default
[redun]
[redun] | JOB STATUS 2021/06/18 10:34:29
[redun] | TASK                             PENDING RUNNING  FAILED  CACHED    DONE   TOTAL
[redun] |
[redun] | ALL                                    0       0       0       0       8       8
[redun] | redun.examples.compile.compile         0       0       0       0       3       3
[redun] | redun.examples.compile.link            0       0       0       0       2       2
[redun] | redun.examples.compile.make            0       0       0       0       1       1
[redun] | redun.examples.compile.make_prog       0       0       0       0       2       2
[redun]
[File(path=prog, hash=a8d14a5e), File(path=prog2, hash=04bfff2f)]
```

This should have taken three C source files (`lib.c`, `prog.c`, and `prog2.c`), compiled them to three object files (`lib.o`, `prog.o`, `prog2.o`), and then linked them into two binaries (`prog` and `prog2`). The above execution assumes you have a C compiler, `gcc`, installed. We can test the binaries with the following commands:

```
./prog

prog1: Hello, World!
```

```
./prog2

prog2: Hello, World!
```


## Understanding the workflow code

The lowest-level tasks (`compile` and `link`) perform the necessary shell commands to compile and link C source code. Looking at `compile` we see the following:

```py
@task()
def compile(c_file: File) -> File:
    """
    Compile one C file into an object file.
    """
    os.system("gcc -c {c_file}".format(c_file=c_file.path))
    return File(c_file.path.replace('.c', '.o'))
```

One new feature we see is the use of a redun class called `File`. `File` contains an attribute called `path` that represents a filename (or URL such as `s3://bucket/myfile`). As we will see in a moment, the `File` class provides special features such file content reactivity, but for now its just wrapping a `path` string. The `link` task works in a similar fashion.

The next useful task to understand is `make_prog`, which looks like this:

```py
@task()
def make_prog(prog_path: str, c_files: List[File]) -> File:
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

Here, we see a few things. First, a list of C source files are all compiled using the `compile` task. As we will see soon, these compilations actually happen in parallel, even though the code above at first glance looks sequential. After compiling all the C files, we will have a list of object files (a typical step in compiling C programs), which we pass to the `link` task to produce a single binary, `prog_file`.

This is a first example of a fan-out and fan-in pattern, also called [map-reduce](https://en.wikipedia.org/wiki/MapReduce). Notice, there is no special syntax for performing parallelism nor for waiting for all upstream tasks to complete. In the third example, we will explain how redun is intercepting these task calls and returning lazy expressions to define workflows. There are special rules for how these lazy expressions are evaluated, which allow us to use fairly pythonic looking syntax, sometimes called "invocation-style".

Lastly, we have the top-level task, `make`, which builds upon the `make_prog` task to compile multiple programs:

```py
@task()
def make(files : Dict[str, List[File]]=files) -> List[File]:
    """
    Top-level task for compiling all the programs in the project.
    """
    progs = [
        make_prog(prog_path, c_files)
        for prog_path, c_files in files.items()
    ]
    return progs
```

Again, the list comprehension contains several calls to the lower-level task `make_prog` and these task calls will run in parallel automatically.

This task also highlights a design principle with redun. Namely, redun encourages building up large workflows in terms of smaller workflows, similar to how one in a normal program builds up high-level functions by hierarchically calling lower-level functions. This has several benefits. It leads to writing reusable tasks that can be used across workflows. It also allows creating workflows that are easier to reason about, because one only needs to understand a little bit of the dataflow and workflow logic at a time.

In case it helps, here is the dataflow DAG (directed acyclic graph) of the workflow:

<p align="center">
  <img width="400" src="images/compile-dag.svg">
</p>

## Data provenance

Like we saw in the first example, we can use `redun log -` to look at the data provenance of the most recent execution. Let's do that here:

```
redun log -

Exec 69c40fe5-c081-4ca6-b232-e56a0a679d42 [ DONE ] 2021-06-18 10:34:28:  run make.py make
Duration: 0:00:01.47

Jobs: 8 (DONE: 8, CACHED: 0, FAILED: 0)
--------------------------------------------------------------------------------
Job 72bdb973 [ DONE ] 2021-06-18 10:34:28:  redun.examples.compile.make(files={'prog': [File(path=prog.c, hash=dfa3aba7), File(path=lib.c, hash=a2e6cbd9)], 'prog2': [File(path=prog2.c, hash=c748e4c7), Fil
  Job 096be12b [ DONE ] 2021-06-18 10:34:28:  redun.examples.compile.make_prog('prog', [File(path=prog.c, hash=dfa3aba7), File(path=lib.c, hash=a2e6cbd9)])
    Job dfdd2ee2 [ DONE ] 2021-06-18 10:34:28:  redun.examples.compile.compile(File(path=prog.c, hash=dfa3aba7))
    Job 225f924d [ DONE ] 2021-06-18 10:34:28:  redun.examples.compile.compile(File(path=lib.c, hash=a2e6cbd9))
    Job a8b21ec0 [ DONE ] 2021-06-18 10:34:28:  redun.examples.compile.link('prog', [File(path=prog.o, hash=4934098e), File(path=lib.o, hash=7caa7f9c)])
  Job 32ed5cf8 [ DONE ] 2021-06-18 10:34:28:  redun.examples.compile.make_prog('prog2', [File(path=prog2.c, hash=c748e4c7), File(path=lib.c, hash=a2e6cbd9)])
    Job 3f9ea7ae [ DONE ] 2021-06-18 10:34:28:  redun.examples.compile.compile(File(path=prog2.c, hash=c748e4c7))
    Job 5707a358 [ DONE ] 2021-06-18 10:34:29:  redun.examples.compile.link('prog2', [File(path=prog2.o, hash=cd0b6b7e), File(path=lib.o, hash=7caa7f9c)])
```

Now, we are starting to see a more complex job tree, that has three levels. We can clearly see each compile and link step along with its arguments. One thing you might see is that the second program, `prog2`, did not need to compile `lib.c`, even though the workflow code makes a call to `compile(File('lib.c'))`. Intuitively, this is because the library `lib.c` was already compiled for the first program, `prog`. More concretely, in addition to using the cache to replay past return values, redun also performs [common subexpression elimination](https://en.wikipedia.org/wiki/Common_subexpression_elimination). That is, redun detects that a particular task is being called multiple times with the same arguments and will automatically run it only once. In this case, the result `File('lib.o')` was replayed for the second program.

## File reactivity

Reactivity refers to the ability of the scheduler to detect changes in a workflow (either in the code, the data, etc) and to automatically rerun the minimal parts of the workflow. Previously, we have seen two kinds of workflow reactivity, argument and code reactivity. Here, we will see a third, file reactivity, which is provided by the special class `File`.

Specifically, the `File` class defines it own hashing logic which is used when a `File` is passed as an argument to a task or returned as a return value from a task. By default, redun performs a fast pseudo hashing of a file by hashing the triplet (file path, byte size, modification time) (a technique used by other workflow engines, such as [Nextflow](https://www.nextflow.io/)).

Using your text editor, open the source file `lib.c`, which should look like:

```c
char *get_message() {
    return "Hello, World!\n";
}
```

and change the code to be more exciting!!!!!!

```c
char *get_message() {
    return "Hello, World!!!!!!\n";
}
```

Let's now run the workflow again:

```
redun run make.py make

[redun] redun :: version 0.4.15
[redun] config dir: /Users/rasmus/projects/redun/examples/compile/.redun
[redun] Start Execution 4a2b624d-b6c7-41cb-acca-ec440c2434db:  redun run make.py make
[redun] Run    Job 84d14769:  redun.examples.compile.make(files={'prog': [File(path=prog.c, hash=dfa3aba7), File(path=lib.c, hash=2b6a7ce0)], 'prog2': [File(path=prog2.c, hash=c748e4c7), File(path=lib.c, hash=2b6a7ce0)]}) on default
[redun] Run    Job 2f8b4b5f:  redun.examples.compile.make_prog(prog_path='prog', c_files=[File(path=prog.c, hash=dfa3aba7), File(path=lib.c, hash=2b6a7ce0)]) on default
[redun] Run    Job 4ae4eaf6:  redun.examples.compile.make_prog(prog_path='prog2', c_files=[File(path=prog2.c, hash=c748e4c7), File(path=lib.c, hash=2b6a7ce0)]) on default
[redun] Cached Job 049a0006:  redun.examples.compile.compile(c_file=File(path=prog.c, hash=dfa3aba7)) (eval_hash=434cbbfe)
[redun] Run    Job 0f8df953:  redun.examples.compile.compile(c_file=File(path=lib.c, hash=2b6a7ce0)) on default
[redun] Cached Job 98d24081:  redun.examples.compile.compile(c_file=File(path=prog2.c, hash=c748e4c7)) (eval_hash=96ab0a2b)
[redun] Run    Job 8c95f048:  redun.examples.compile.link(prog_path='prog', o_files=[File(path=prog.o, hash=4934098e), File(path=lib.o, hash=d90885ad)]) on default
[redun] Run    Job 9006bd19:  redun.examples.compile.link(prog_path='prog2', o_files=[File(path=prog2.o, hash=cd0b6b7e), File(path=lib.o, hash=d90885ad)]) on default
[redun]
[redun] | JOB STATUS 2021/06/18 11:41:43
[redun] | TASK                             PENDING RUNNING  FAILED  CACHED    DONE   TOTAL
[redun] |
[redun] | ALL                                    0       0       0       2       6       8
[redun] | redun.examples.compile.compile         0       0       0       2       1       3
[redun] | redun.examples.compile.link            0       0       0       0       2       2
[redun] | redun.examples.compile.make            0       0       0       0       1       1
[redun] | redun.examples.compile.make_prog       0       0       0       0       2       2
[redun]
[File(path=prog, hash=2f43c23c), File(path=prog2, hash=b4537ad7)]
```

Again, we see incremental compute in action. We see `lib.c` was recompiled, which forced `prog` and `prog2` to be relinked, since they are downstream tasks. We also see that the two other C files, `prog.c` and `prog2.c`, did not need to be recompiled. To verify that the programs were actually recompiled, we can run them again:

```
./prog

prog1: Hello, World!!!!!!
```

```
./prog2

prog2: Hello, World!!!!!!
```


## Data provenance for files

We can use the `--file` filter to show all Files touched by the workflow (i.e. used as a task argument or result).

```
redun log --file

File 2b6a7ce0 2021-06-18 11:41:42 r  lib.c
File d90885ad 2021-06-18 11:41:42 rw lib.o
File 2f43c23c 2021-06-18 11:41:42 w  prog
File dfa3aba7 2021-06-18 10:34:28 r  prog.c
File 4934098e 2021-06-18 10:34:28 rw prog.o
File b4537ad7 2021-06-18 11:41:42 w  prog2
File c748e4c7 2021-06-18 10:34:28 r  prog2.c
File cd0b6b7e 2021-06-18 10:34:28 rw prog2.o
```

Here, we see each file, and whether it was read, `r`, or written, `w`, by the workflow.

We can also look at the provenance of a single file, such as `prog`:

```
redun log prog

File 2f43c23c 2021-06-18 11:41:42 w  prog
Produced by Job 8c95f048

  Job 8c95f048-e60b-4486-bcf4-4422be265608 [ DONE ] 2021-06-18 11:41:42:  redun.examples.compile.link('prog', [File(path=prog.o, hash=4934098e), File(path=lib.o, hash=d90885ad)])
  Traceback: Exec 4a2b624d > (1 Job) > Job 2f8b4b5f make_prog > Job 8c95f048 link
  Duration: 0:00:00.24

    CallNode 6c56c8d472dc1d07cfd2634893043130b401dc84 redun.examples.compile.link
      Args:   'prog', [File(path=prog.o, hash=4934098e), File(path=lib.o, hash=d90885ad)]
      Result: File(path=prog, hash=2f43c23c)

    Task a20ef6dc2ab4ed89869514707f94fe18c15f8f66 redun.examples.compile.link

      def link(prog_path: str, o_files: List[File]) -> File:
          """
          Link several object files together into one program.
          """
          os.system("gcc -o {prog_path} {o_files}".format(
              prog_path=prog_path,
              o_files=' '.join(o_file.path for o_file in o_files),
          ))
          return File(prog_path)


    Upstream dataflow:

      result = File(path=prog, hash=2f43c23c)

      result <-- <6c56c8d4> link(prog_path, o_files)
        prog_path = <ee510692> 'prog'
        o_files   = <f1eaf150> [File(path=prog.o, hash=4934098e), File(path=lib.o, hash=d90885ad)]

      prog_path <-- argument of <a4ac4959> make_prog(prog_path, c_files)
                <-- origin

      o_files <-- derives from
        compile_result   = <d90885ad> File(path=lib.o, hash=d90885ad)
        compile_result_2 = <4934098e> File(path=prog.o, hash=4934098e)

      compile_result <-- <45054a8f> compile(c_file)
        c_file = <2b6a7ce0> File(path=lib.c, hash=2b6a7ce0)

      c_file <-- argument of <74cceb4e> make_prog(prog_path, c_files)
             <-- argument of <45400ab5> make(files)
             <-- origin

      compile_result_2 <-- <8d85cebc> compile(c_file_2)
        c_file_2 = <dfa3aba7> File(path=prog.c, hash=dfa3aba7)

      c_file_2 <-- argument of <a4ac4959> make_prog(prog_path, c_files)
               <-- argument of <a9a6af53> make(files)
               <-- origin
```

When given a file path (either local file or URL), redun will show the original Job and Task that created that file. Here, we see it was the `link` task that created the binary `prog`.

We also see the upstream dataflow, which is much more interesting this time. Think of the dataflow as a derivation. We start with a first value, `result`, which is our original file, `prog`. On the following lines, we see how it is derived. You can read `result <-- link(...)` as "result is derived from the task link". Below each derivation line, you will see an indented list of variable definitions (e.g. `prog_path` and `o_file`), which indicated what the arguments were for the `link` task. As new variables are introduced, their derivations are given as well. Overall, as you scroll down the dataflow description you are going further and further back in time of the workflow. As you can see, hashes are given for each Value and CallNode, which can be used for additional `redun log` commands to dig deeper.

Workflows are also reactive to outputs. If you delete an output file, you can rerun the workflow to regenerate it.

```
rm prog

redun run make.py make

[redun] redun :: version 0.4.15
[redun] config dir: /Users/rasmus/projects/redun/examples/compile/.redun
[redun] Start Execution e48c0b04-83b4-4931-808b-8731f4a6e222:  redun run make.py make
[redun] Cached Job 15828899:  redun.examples.compile.make(files={'prog': [File(path=prog.c, hash=dfa3aba7), File(path=lib.c, hash=2b6a7ce0)], 'prog2': [File(path=prog2.c, hash=c748e4c7), File(path=lib.c, hash=2b6a7ce0)]}) (eval_hash=c88a89ed)
[redun] Cached Job 507616dc:  redun.examples.compile.make_prog(prog_path='prog', c_files=[File(path=prog.c, hash=dfa3aba7), File(path=lib.c, hash=2b6a7ce0)]) (eval_hash=02b4da4a)
[redun] Cached Job 772d256d:  redun.examples.compile.make_prog(prog_path='prog2', c_files=[File(path=prog2.c, hash=c748e4c7), File(path=lib.c, hash=2b6a7ce0)]) (eval_hash=2d0bbfb1)
[redun] Cached Job a04e22db:  redun.examples.compile.compile(c_file=File(path=prog.c, hash=dfa3aba7)) (eval_hash=434cbbfe)
[redun] Cached Job b8750752:  redun.examples.compile.compile(c_file=File(path=lib.c, hash=2b6a7ce0)) (eval_hash=2077f91a)
[redun] Cached Job 623ea28a:  redun.examples.compile.compile(c_file=File(path=prog2.c, hash=c748e4c7)) (eval_hash=96ab0a2b)
[redun] Miss   Job 8dfa32fe:  Cached result is no longer valid (result=File(path=prog, hash=2f43c23c), eval_hash=9ebcd837).
[redun] Run    Job 8dfa32fe:  redun.examples.compile.link(prog_path='prog', o_files=[File(path=prog.o, hash=4934098e), File(path=lib.o, hash=d90885ad)]) on default
[redun] Cached Job 5907aa14:  redun.examples.compile.link(prog_path='prog2', o_files=[File(path=prog2.o, hash=cd0b6b7e), File(path=lib.o, hash=d90885ad)]) (eval_hash=a66c5f71)
[redun]
[redun] | JOB STATUS 2021/06/18 12:01:34
[redun] | TASK                             PENDING RUNNING  FAILED  CACHED    DONE   TOTAL
[redun] |
[redun] | ALL                                    0       0       0       7       1       8
[redun] | redun.examples.compile.compile         0       0       0       3       0       3
[redun] | redun.examples.compile.link            0       0       0       1       1       2
[redun] | redun.examples.compile.make            0       0       0       1       0       1
[redun] | redun.examples.compile.make_prog       0       0       0       2       0       2
[redun]
[File(path=prog, hash=10696249), File(path=prog2, hash=b4537ad7)]
```

Specifically, you can see that redun detected that a previous output file no longer has the same hash (due to being deleted).

```
[redun] Miss   Job 8dfa32fe:  Cached result is no longer valid (result=File(path=prog, hash=2f43c23c), eval_hash=9ebcd837).
[redun] Run    Job 8dfa32fe:  redun.examples.compile.link(prog_path='prog', o_files=[File(path=prog.o, hash=4934098e), File(path=lib.o, hash=d90885ad)]) on default
```


## Conclusion

In this example, we saw a larger workflow that was more hierarchical. We saw examples of automatic parallelism (fan-out and fan-in), as well as file reactivity. We also got to see the power of querying the data provenance for files and figuring out the data lineage of files via the call graphs.

In the next example, we will use a Jupyter notebook to more deeply understand how redun is using lazy expressions to achieve these powerful features.


## Bonus round

If you're interested, check out [make2.py](make2.py), which is another way of defining a Makefile-like workflow for any kind of shell command. It shows off how redun is general enough to mimic other other workflow engines like make and Snakemake. To try it out, use:

```sh
redun run make2.py make
```
