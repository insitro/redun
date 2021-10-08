# redun

*yet another redundant workflow engine*

redun aims to be a more expressive and efficient workflow framework, built on top of the Python programming language. It takes the somewhat contrarian view that writing dataflows directly is unnecessarily restrictive, and by doing so we lose abstractions we have come to rely on in most modern high-level languages (control flow, compositiblity, recursion, high order functions, etc). redun's key insight is that workflows can be expressed as lazy expressions, that are then evaluated by a scheduler which performs automatic parallelization, caching, and data provenance logging.

redun's key features are:

- Workflows are defined by lazy expressions that when evaluated emit dynamic directed acyclic graphs (DAGs), enabling complex data flows.
- Incremental computation that is reactive to both data changes as well as code changes.
- Workflow tasks can be executed on a variety of compute backend (threads, processes, AWS batch jobs, Spark jobs, etc). 
- Data changes are detected for in memory values as well as external data sources such as files and object stores using file hashing.
- Code changes are detected by hashing individual Python functions and comparing against historical call graph recordings (inspired by [redo](https://github.com/apenwarr/redo)).
- Past intermediate results are cached centrally and reused across workflows.
- Past call graphs can be used as a data lineage record and can be queried for debugging and auditing.

See the [docs](docs/source/design.md) and the [tutorial](examples/README.md) for more.

*About the name:* The name "redun" is self deprecating (there are [A LOT](https://github.com/pditommaso/awesome-pipeline) of workflow engines), but it is also a reference to its original inspiration, the [redo](https://apenwarr.ca/log/20101214) build system.

## Install

```sh
pip install redun
```

### Postgres backend

To use postgres as a backend, use

```sh
pip install redun[postgres]
```

The above assumes the following dependencies are installed:
* `pg_config` (in the `postgresql-devel` package; on ubuntu: `apt-get install libpq-dev`)
* `gcc` (on ubuntu or similar `sudo apt-get install gcc`)


## Small taste

Here is a quick example of using redun for a familar workflow, compiling a C program ([full example](examples/02_compile/README.md)). In general, any kind of data processing could be done within each task (e.g. reading and writing CSVs, DataFrames, databases, APIs).

```py
# make.py

import os
from typing import Dict, List

from redun import task, File


redun_namespace = "redun.examples.compile"


@task()
def compile(c_file: File) -> File:
    """
    Compile one C file into an object file.
    """
    os.system(f"gcc -c {c_file.path}")
    return File(c_file.path.replace('.c', '.o'))


@task()
def link(prog_path: str, o_files: List[File]) -> File:
    """
    Link several object files together into one program.
    """
    o_files=" ".join(o_file.path for o_file in o_files)
    os.system(f"gcc -o {prog_path} {o_files}")
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
    "prog": [
        File("prog.c"),
        File("lib.c"),
    ],
    "prog2": [
        File("prog2.c"),
        File("lib.c"),
    ],
}


@task()
def make(files : Dict[str, List[File]] = files) -> List[File]:
    """
    Top-level task for compiling all the programs in the project.
    """
    progs = [
        make_prog(prog_path, c_files)
        for prog_path, c_files in files.items()
    ]
    return progs
```

We can run the workflow using the `redun run` command:

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

This should have taken three C source files (`lib.c`, `prog.c`, and `prog2.c`), compiled them to three object files (`lib.o`, `prog.o`, `prog2.o`), and then linked them into two binaries (`prog` and `prog2`). Specifically, redun automatically determined the following dataflow DAG and performed the compiling and linking steps in separate threads:

<p align="center">
  <img width="400" src="examples/02_compile/images/compile-dag.svg">
</p>

Using the `redun log` command we can see the full job tree of the most recent execution (denoted `-`):

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

Using the `--file` option, we can see all files (or URLs) that were read, `r`, or written, `w`, by the workflow:

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

We can also look at the provenance of a single file, such as the binary `prog`:

```
redun log prog

File 2f43c23c 2021-06-18 11:41:42 w  prog
Produced by Job a8b21ec0

  Job a8b21ec0-e60b-4486-bcf4-4422be265608 [ DONE ] 2021-06-18 11:41:42:  redun.examples.compile.link('prog', [File(path=prog.o, hash=4934098e), File(path=lib.o, hash=d90885ad)])
  Traceback: Exec 4a2b624d > (1 Job) > Job 2f8b4b5f make_prog > Job a8b21ec0 link
  Duration: 0:00:00.24

    CallNode 6c56c8d472dc1d07cfd2634893043130b401dc84 redun.examples.compile.link
      Args:   'prog', [File(path=prog.o, hash=4934098e), File(path=lib.o, hash=d90885ad)]
      Result: File(path=prog, hash=2f43c23c)

    Task a20ef6dc2ab4ed89869514707f94fe18c15f8f66 redun.examples.compile.link

      def link(prog_path: str, o_files: List[File]) -> File:
          """
          Link several object files together into one program.
          """
          o_files=" ".join(o_file.path for o_file in o_files)
          os.system(f"gcc -o {prog_path} {o_files}")
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

      c_file <-- argument of <a4ac4959> make_prog(prog_path, c_files)
             <-- argument of <a9a6af53> make(files)
             <-- origin

      compile_result_2 <-- <8d85cebc> compile(c_file_2)
        c_file_2 = <dfa3aba7> File(path=prog.c, hash=dfa3aba7)

      c_file_2 <-- argument of <74cceb4e> make_prog(prog_path, c_files)
               <-- argument of <45400ab5> make(files)
               <-- origin
```

We can change one of the input files, such as `lib.c`, and rerun the workflow. Due to redun's automatic incremental compute, only the minimal tasks are rerun:

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

Notice, two of the compile jobs are cached (`prog.c` and `prog2.c`), but compiling the library `lib.c` and the downstream link steps correctly rerun.

Check out the [examples](examples/) for more example workflows and features of redun. Also see the [design notes](docs/source/design.md) for more information on redun's features.


## Release

Redun releases are done via the `redun-release-auto` [codebuild pipeline](https://us-west-2.console.aws.amazon.com/codesuite/codebuild/projects/redun-release-auto/).

### Release Steps

1. Prepare a release branch and make a PR from this branch with an updated [`__version__`](https://github.com/insitro/redun/blob/db17e39a2efaf9b3be466c60bdfecbe6ce4ea054/redun/__init__.py#L14) and [`CHANGELOG`](https://github.com/insitro/redun/blob/master/docs/source/CHANGELOG.rst)
    e.g. https://github.com/insitro/redun/pull/107. Use [`release_notes.py`](docs/release_notes.py) script to generate the release notes (see that script for usage).
2. Merge the release branch. The codebuild pipeline will trigger automatically. If the version has been updated, it will initiate the release process. If the CHANGELOG is not updated to reflect the new version, the codebuild will terminate without releasing.
