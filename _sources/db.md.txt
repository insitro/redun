---
tocpdeth: 3
---

# Backend and database

redun stores data provenance and cached values in *repositories*, (repos for short), similar to how [git stores commit graphs in repos](https://git-scm.com/book/en/v2/Git-Basics-Getting-a-Git-Repository).
redun repos are currently implemented using either sqlite (the default) or PostgreSQL.
Additionally, users may specify an [external key-value store](#optional-value-store) to avoid bloating the primary repo with large binary objects.

Once your workflows mature to the point where you want to share them or their results with collaborators, we recommend configuring a persistent database repo for provenance tracking.
See the [backend configuration](config.md#backend) section for details.


## Querying call graphs

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


## Syncing provenance data

The redun database (stored in `.redun/redun.db` by default) is designed to be retained for long term storage and analysis. To aid this, the redun CLI provides several commands to make backing up and syncing databases easy.

### Repositories
Redun organizes data provenance into repositories ("repos" for short) similar to how git organizes code in repos. By default, all provenance is tracked in the default, nameless repository.

Additional named repositories may be added with `redun repo add <repo name> <config dir>`. The configuration for the repository may be local or on S3.

Available repositories can be enumerated with `redun repo list`.

Removing repositories via the command line is not currently supported, but `redun repo remove <repo name>` will instruct you on how to edit your config file.

### Running redun in other repositories

To run redun commands in a different database, `redun --repo <repo name> <command>` will work for most commands. For example, `redun --repo foo log --task` will print tasks present in the "foo" repository.

### Synchronizing repositories

Just like git, `redun pull <repo>` and `redun push <repo>` will synchronize data to and from remote repositories.

You can also sync from remote to remote: `redun --repo foo push bar` will push all records from repository "foo" to "bar". The number of records pushed will be displayed.

### Manual exports
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

## Database migration

The redun CLI will automatically initialize and upgrade the redun database in most cases, so users will oftentimes not need to manage database versioning themselves. However, there are certain situations where users may want or need to manage database upgrades explicitly, such as upgrading a central shared PostgreSQL database. Here, we review common commands for inspecting and managing database version upgrades.

When the redun CLI is run for the first on a workflow script, by default a new sqlite database is created at `.redun/redun.db`.

```sh
redun run workflow.py main

# .redun/redun.db sqlite database is created and upgraded to latest version known to the CLI.
```

The version of the database can be queried using `redun db info`, which should produce output something like:

```sh
redun :: version 0.4.11
config dir: /Users/rasmus/projects/redun/.redun

db version: 2.0
CLI requires db versions: >=2,<3
CLI compatible with db: True
```

The line `CLI requires db versions: >=2,<3` specifies what range of database versions the currently install CLI can work with. If the CLI requires a newer database version, the database will need to be upgraded before use.

To see what database versions are available to the currently installed redun CLI, use the following:

```
$ redun db versions

Version Migration    Description

1.0     806f5dcb11bf Prototype schema.
2.0     647c510a77b1 Initial production schema.
```

To upgrade the database to a new version, use the following:

```
$ redun db upgrade 3.0

redun :: version 0.4.11
config dir: /Users/rasmus/projects/redun/.redun

Initial db version: 2.0
[redun] Upgrading db from version 2.0 to 3.0...
Final db version: 3.0
```

Downgrades can be done with `redun db downgrade <version>`.


### Automatic database upgrading

For the most common use case, the redun CLI uses a local sqlite database file (e.g. `.redun/redun.db`).
Since such a database is used by only one client, it is typically safe to automatically upgrade the database if needed.
Such an upgrade will happen automatically on the next `redun run ...` command.
If you would like to disable automatic upgrades, it can be turned off with the [`automigrate`](config.md#automigrate) configuration option.
Automigration is not used for non-sqlite databases, since a centrally used database will likely need more coordination to not disrupt clients.


### Database version capabilities

Each version of the redun library requires the redun database to be within a specific version range. To understand what database version is required for your library or database, consult the table below.

| redun lib version range | required database version range |
|-------------------------|---------------------------------|
| <=0.4.10                | >=1 <2                          |
| >=0.4.11 <=0.5.0        | >=2 <3                          |
| >=0.5.1 <=0.6.0         | >=3.0 <4                        |
| >=0.6.1                 | >=3.1 <4                        |


### No downtime migrations

The compatibility ranges between the library and database are designed to allow gradual upgrades, so that redun clients never need to fully stop access.

- Migrations are designed to be *pre-deploy*, that is, they are always applied to the database (e.g. `redun db upgrade`) before upgrading clients.
- Minor database versions (e.g `3.0 --> 3.1`) can be upgraded to without disruption of current clients, because they typically contain non-breaking changes such as adding indexes or changing constraints.
- To upgrade to a major database version (e.g. `3.0 --> 4.0`) without downtime, all clients need to allow the new major version (see table above for details) before the migration is applied.


## Optional value store

By default, values are stored in the database as BLOB columns.
For most usage patterns, these BLOBs represent the fastest source of growth in the repo.
Users can specify an external key-value store and a minimum size cutoff beyond which values are written to the value store instead of the primary repo.
See `ValueStore` for implementation details.
