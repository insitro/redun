# Running scripts

It is common, especially in bioinformatics, to use workflow engines to combine together unix programs into a computational pipeline. Typically, each program exchanges data with each other through input and output files. redun provides [several features](https://insitro.github.io/redun/design.html#shell-scripting) to ease the calling of programs through scripts and managing their input and output files.

This example is focused solely on calling scripts. We will soon see in later examples, how this combines nicely with redun's other features of running commands within Docker as well as running Docker containers in the cloud.

## Running the workflow

This example demonstrates a small workflow for analyzing a tab-delimited dataset, `data.tsv`. In the following data, we have the columns: id, name, and color preference.

```
1	Alice	red
2	Bob	red
3	Claire	blue
4	Dan	green
5	Eve	green
6	Frank	red
7	Gina	green
```

The provided workflow, `workflow.py`, uses Python and shell commands to count the histogram of color preference. To run the workflow, simply use:

```sh
redun run workflow.py main
```

which should produce logging output something like this:

```
[redun] redun :: version 0.4.15
[redun] config dir: /Users/rasmus/projects/redun/examples/04_script/.redun
[redun] Upgrading db from version -1.0 to 2.0...
[redun] Tasks will require namespace soon. Either set namespace in the `@task` decorator or with the module-level variable `redun_namespace`.
tasks needing namespace: workflow.py:parse_int, workflow.py:find_colors, workflow.py:count_color, workflow.py:count_colors, workflow.py:count_colors_by_script, workflow.py:main
[redun] Start Execution 1226c2fb-4646-4f8a-8a39-3616f870d03c:  redun run workflow.py main
[redun] Run    Job f8735a11:  main(data=File(path=data.tsv, hash=14faa10c)) on default
[redun] Run    Job 9296a0cf:  find_colors(data=File(path=data.tsv, hash=14faa10c), column=2) on default
[redun] Run    Job 57cccfaa:  count_colors_by_script(data=File(path=data.tsv, hash=14faa10c), output_path='/Users/rasmus/projects/redun/examples/04_script/color-counts.txt') on default
[redun] Run    Job 839d637c:  redun.script(command='cd "/var/folders/n5/vtwfqcl95hn1wgpnqdm_szv00000gs/T/tmp46x6obpy.tempdir"\ncp /Users/rasmus/projects/redun/examples/04_script/data.tsv data\n(\n# Save command to temp file.\nCOMMAND_FILE="$(mktemp..., inputs=[File(path=/Users/rasmus/projects/redun/examples/04_script/data.tsv, hash=685ce68d)], outputs={'colors-counts': <redun.file.StagingFile object at 0x107013a30>, 'log': <redun.file.StagingFile object at 0x107013cd0>}, task_options={}, temp_path='/var/folders/n5/vtwfqcl95hn1wgpnqdm_szv00000gs/T/tmp46x6obpy.tempdir') on default
[redun] Run    Job 5c3913cd:  count_colors(data=File(path=data.tsv, hash=14faa10c), colors=['blue', 'green', 'red']) on default
[redun] Run    Job 103f0008:  redun.script_task(command='cd "/var/folders/n5/vtwfqcl95hn1wgpnqdm_szv00000gs/T/tmp46x6obpy.tempdir"\ncp /Users/rasmus/projects/redun/examples/04_script/data.tsv data\n(\n# Save command to temp file.\nCOMMAND_FILE="$(mktemp...) on default
[redun] Run    Job 48a63d75:  count_color(data=File(path=data.tsv, hash=14faa10c), color='blue') on default
[redun] Run    Job ad358e10:  count_color(data=File(path=data.tsv, hash=14faa10c), color='green') on default
[redun] Run    Job fa6b7cac:  count_color(data=File(path=data.tsv, hash=14faa10c), color='red') on default
[redun] Run    Job 10bbff05:  redun.script(command='(\n# Save command to temp file.\nCOMMAND_FILE="$(mktemp)"\ncat > "$COMMAND_FILE" <<"EOF"\n#!/usr/bin/env bash\nset -exo pipefail\nawk \'$3 == "blue"\' data.tsv | wc -l\nEOF\n\n# Execute temp file...., inputs=[], outputs=File(path=-, hash=4df20acf), task_options={}, temp_path=None) on default
[redun] Run    Job 3e792e37:  redun.script(command='(\n# Save command to temp file.\nCOMMAND_FILE="$(mktemp)"\ncat > "$COMMAND_FILE" <<"EOF"\n#!/usr/bin/env bash\nset -exo pipefail\nawk \'$3 == "green"\' data.tsv | wc -l\nEOF\n\n# Execute temp file..., inputs=[], outputs=File(path=-, hash=4df20acf), task_options={}, temp_path=None) on default
[redun] Run    Job 8047a3a1:  redun.script(command='(\n# Save command to temp file.\nCOMMAND_FILE="$(mktemp)"\ncat > "$COMMAND_FILE" <<"EOF"\n#!/usr/bin/env bash\nset -exo pipefail\nawk \'$3 == "red"\' data.tsv | wc -l\nEOF\n\n# Execute temp file.\..., inputs=[], outputs=File(path=-, hash=4df20acf), task_options={}, temp_path=None) on default
[redun] Run    Job 49462011:  redun.script_task(command='(\n# Save command to temp file.\nCOMMAND_FILE="$(mktemp)"\ncat > "$COMMAND_FILE" <<"EOF"\n#!/usr/bin/env bash\nset -exo pipefail\nawk \'$3 == "blue"\' data.tsv | wc -l\nEOF\n\n# Execute temp file....) on default
[redun] Run    Job d9d7706c:  redun.script_task(command='(\n# Save command to temp file.\nCOMMAND_FILE="$(mktemp)"\ncat > "$COMMAND_FILE" <<"EOF"\n#!/usr/bin/env bash\nset -exo pipefail\nawk \'$3 == "green"\' data.tsv | wc -l\nEOF\n\n# Execute temp file...) on default
[redun] Run    Job 896e6c1d:  redun.script_task(command='(\n# Save command to temp file.\nCOMMAND_FILE="$(mktemp)"\ncat > "$COMMAND_FILE" <<"EOF"\n#!/usr/bin/env bash\nset -exo pipefail\nawk \'$3 == "red"\' data.tsv | wc -l\nEOF\n\n# Execute temp file.\...) on default
[redun] Run    Job daefae7d:  redun.postprocess_script(result=b'', outputs={'colors-counts': <redun.file.StagingFile object at 0x107013a30>, 'log': <redun.file.StagingFile object at 0x107013cd0>}, temp_path='/var/folders/n5/vtwfqcl95hn1wgpnqdm_szv00000gs/T/tmp46x6obpy.tempdir') on default
[redun] Run    Job ed47973e:  redun.postprocess_script(result=b'       1\n', outputs=File(path=-, hash=4df20acf), temp_path=None) on default
[redun] Run    Job f1c8f187:  parse_int(text=b'       1\n') on default
[redun] Run    Job 6645b262:  redun.postprocess_script(result=b'       3\n', outputs=File(path=-, hash=4df20acf), temp_path=None) on default
[redun] Run    Job 047d4fed:  parse_int(text=b'       3\n') on default
[redun] Cached Job 724363b4:  redun.postprocess_script(result=b'       3\n', outputs=File(path=-, hash=4df20acf), temp_path=None) (eval_hash=14cf14f5)
[redun] Cached Job 90aadaf5:  parse_int(text=b'       3\n') (eval_hash=7f01c4ac)
[redun]
[redun] | JOB STATUS 2021/06/21 06:35:50
[redun] | TASK                     PENDING RUNNING  FAILED  CACHED    DONE   TOTAL
[redun] |
[redun] | ALL                            0       0       0       2      20      22
[redun] | count_color                    0       0       0       0       3       3
[redun] | count_colors                   0       0       0       0       1       1
[redun] | count_colors_by_script         0       0       0       0       1       1
[redun] | find_colors                    0       0       0       0       1       1
[redun] | main                           0       0       0       0       1       1
[redun] | parse_int                      0       0       0       1       2       3
[redun] | redun.postprocess_script       0       0       0       1       3       4
[redun] | redun.script                   0       0       0       0       4       4
[redun] | redun.script_task              0       0       0       0       4       4
[redun]
{'method1': {'blue': 1, 'green': 3, 'red': 3},
 'method2': {'colors-counts': File(path=/Users/rasmus/projects/redun/examples/04_script/color-counts.txt, hash=ec848b53),
             'log': File(path=/Users/rasmus/projects/redun/examples/04_script/color-counts.txt.log, hash=2716ed53)}}
```

From the output, we can see that the first method of counting colors, `method1`, is returning the histogram to stdout, `{'blue': 1, 'green': 3, 'red': 3}`. Next, we can see the other method, `method2`, has stored the histogram in an output file `color-counts.txt` along with a log file `color-counts.txt.log`. Feel free to inspect these files and compare them to the workflow source code.

## Using the `script()` task

Many of the tasks should be straightforward to understand given the previous examples, so let us focus on the ones that involve the new builtin task `script()`. For example, let's look at the task `count_color`:

```py
@task()
def count_color(data: File, color: str) -> int:
    """
    Use a script to count people who like a particular color.
    """

    # script() will return the standard out of a shell script.
    # We use the f-string syntax to fill in parameters.
    # Feel free to indent the script to match the code,
    # dedenting is applied automatically.
    output = script(
        f"""
        awk '$3 == "{color}"' {data.path} | wc -l
        """
    )
    return parse_int(output)
```

The task `script()` is available from the redun library. It's first argument is a string representing a shell script to execute. This string can be multiple lines and can be indented to match the surrounding code. redun uses [dedent()](https://docs.python.org/3/library/textwrap.html#textwrap.dedent) to remove leading white space in scripts. It is also common to use [Python's f-string syntax](https://docs.python.org/3/reference/lexical_analysis.html#f-strings) to customize the command.

Since `script()` is a task, its return value is a lazy expression. The reason for this design will be more clear in the later Docker and AWS Batch examples, where we make use of running the script asynchronously. The lazy expression will eventually evaluate to a string representing the stdout of the command. In this example, the string is a count of a particular color, and we use a post-processing task, `parse_int`, to parse the string to an integer to get our desired end result.

## Script isolation

In real world workflows, shell scripts often create intermediate files, especially in the current working directory. This can be problematic when running lots of scripts in parallel, since these files could overwrite one another. There are several ways we can isolate such scripts to avoid conflict.

In the following task, we use a temporary directory and we copy input and output files to and from the temporary directory. We will use a similar pattern to copy files to and from a Docker container.

```py
@task()
def count_colors_by_script(data: File, output_path: str) -> Dict[str, File]:
    """
    Count colors using a multi-line script.
    """
    # This example task uses a multi-line script.
    # We also show how to isolate the script from other scripts that might be running.
    # By using `tempdir=True`, we will run this script within a temporary directory.

    # Use absolute paths for input and output because script will run in a new temp directory.
    data = File(os.path.abspath(data.path))
    output = File(os.path.abspath(output_path))
    log_file = File(os.path.abspath(output_path) + ".log")

    # Staging Files.
    # The `File.stage()` method pairs a local path (relative to current working directory
    # in the script) with a remote path (url or project-level path). This pairing is used
    # to automatically copy files to and from the temporary directory.
    #
    # staged_file: StagingFile = File(remote_path).stage(local_path)

    return script(
        f"""
        echo 'sorting colors...' >> log.txt
        cut -f3 data | sort > colors.sorted

        echo 'counting colors...' >> log.txt
        uniq -c colors.sorted | sort -nr > color-counts.txt
        """,

        # Use a temporary directory for running the script.
        tempdir=True,

        # Stage the input file to a local file called `data`.
        inputs=[data.stage("data")],

        # Unstage the output files to our project directory.
        # Final return value of script() takes the shape of outputs, but with each StagingFile
        # replaced by `File(remote_path)`.
        outputs={
            "colors-counts": output.stage("color-counts.txt"),
            "log": log_file.stage("log.txt"),
        },
    )
```

Notice the intermediate file `colors.sorted` is created within the temporary directory and is not left behind in our working project directory.


## Conclusion

Here, we saw several variations for running shell scripts from a redun workflow. We saw how to parameterize scripts, isolate their execution, and stage input/output files. Next, we will see how these features can be combined with Docker containers and AWS Batch.
