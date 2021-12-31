# FAQ

A collection of gotchas, how-to's, and other common questions.

## How can I use an existing JobDefinition in AWS Batch Executor?
New options and features are added to the JobDefintions in AWS Batch long before they make their way to boto3.
Additionally, redun will not always be on the latest version of boto3.
So, there can be a significant delay in the time an option is introduced for a JobDefinition in AWS Batch and the it becoming available to set in the version of boto3 used by redun(sharedMemory for example).

Given that potential delay, redun allows the user to create the JobDefinition themselves in AWS and then use that existing JobDefinition when running their tasks.
To do this, set `autocreate_job=False` as a task option *and* specify a `job_def_name` in the task options.
You *must* set the `job_def_name` to the name of the JobDefintion you manually created in AWS Batch.
When both the above options are set on a task, redun will look for an existing JobDefintion by name only and will raise an error if not matches are found..
This is different from the default lookup which compares both the name and the container properties and will also create a definition if one does not exist. 


## How do I read a portion of a file without downloading/reading it all?

```py
from redun.file import File
input_file = File("<path_to_file>")
stream = input_file.open()
stream.seek(100)  # Move to the 101st byte
data = stream.read(10)  # Read the next 10 bytes
```


## Gotcha when modifying file passed as task arg

Let's say we had the following task which adds a string to the end of the input file:

```py
from redun.file import File
from redun.task import task

@task()
def add_footer(f: File) -> File
   with f.open("w") as outfile:
       outfile.write("EOF")
   return f
```

The issue with the above is that by writing to the file, we have changed the file hash and thus invalidated the input of the task we just ran.
If you called the task again, it would run again because the hash of the file has changed.
In cases where you plan on modifying a file within a task, it often makes sense to pass a file path rather than a ``File``.
So, the above example could be rewritten as:

```py
from redun.file import File
from redun.task import task

@task()
def add_footer(file_path: str) -> File
   f = File(file_path)
   with f.open("w") as outfile:
       outfile.write("EOF")
   return f
```

In the above example, we still end up with the same output.
However, if we rerun the task, it will not rerun as the ``file_path`` is unchanged so redun will find the past run in the cache and will no rerun.

In the case where your task is processing a file purely for reading (no writing), ``File`` is the ideal task argument type to use. By using ``File`` as a task argument type, the data provenance record the file hash and the workflow will properly react (rerun) due to changes in the input file.

In summary, input ``File`` should be arguments, and output ``File`` should be return results.
