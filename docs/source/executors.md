---
tocpdeth: 3
---

# Executors

redun is able to perform task execution across various compute infrastructure using modules called Executors. For example, redun supports executors that execute jobs on threads, processes, and AWS Batch jobs. New kinds of infrastructure can be utilized by registering additional Executor modules.

Executors are instantiated by sections in the [redun configuration](./config) (`.redun/redun.ini`) following the format `[executors.{executor_name}]`, where `{executor_name}` is a user-specific name for the executor, such as `default`, `batch`, or `my_executor`. These names can then be referenced in workflows using the `executor` task option to indicate on which executor the task should execute:

```py
@task(executor="my_executor")
def task1():
    # ...
```

By default, tasks execute on the `default` executor, which is a [Local executor](#local-executor) that executes tasks on multiple threads (or processes, if configured).

In terms of executors, there is no restriction on which tasks can call which other tasks. For example, an AWS Batch task can seemingly "call" a local task, even though AWS Batch jobs don't have access to the local machine. This is possible because all task calls are lazy and are performed by the scheduler after the calling task completes.

```py
@task(executor="batch")
def task_on_batch():
    # This code runs on AWS Batch.
    return task_on_default(10)  # This appears to be a call back to a local task.

@task()  # option `executor` has default value "default"
def task_on_default(x: int):
    # This code runs on the local machine.
    return x + 1
```

Some executor options can be overridden per task using task options. For example, a user can specify a specific memory requirement (e.g. 10Gb) for a task using the `memory` task option:

```py
@task(executor="batch", memory=10)
def task2():
    # ...
```

Options can also be dynamic and overridden at call-time using the `Task.options` method:

```py
@task()
def main(data):
    memory = len(data) * gigs_per_row
    x = task_on_batch.options(memory=memory)(data)
    # ...
```

## Local executor

The **Local executor** executes tasks on the same machine as the scheduler using either multiple threads or processes (configured by the [`mode` option](config.md#mode)). By default, redun defines a Local executor named "default" that is used as the default executor for tasks. Users can configure the local executor using the [configuration file](config.md#local-executor).


## AWS Batch executor

The **AWS Batch executor** executes tasks as jobs on [AWS Batch](https://aws.amazon.com/batch/), which is an AWS service for running [Docker-based](https://www.docker.com/) jobs on a compute cluster. AWS Batch manages job queues, compute nodes, and job assignments such that compute requirements are met (e.g. memory, vcpus, etc). redun manages the task dependency graph and will only submit a task to execute on AWS Batch once all upstream tasks are complete.

### Docker image

To use AWS Batch, users must define a Docker image that contains the necessary code for their task. If a task is a pure [script task](design.md#shell-scripting), only the commands used in the script need to be installed in the Docker image. However, if a regular task is to run on AWS Batch, then the `redun` python package must be installed in the Docker image to facilitate the execution of the task on the AWS Batch compute node. Typically, the workflow python code itself does not need to be installed inside the Docker image. Instead, redun provides automatic [code packaging](#code-packaging) as a convenience for quick iterative development.

### Configuration

To use AWS Batch, at a minimum three options must be [configured](config.md#aws-batch-executor), a Docker image for the job ([`image`](config.md#image)), a AWS Batch queue to submit to ([`queue`](config.md#queue)), and a S3 path to store temporary files for communication between the scheduler and jobs ([`s3_scratch`](config.md#s3-scratch)).

### S3 scratch space

redun performs simple communication with AWS Batch jobs through a user defined S3 scratch space. Specifically, the arguments to a task are serialized as a python pickle and stored at a path such as `s3://{s3_scratch}/{eval_hash}/input`, where `eval_hash` is the hash of a task's hash and its arguments and `s3_scratch` is defined in the [configuration](config.md#s3-scratch). When a task completes, its output is stored similarly in a pickle file `s3://{s3_scratch}/{eval_hash}/output`. Standard output and standard error is also captured in log files within the scratch space. All of these files are temporary and can be deleted by users once a workflow is complete.


### Code packaging

When running regular (non-script) tasks on AWS Batch, redun needs access to the workflow python code itself within the Docker container at runtime. While one could install the workflow python code in the Docker image, rebuilding and pushing Docker images for each code change could be burdensome during quick iterative development. As a convenience, redun provides a mechanism, called code packaging, for copying code into the Docker container at runtime.

By default, redun copies all files matching the pattern `**/*.py` into a tar file that is copied to the s3 scratch space. This tar file is then downloaded and unzipped within the running Docker container prior to executing the task. The specific files included in the code package can be controlled using [`code_includes` and `code_excludes` configuration options](config.md#code-includes).

### Job reuniting

In certain situations, such as errors or user initiated killing, the redun scheduler might be terminated while AWS Batch jobs are running. If the redun scheduler is restarted, it will attempt to determine if a batch task has an existing AWS Batch job already running or if one has recently completed leaving an output file in s3 scratch space. If so, the redun scheduler will "reunite" with such jobs and output and avoid double submission of AWS Batch jobs. redun uses the `eval_hash` to ensure the task hash and arguments are the same since the previous job submission.

### Local debugging

During development, it may be easier to run the Docker image locally in order to debug and interactively inspect a job. Local execution of Docker-base jobs can be achieved by using the [`debug=True` option](config.md#debug). The S3 scratch space will still be used to transfer input and output with the Docker container.

The docker container will run in interactive mode (e.g. `docker run --interactive ...`), allowing users to place debugging breakpoints within tasks or see task output on stdout. The task option `interactive=False` can also be used to run the Docker container without interactive mode.

The task option `volume` can also be used to define volume mounts for the Docker container during debugging. Format is `volume = [(host, container), ...]`, where `host` defines a source path on the host machine, and `container` defines a destination path within the container to perform the mount.

### Multi-node

AWS Batch allows for jobs that simultaneously use multiple compute nodes. See AWS [documentation](https://docs.aws.amazon.com/batch/latest/userguide/multi-node-parallel-jobs.html)

If the executor is configured to use multiple nodes, by setting `num_nodes`, the executor will invoke the task with identical arguments on each node. Batch starts the main node first, then starts the rest of the nodes. The task implementation may inspect the AWS environment variables for details on the multi-node configuration, such as detecting if it is the main node, or determining the IPs to construct a peer network.   

Warning: For python tasks, the executor will instruct only the main node to write its outputs to storage and non-main node outputs are discarded. For script tasks, the various nodes must somehow arrange that the output is only written once, but the infrastructure does not help. 

Multi-node jobs are currently incompatible with array jobs, because this appears not to be supported by AWS.

## AWS Glue Spark executor

The **AWS Glue executor** executes tasks as jobs on [AWS Glue](https://aws.amazon.com/glue/), which runs [Apache Spark](https://spark.apache.org/) jobs on a managed cluster. Spark jobs run on many CPUs and are especially useful for working with large tabular datasets such as those represented in Pandas DataFrames.

Spark jobs are essentially a mini compute cluster, with a driver that maintains a SparkContext object, and a number of workers.
Each worker can have one or more **executors**, which are the processes that run individual tasks in the Spark job. They typically
run for the life of the application, and send results to the driver when complete. Executors may use multiple vCPU cores to get
their work done, depending on the configuration.

To use AWS Glue, at a minimum you must configure a temporary location in S3 where files used to communicate between the scheduler and jobs are stored. Scratch space, code packaging, and job reuniting are all done in a similar way to the AWS Batch executor.


### Loading and writing datasets

Spark datasets are typically sharded across multiple files on disk. The `ShardedS3Dataset` class provides an interface to
these datasets that can be tracked and recorded by redun.

```eval_rst
.. autoclass:: redun.file.ShardedS3Dataset
   :members:
```

### Helper functions

Spark jobs are written a bit differently than pure Python. You'll want to load large datasets to Spark DataFrames with `ShardedS3Dataset`, but frequently other operations will be require the use of the Spark context that is defined when the
job is running.

The `redun.glue` module provides helper functions that can be used in glue executor jobs, and can be imported in the
top level of your redun script, even when Spark isn't yet defined. The `redun.glue.get_spark_context()` and
`redun.glue.get_spark_session()` functions can be used in your tasks to retrieve the currently defined spark environment.


#### User-defined functions

You might want to define your own functions to operate on a dataset. Typically, you'd use the `pyspark.sql.functions.udf` decorator
on a function to make it a UDF, but when redun evaluates the decorator it will error out as there is no spark context available
to register the function to. The `redun.glue.udf` decorator handles this issue. See the redun examples for a real-world use
of UDFs and this decorator.

### Available Python modules

AWS Glue automates management, provisioning, and deployment of Spark clusters, but only with a [pre-determined set of Python modules](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-python-libraries.html#glue20-modules-provided).
Most functionality you may need is already available, including scipy, pandas, etc.

Additional modules that are available in the public PyPi repository can be installed with the `additional_libs` task option.
However, other modules, especially those using C/C++ compiled extensions, are not really installable at this time. 

### Task options

The following configuration options may be specified on a per-task basis in the decorator.

#### `workers`

An integer that specifies the number of workers available by default to Glue jobs. Each worker provides one or more "data processing units" (DPUs). AWS defines a  DPU as "a relative measure of processing power that consists of 4 vCPUs of compute capacity and 16GB of memory." Depending on the worker type, there will can be one or more Spark executors per DPU, each with one or more spark cores. Jobs are billed by number of DPUs and time.

#### `worker_type`

Choose from:

* `Standard`: each worker will have a 50GB disk scratch space and 2 executors, each with 4 vCPU cores.
* `G.1X`: each worker maps to 1 DPU and a single executor, with 8 vCPU cores and 10 GiB of memory. AWS recommends
this worker type for memory-intensive jobs.
* `G.2X`: each worker maps to 2 DPUs and a single executor, with 16 vCPU cores and 24576 MiB of memory. AWS recommends
this worker type for memory-intensive jobs or ML transforms. Note that as this worker type provides 2 DPUs, it is twice
as expensive as the others.

#### `additional_libs`

A list of additional Python libraries that will be `pip install`'ed before the run starts. For example,
`additional_libs=["promise", "alembic==1.0.0"]` will install the promise and alembic libraries.

#### `extra_files`

A list of files that will be made available in the root directory of the run.
