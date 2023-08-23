---
tocpdeth: 3
---

# Configuration

A redun workflow is configured using a configuration file called `.redun/redun.ini` which follows the [INI format](https://en.wikipedia.org/wiki/INI_file)). Here is an example:

```ini
[backend]
db_uri = sqlite:///redun.db

[executors.default]
type = local
max_workers = 20

[executors.batch]
type = aws_batch
image = my-docker-image
queue = my-aws-batch-queue
s3_scratch = s3://my-bucket/my-path/
```

## Configuration directory

The configuration directory, `.redun/`, is specified by the following (in order of precedence):
- Command line option: `redun --config <config_dir> ...`
- Environment variable: `export REDUN_CONFIG=<config_dir>`
- Filesystem search: Find a directory named `.redun/` starting in the current working directory and proceeding to parent directories.
- Lastly, automatically create a configuration directory, `.redun/`, in the current working directory.


## Configuration options

Currently, redun supports the following configuration sections and options.


### Scheduler

Scheduler options can be configured in the `[scheduler]` section.

#### `ignore_warnings`

A whitespace-separated string of redun warnings to suppress from logging. Current warning types include:

- `namespace`: Task namespaces will be required soon. Until then, it is a warning to not define a namespace for a task.

#### `setup_scheduler`

A string that specifies the location of a user-defined function for setting up the scheduler. It should follow the syntax `{file_or_module}::{function_name}`, where

- `{file_or_module}` should be a filename relative to the `redun.ini` file (e.g. `../workflow.py`), or a python module using dot-syntax (e.g. `workflow_lib.utils.scheduler`).
- `{function_name}` should the name of a function, typically `setup_scheduler`.

The user function `setup_scheduler()` should take as its first argument `config` (see [Config](redun.config.Config)) and it should return an instantiated `scheduler` (see [Scheduler](redun.scheduler.Scheduler)).

```py
from redun.config import Config
from redun import Scheduler

def setup_scheduler(config: Config) -> Scheduler:
    scheduler = Scheduler(config=config)
    # Perform additional setup such as
    #   scheduler.add_executor(...)
    return scheduler
```

If `setup_scheduler()` has additional arguments, they are automatically parsed from the command line using `--setup <option>=<value>`. For example, say we had the following workflow script which supports customizing the executors depending on whether we are in a production or development environment.

```py
# workflow.py
from redun.config import Config
from redun import Scheduler, task

from my_workflow_lib.executors import ProdExecutor, DevExecutor


redun_namespace = "acme"


@task()
def main(x: int, y: int):
    # ...


def setup_scheduler(config: Config, profile: str = "prod") -> Scheduler:
    scheduler = Scheduler(config=config)
    if profile == "prod":
        scheduler.add_executor(ProdExecutor())
    else:
        scheduler.add_executor(DevExecutor())
    return scheduler
```

We could then call this workflow using:

```sh
redun --setup profile=dev run workflow.py main --x 1 --y 2
```


#### `job_status_interval`

An integer (default: 20) of how many seconds between displays of the job status table.


### `federated_configs`

A config file may mark other config files that should be imported. This is particularly useful
for importing remote config files containing federated entrypoints and their executors.

```
[scheduler]
federated_configs = <path to config_dir>
```

or

```
[scheduler]
federated_configs = 
    <path to config_dir_1>
    <path to config_dir_2>
```

Only the `executor` and `federated_tasks` from these config file(s) are imported, all other sections
are ignored. There is no namespacing for these imported executors and federated tasks, either
from each other or from the rest of the scheduler's configuration. Any duplicate names will result 
in an error being raised.

### Backend

Backend options, such as connecting to the redun database, can be configured in the `[backend]` section.

#### `automigrate`

A bool indicating whether the redun client should automigrate the redun repo database to the latest schema before execution. If the database is a local sqlite file, `automigrate` is `True` by default for convenience. Otherwise, `automation` is `False` by default for safety since a central shared database likely needs a coordinated migration.

#### `db_aws_secret_name`

When specified, the database URI will be constructed from the data stored in the named secret.
__NOTE:__ This setting takes precedence over the following backend config options.
If you set this, the `db_uri` and environment variable settings below will be ignored.

This setting assumes the standard fields in an AWS RDS or Other Database secret, namely:
 * engine
 * username
 * password
 * host
 * port
 * dbname

#### `db_uri`

[URI](https://docs.sqlalchemy.org/en/13/core/engines.html#database-urls) for the redun database. redun supports the `sqlite` and `postgresql` protocols. If the URI is relative, it will be interpreted as relative to the configuration directory, `.redun/`.

For security reasons, username and password are not allowed in `db_uri`, since `redun.ini` files are often checked into source control. See below for alternative ways to define database credentials.

#### `db_username_env`

Environment variable (default: `REDUN_DB_USERNAME`) containing the username login for `db_uri`.

#### `db_password_env`

Environment variable (default: `REDUN_DB_PASSWORD`) containing the password login for `db_uri`.

#### `max_value_size`

An integer (default: `1000000000`, 1Gb) specifying the maximum number of bytes to allow for storing a single value in the cache.

#### `value_store_path`

Location of an optional `ValueStore`.
If a local, non-absolute path is specified, it is assumed relative to `config_dir`.

#### `value_store_min_size`

Minimum value size in bytes beyond which values are written to the value store instead of the primary backend.

### Limits

redun supports limiting the parallelism of tasks using resource limits, similar to other workflow engines like [Luigi](https://luigi.readthedocs.io/en/stable/configuration.html#resources-config). For example, the following configuration:

```ini
[limits]
db = 10
web_api = 5
```

specifies two resource limits `db` and `web_api` with available amounts of 10 and 5, respectively. Users can specify that certain tasks require a resource using the `limit` task option:

```py
@task(limits=["db"])
def read_from_database(args):
    # ...
```

Every time the task `read_from_database` is executing, it consumes one unit of resource `db`. It releases its resource when the task completes. This limit allows users to define a maximum concurrency on a per task or resource basis, which is useful for preventing overloading databases or APIs with too many concurrent connections.

Tasks may specify multiple resources within the `limits` list. Tasks may also consume multiples of a resource by specifying `limits` with a `dict`:

```py
@task(limits={"db": 2})
def read_from_database(args):
    # ...
```

In the above example, `read_from_database()` consumes 2 units of the resource `db` for each running task.


### Executors

redun is able to perform task execution across various compute infrastructure using modules call [Executors](./executors). Each executor has its own configuration options.

Executors are defined using a section with the format `executor.{executor_name}`, where `{executor_name}` is a user-specific name for the executor, such as `default` or `batch`. The executor module is specified by the option `type`. For example, we can configure a local executor with name `local_exec` and 20 maximum threads using:

```ini
[executors.local_exec]
type = local
max_workers = 20
```

Within a workflow, users can specify which tasks should execute on this executor using the `executor` task option:

```py
@task(executor="local_exec")
def task1():
    # ...
```


#### `type`

A string that specifies the executor module (e.g. `local`, `aws_batch`, etc) to use for this executor.


#### Local executor

The [Local executor](executors.md#local-executor) (`type = local`) executes tasks on the local machine using either multiple threads or processes. 

##### `mode`

A string (default: `thread`) that specifies whether to run tasks on new threads (`mode = thread`) or new processes (`mode = process`).

##### `max_workers`

An integer (default: 20) that specifies the maximum number of workers to use in the thread/process pool for task execution.

##### `start_method`

A string (default: `fork`) that specifies the [start method](https://docs.python.org/3/library/multiprocessing.html#contexts-and-start-methods) for new processes. Other start methods include `spawn` and `forkserver`.

#### Alias executor

The [Alias executor](executors.md#alias-executor) (`type = alias`) is a "symlink" to 
another underlying executor.

##### `target`

A string containing the  name of another executor to defer to. It is lazily resolved 
at execution time.


#### Docker executor

##### `image`

A string that specifies the default Docker image. This can be overridden on a per task basis using task options.

##### `scratch`

A string that specifies the scratch path to volume mount for communicating with the Docker container. If the path is relative, it is interpreted to be relative to the redun configuration directory (e.g. `.redun/`).

##### `vcpus`

An integer (default: 1) that specifies the default number of virtual CPUs required for each task. This can be overridden on a per task basis using task options.

##### `gpus`

An integer (default: 0) that specifies the default number of GPUs required for each task. This can be overridden on a per task basis using task options.

##### `memory`

A float (default: 4) that specifies the default amount of memory (in Gb) required for each task. This can be overridden on a per task basis using task options.

##### `interactive`

A bool (default: False) that specifies whether the Docker container should be run in interactive mode. This is useful for debugging a workflow since the stdout of every container will be redirected to the terminal.

##### `volumes`

A JSON list of pairs that specifies [Docker volume mounts](https://docs.docker.com/storage/volumes/). Each pair is a host and container path. For example, the following syntax is used to mount `/tmp/data` and `/tmp/data2` host directories to `/workflow/data` and `/workflow/data2` container directories, respectively.

```ini
volumes = [["/tmp/data", "/workflow/data"], ["/tmp/data2", "/workflow/data2"]]
```

If host paths are relative, they are assumed relative to the configuration directory (e.g. `.redun`).

#### AWS Batch executor

The [AWS Batch executor](executors.md#aws-batch-executor) (`type = aws_batch`) executes tasks on the AWS Batch compute service.

##### `image`

A string that specifies the default Docker image to use for AWS Batch jobs. This can be overridden on a per task basis using task options.

##### `queue`

A string that specifies the default AWS Batch queue to use for submitting jobs. This can be overridden on a per task basis using task options.

##### `s3_scratch`

A string that specifies the [S3 scratch space](executors.md#s3-scratch-space) used to communicate with AWS Batch jobs.

##### `aws_region`

A string that specifies the AWS region containing the user's AWS Batch service.

##### `role`

A string (default: the AWS account's `ecsTaskExecutionRole`) that specifies the ARN of the IAM role that AWS Batch jobs should adopt while executing. This may be needed for jobs to have proper access to resources (S3 data, other services, etc). This can be overridden on a per task basis using task options. To disable setting a role on the job definition use `None`.

This key is mapped to the [`jobRoleArn` property](https://docs.aws.amazon.com/batch/latest/userguide/job_definition_parameters.html#containerProperties) of the Batch API.

##### `vcpus`

An integer (default: 1) that specifies the default number of virtual CPUs required for each task. This can be overridden on a per task basis using task options.

##### `gpus`

An integer (default: 0) that specifies the default number of GPUs required for each task. This can be overridden on a per task basis using task options.

##### `memory`

A float (default: 4) that specifies the default amount of memory (in Gb) required for each task. This can be overridden on a per task basis using task options.

##### `retries`

An integer (default: 1) that specifies the default number of retries to use for submitting a job to the AWS Batch queue. This can be overridden on a per task basis using task options.

##### `job_name_prefix`

A string (default: `batch-job`) that specifies the prefix to use for AWS Batch job names. This can make it easier for users to distinguish which AWS Batch jobs correspond to their workflow.

##### `code_package`

A bool (default: True) that specifies whether to perform [code packaging](executors.md#code-packaging).

##### `code_includes`

A string (default: `**/*.py`) that specifies a pattern for which files should be included in a [code package](executors.md#code-packaging). Multiple patterns can be specified separated by whitespace. Whitespace can be escaped using [shell-like syntax](https://docs.python.org/3/library/shlex.html#shlex.split)

##### `code_excludes`

A string (default: None) that specifies a pattern for which files should be excluded from a [code package](executors.md#code-packaging). Multiple patterns can be specified separated by whitespace. Whitespace can be escaped using [shell-like syntax](https://docs.python.org/3/library/shlex.html#shlex.split)

##### `debug`

A bool (default: False) that specifies whether to run the [Docker container locally](executors.md#local-debugging).

##### `job_monitor_interval`

A float (default: 5.0) that specifies how often, in seconds, the AWS Batch API is queried to monitor running jobs.

##### `min_array_size`

Minimum number (default: 5) of equivalent tasks that will be submitted together as an AWS Batch array job. "Equivalent" means the same task and execution requirements (CPU, memory, etc), but with possibly different arguments. Set to 0 to disable array job submission. Defaults to 5.

##### `max_array_size`

Maximum number (default: 1000) of equivalent tasks that will be submitted together as an AWS Batch array job. Must be greater than or equal to `min_array_size`. If greater than the AWS maximum array size (currently 10000), will be clamped to this value.

##### `job_stale_time`

A float (default: 3.0) that specifies the maximum time, in seconds, jobs will wait before submission to be possibly bundled into an array job.

##### `timeout`

An optional integer (default: None) that specifies the time duration in seconds (measure from job attempt's `startedAt` timestamp) after which AWS Batch will terminate the job. For more on job timeouts, see the [Job Timeouts on Batch docs](https://docs.aws.amazon.com/batch/latest/userguide/job_timeouts.html). When not set, jobs will run indefinitely (unless on Fargate where there is a 14 day limit).

##### privileged

An optional bool (default: False) that specifies whether to run the job in privileged mode.

##### autocreate_job_def

An optional bool (default: True). If `autocreate_job_def` is disabled, then we require a `job_def_name`
to be present and lookup the job by name. If `autocreate_job_def` is enabled, then we will create
a new job definition if an existing one matching `job_def_name` and required properties cannot be found.
For backwards-compatibility, the deprecated `autocreate_job` is also supported.

##### job_def_name

An optional str (default: None) that specifies a job definition to use. If not set, a new job definition will created.

##### `batch_tags`

An optional JSON mapping of string key-value pairs to use as Batch job tags. This could be used to track jobs or query them after execution.

```ini
batch_tags = {"project": "acme", "user": "alice"}
```

##### `default_batch_tags`

A bool (default: True) that specifies whether redun should add default tags to all batch jobs. The default tags include:

- redun_job_id: id of the redun Job.
- redun_task_name: fullname of the redun Task.
- redun_execution_id: id of the high-level execution for the whole workflow.
- redun_project: the project of the workflow, which is typically the root task namespace.
- redun_aws_user: the user as identified by sts.get_caller_identity.

##### `num_nodes`

If not none, use a multi-node job and set the number of workers. 

#### AWS Glue executor

The [AWS Glue executor](executors.md#aws-glue-spark-executor) (`type = aws_glue`) executes tasks on the AWS Glue compute service.

##### `s3_scratch`

A string that specifies the [S3 scratch space](executors.md#s3-scratch-space) used to communicate with AWS Glue jobs.

##### `workers`

An integer that specifies the number of workers available by default to Glue jobs. Each worker provides one or more "data processing units" (DPUs). AWS defines a  DPU as "a relative measure of processing power that consists of 4 vCPUs of compute capacity and 16GB of memory." Depending on the worker type, there will be one or more Spark executors per DPU, each with one or more spark cores. Jobs are billed by number of DPUs and time. This parameter can be overridden on a per-task basis using task options.

##### `worker_type`

Choose from:

* `Standard`: each worker will have a 50GB disk scratch space and 2 executors, each with 4 vCPU cores.
* `G.1X`: each worker maps to 1 DPU and a single executor, with 8 vCPU cores and 10 GiB of memory. AWS recommends
this worker type for memory-intensive jobs.
* `G.2X`: each worker maps to 2 DPUs and a single executor, with 16 vCPU cores and 24576 MiB of memory. AWS recommends
this worker type for memory-intensive jobs or ML transforms. Note that as this worker type provides 2 DPUs, it is twice
as expensive as the others.

This can be overridden on a per-task basis using task options.

##### `aws_region`

A string that specifies the AWS region containing the user's AWS Glue service.

##### `role`

A string (default: your account's `AWSGlueServiceRole`) that specifies the ARN of the IAM role that AWS Glue jobs should adopt while executing. AWS provides a [managed policy](https://docs.aws.amazon.com/glue/latest/dg/create-service-policy.html), `AWSGlueServiceRole` that has the minimal permissions to run glue jobs, but you may need to provide access to S3 or other resources.

This key is mapped to the [`Role` property](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-jobs-job.html) of the Glue API.


##### `glue_job_prefix`

A string specifying the prefix for Glue job definitions created and used by redun. Defaults to `REDUN`.

##### `spark_history_dir`

String specifying an S3 path to the location where Spark will log history information that can be used by a
Spark History Server to visualize and monitor job progress. Defaults to `f"{s3_scratch_path}/glue/spark_history"`

##### `job_monitor_interval`

Float specifying how frequently to query AWS for the status of running Glue jobs. Defaults to 10.0 seconds.

##### `job_retry_interval`

Float specifying how frequently to retry submitting Glue jobs that were not able to be started due to resource
constraints. Five attempts will be made to submit pending jobs, and then the retry period will pass. Defaults
to 60.0 seconds.

##### `timeout`

Integer number of minutes to run jobs before they are killed. Defaults to 2880 (48 hours). Useful for preventing
cost overruns.

##### `code_package`

A bool (default: True) that specifies whether to perform [code packaging](executors.md#code-packaging).

##### `code_includes`

A string (default: `**/*.py`) that specifies a pattern for which files should be included in a [code package](executors.md#code-packaging). Multiple patterns can be specified separated by whitespace. Whitespace can be escaped using [shell-like syntax](https://docs.python.org/3/library/shlex.html#shlex.split)

##### `code_excludes`

A string (default: None) that specifies a pattern for which files should be excluded from a [code package](executors.md#code-packaging). Multiple patterns can be specified separated by whitespace. Whitespace can be escaped using [shell-like syntax](https://docs.python.org/3/library/shlex.html#shlex.split)

### Federated tasks

The following config keys are required. All other data is passed to the `subrun` that 
implements the bulk of the federated task behavior, hence they are either consumed by `subrun` directly or
are set as task options.

#### `namespace`

The namespace of the federated task.

#### `task_name`

The name of the federated task.

#### `load_module`

The name of the module to load, which will ensure that the above task is actually defined.

#### `executor`

The name of the executor that has the execution context for this federated task.

#### `config_dir`

The path to the configuration to use for the remainder of the execution. Either an absolute
path or relative to the executor entrypoint. 

#### `task_signature`

Optional. The signature of the federated task. Because the task options can be used to change the name of the task itself, task_name _and_ task_signature are separate options in the config.

#### `description`

Optional. A description of this entrypoint.

#### Kubernetes (k8s) executor

The [Kubernetes executor](executors.md#kubernetes-k8s-executor) (`type = k8s`) executes tasks on a Kubernetes cluster.

##### `image`

A string that specifies the default Docker image. This can be overridden on a per task basis using task options.

##### `scratch`

A string that specifies the [scratch space](executors.md#s3-scratch-space) used to communicate with k8s jobs. This can be a path on any accessible object storage system (e.g. S3, GCS, etc).

##### `namespace`

A string (default: `default`) that specifies the k8s namespace to use for all resources.

##### `secret_name`

An optional string that specifies the name of a k8s secret to use for passing AWS secrets.

##### `import_aws_secrets`

A bool (default: True) that specifies whether to import the local AWS environment variable secrets into the k8s jobs via a k8s secret (see `secret_name`).

##### `secret_env_vars`

A whitespace separated list of environment variables to import into the k8s jobs via a k8s secret (see `secret_name`).

##### `vcpus`

An integer (default: 1) that specifies the default number of virtual CPUs required for each task. This can be overridden on a per task basis using task options.

##### `memory`

A float (default: 4) that specifies the default amount of memory (in Gb) required for each task. This can be overridden on a per task basis using task options.

##### `retries`

An integer (default: 1) that specifies the default number of retries to use for submitting a job to the AWS Batch queue. This can be overridden on a per task basis using task options.

##### `job_name_prefix`

A string (default: `batch-job`) that specifies the prefix to use for AWS Batch job names. This can make it easier for users to distinguish which AWS Batch jobs correspond to their workflow.

##### `service_account_name`

A string (default: `default`) that specifices the k8s [service account](https://kubernetes.io/docs/reference/access-authn-authz/service-accounts-admin/) to use.

##### `annotations`

An optional JSON object that specifies k8s annotations to apply to each job.

##### `k8s_labels`

An optional JSON object that specifies k8s labels to apply to each job.

##### `default_k8s_labels`

A bool (default: True) that specifies whether to add default k8s labels to jobs, such as `redun_job_id`, `redun_task_name`, etc.

##### `code_package`

A bool (default: True) that specifies whether to perform [code packaging](executors.md#code-packaging).

##### `code_includes`

A string (default: `**/*.py`) that specifies a pattern for which files should be included in a [code package](executors.md#code-packaging). Multiple patterns can be specified separated by whitespace. Whitespace can be escaped using [shell-like syntax](https://docs.python.org/3/library/shlex.html#shlex.split)

##### `code_excludes`

A string (default: None) that specifies a pattern for which files should be excluded from a [code package](executors.md#code-packaging). Multiple patterns can be specified separated by whitespace. Whitespace can be escaped using [shell-like syntax](https://docs.python.org/3/library/shlex.html#shlex.split)

##### `job_monitor_interval`

A float (default: 5.0) that specifies how often, in seconds, the k8s API is queried to monitor running jobs.

##### `min_array_size`

Minimum number (default: 5) of equivalent tasks that will be submitted together as an [array job](https://kubernetes.io/docs/tasks/job/indexed-parallel-processing-static/). "Equivalent" means the same task and execution requirements (CPU, memory, etc), but with possibly different arguments. Set to 0 to disable array job submission.

##### `max_array_size`

Maximum number (default: 1000) of equivalent tasks that will be submitted together as an array job. Must be greater than or equal to `min_array_size`.

##### `job_stale_time`

A float (default: 3.0) that specifies the maximum time, in seconds, jobs will wait before submission to be possibly bundled into an array job.

##### `share_id`

Queues with Scheduling Policies require all jobs be assigned a Fair Share 
Scheduling `shareIdentifier`. Can be further overridden on a per-task basis using task options.

##### `scheduling_priority_override`

Queues with Scheduling Policies require that all job definitions specify a `schedulingPriority` 
Alternatively, if the batch job definition does *not* configure a `schedulingPriority`, you 
must provide a `schedulingPriorityOverride` by setting this variable. 
Can be further overridden on a per-task basis using task options. 

## Configuration variables

The redun configuration file supports [variable interpolation](https://docs.python.org/3/library/configparser.html#configparser.ExtendedInterpolation). When using the `${var}` variable reference syntax, redun will replace the variable reference with the value of the variable `var` from one of these sources (in descreasing precedence):

- variable in the same section.
- variable in the `DEFAULT` section.
- environment variable.

For example, if one wanted to configure the AWS role used for an AWS Batch Job using an environment variable, `REDUN_ROLE`, it could be achieved with the following config file:

```ini
[executors.batch]
type = aws_batch
image = 123.abc.ecr.us-west-2.amazonaws.com/amazonlinux-python3
queue = my-queue
s3_scratch = s3://bucket/redun/
role = ${REDUN_ROLE}
```
