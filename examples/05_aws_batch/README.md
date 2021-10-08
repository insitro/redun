# In the cloud: Running redun tasks on AWS Batch

redun can run individual tasks on AWS Batch. As we will see in this example, by specifying the `executor` task option, the user can choose which tasks should be executed on AWS Batch and which should be executed locally (or yet another remote compute system). redun takes care of routing inputs and outputs between tasks, launching remote jobs and monitoring for their completion. redun's design should allow the user to mix-and-match compute environments with different parts of their workflow however they please.

## Setup

When running full redun tasks on AWS Batch (as opposed to just a shell command), we need to install redun in our Docker image. To avoid [credential complexities](http://blog.oddbit.com/post/2019-02-24-docker-build-learns-about-secr/) when installing from private github repos, in this example, we will just copy redun into the container. Use the following make command to do so:

```sh
cd docker/
make setup
```

Now, we can use the following make command to build our Docker image. To specify a specific registry and image name, please edit the variables in [Makefile](docker/Makefile).

```sh
make login
make build
```

After the image builds, we need to publish it to ECR so that it is accessible by AWS Batch. There are several steps for doing that, which are covered in these make commands:

```sh
# If the docker repo does not exist yet.
make create-repo

# Push the locally built image to ECR.
make push
```

## Executors

redun is able to perform task execution across various compute infrastructure using modules called [Executors](../../docs/source/executors.md). For example, redun supports executors that execute jobs on threads, processes, and AWS Batch jobs. New kinds of infrastructure can be utilized by registering additional Executor modules.

Executors are defined by sections in the [redun configuration](../../docs/source/config.md) ([`.redun/redun.ini`](.redun/redun.ini)) following the format `[executors.{executor_name}]`, where `{executor_name}` is a user-specific name for the executor, such as `default`, `batch`, or `my_executor`. These names can then be referenced in workflows using the `executor` task option to indicate on which executor the task should use:

```py
@task(executor="my_executor")
def task1():
    # ...
```

## Configuring executors

To run tasks on AWS Batch, we need three things:
- An AWS Batch queue that we can submit to.
- A Docker image published in ECR that contains the redun and any other commands we wish to run.
- An S3 prefix redun can use for scratch space (communicating task input/output values).

Once we have these resources ready, we can configure redun to use them, using a config, `.redun/redun.ini`, in our project directory. Here is an example of one such config file:

```ini
# .redun/redun.ini

[executors.batch]
# Required options.
type = aws_batch
image = YOUR_ACCOUNT_ID.dkr.ecr.us-west-2.amazonaws.com/redun_aws_batch_example
queue = YOUR_QUEUE_NAME
s3_scratch = s3://YOUR_BUCKET/redun/

# Extra options.
role = arn:aws:iam::YOUR_ACCOUNT_ID:role/YOUR_ROLE
job_name_prefix = redun-example
batch_tags = {"user": "rasmus", "project": "acme"}
```

To get a working example, you'll need to replace the all caps variables `YOUR_ACCOUNT_ID`, `YOUR_QUEUE_NAME`, and `YOUR_BUCKET`, with your own AWS Account ID, AWS Batch queue name, and S3 bucket, respectively.

In the following output, we use these example variables as well.


## Running the workflow

Once we have our AWS setup complete, we can run our workflow using the usual `run` command:

```sh
redun run workflow.py main --output-path s3://YOUR_BUCKET/redun-examples/aws_batch
```

which should produce output something like:

```
[redun] redun :: version 0.4.15
[redun] config dir: /Users/rasmus/projects/redun/examples/aws_batch/.redun
[redun] Start Execution f9570a9a-8e19-461e-9c0e-35f2eeb689ff:  redun run --no-cache workflow.py main
[redun] Run    Job 1ebd7bdf:  redun.examples.aws_batch.main(y=10) on default
[redun] Run    Job 96e70822:  redun.examples.aws_batch.task_on_batch(x=10) on batch
[redun] Run    Job ecbff1b3:  redun.examples.aws_batch.task_lib.utils.lib_task_on_batch(x=10) on batch
[redun] Run    Job d4d5dd00:  redun.examples.aws_batch.task_on_batch_debug(x=10) on batch_debug
[redun] AWSBatchExecutor: submit redun job 96e70822-c9a2-40ae-92a1-8b7dcf596071 as AWS Batch job d757ab69-a112-4a8f-bd74-42c222fe793b:
[redun]   job_id          = d757ab69-a112-4a8f-bd74-42c222fe793b
[redun]   job_name        = redun-example-5a8c87a70457cb3fb6df6481359245d9ca303268
[redun]   s3_scratch_path = s3://YOUR_BUCKET/redun/jobs/5a8c87a70457cb3fb6df6481359245d9ca303268
[redun]   retry_attempts  = 0
[redun]   debug           = False
[redun] Run    Job 5dddb5b1:  redun.examples.aws_batch.run_script(y=10) on default
[redun] Run    Job d2280110:  redun.script(command='(\n# Save command to temp file.\nCOMMAND_FILE="$(mktemp)"\ncat > "$COMMAND_FILE" <<"EOF"\n#!/usr/bin/env bash\nset -exo pipefail\nuname -a;\nls\nEOF\n\n# Execute temp file.\nchmod +x "$COMMAND_FIL..., inputs=[], outputs=File(path=-, hash=4df20acf), task_options={'executor': 'batch'}, temp_path=None) on default
[redun] Run    Job dc2799b3:  redun.script_task(command='(\n# Save command to temp file.\nCOMMAND_FILE="$(mktemp)"\ncat > "$COMMAND_FILE" <<"EOF"\n#!/usr/bin/env bash\nset -exo pipefail\nuname -a;\nls\nEOF\n\n# Execute temp file.\nchmod +x "$COMMAND_FIL...) on batch
[redun] AWSBatchExecutor: submit redun job ecbff1b3-2d56-4d60-9b7e-2bc09bedc75c as AWS Batch job 35f0895d-6f17-4bac-8aa3-07835ce3ecc6:
[redun]   job_id          = 35f0895d-6f17-4bac-8aa3-07835ce3ecc6
[redun]   job_name        = redun-example-fdb6122b5d60191808e01ab0601610d2ebdf43c1
[redun]   s3_scratch_path = s3://YOUR_BUCKET/redun/jobs/fdb6122b5d60191808e01ab0601610d2ebdf43c1
[redun]   retry_attempts  = 0
[redun]   debug           = False
[redun] AWSBatchExecutor: submit redun job dc2799b3-764b-4d39-9bf0-1d38f4ce0d22 as AWS Batch job fc75470e-06d1-4a8b-8893-1adc1f50a01c:
[redun]   job_id          = fc75470e-06d1-4a8b-8893-1adc1f50a01c
[redun]   job_name        = redun-example-b399e8c497c2b498ba10f7073b5c997ff5235ce8
[redun]   s3_scratch_path = s3://YOUR_BUCKET/redun/jobs/b399e8c497c2b498ba10f7073b5c997ff5235ce8
[redun]   retry_attempts  = 0
[redun]   debug           = False
[redun] redun :: version 0.4.15
[redun] oneshot:  redun --check-version '>=0.4.1' oneshot workflow --import-path . --code s3://YOUR_BUCKET/redun/code/0b0d105ccbc680d990cb85dfa0340a2a88bbd667.tar.gz --no-cache --input s3://YOUR_BUCKET/redun/jobs/089a193016905a2154841b2c64995d00bc895bca/input --output s3://YOUR_BUCKET/redun/jobs/089a193016905a2154841b2c64995d00bc895bca/output --error s3://YOUR_BUCKET/redun/jobs/089a193016905a2154841b2c64995d00bc895bca/error redun.examples.aws_batch.task_on_batch_debug
[redun] AWSBatchExecutor: submit redun job d4d5dd00-b4d0-43b1-96af-6192cf927c4f as Docker container b3272306c25dffea26506b9d5f00a71101cf7a79bd3721464b6be89666abde08:
[redun]   job_id          = b3272306c25dffea26506b9d5f00a71101cf7a79bd3721464b6be89666abde08
[redun]   job_name        = None
[redun]   s3_scratch_path = s3://YOUR_BUCKET/redun/jobs/089a193016905a2154841b2c64995d00bc895bca
[redun]   retry_attempts  = None
[redun]   debug           = True
[redun] Run    Job 4bd3efc5:  redun.examples.aws_batch.task_on_default(x=15) on default
[redun] Run    Job d839641e:  redun.postprocess_script(result=[0, b'Linux ip-172-31-138-172.us-west-2.compute.internal 4.14.232-176.381.amzn2.x86_64 #1 SMP Wed May 19 00:31:54 UTC 2021 x86_64 x86_64 x86_64 GNU/Linux\nredun\nrequirements.txt\n'], outputs=File(path=-, hash=4df20acf), temp_path=None) on default
[redun] Run    Job 8668b9a9:  redun.examples.aws_batch.task_on_default(x=15) on default
[redun]
[redun] | JOB STATUS 2021/06/25 15:42:37
[redun] | TASK                                                      PENDING RUNNING  FAILED  CACHED    DONE   TOTAL
[redun] |
[redun] | ALL                                                             0       0       0       0      10      10
[redun] | redun.examples.aws_batch.main                                   0       0       0       0       1       1
[redun] | redun.examples.aws_batch.run_script                             0       0       0       0       1       1
[redun] | redun.examples.aws_batch.task_lib.utils.lib_task_on_batch       0       0       0       0       1       1
[redun] | redun.examples.aws_batch.task_on_batch                          0       0       0       0       1       1
[redun] | redun.examples.aws_batch.task_on_batch_debug                    0       0       0       0       1       1
[redun] | redun.examples.aws_batch.task_on_default                        0       0       0       0       2       2
[redun] | redun.postprocess_script                                        0       0       0       0       1       1
[redun] | redun.script                                                    0       0       0       0       1       1
[redun] | redun.script_task                                               0       0       0       0       1       1

['main',
 b'Darwin MBP-MML7H006C8 20.5.0 Darwin Kernel Version 20.5.0: Sat May  8 05:10:'
 b'33 PDT 2021; root:xnu-7195.121.3~9/RELEASE_X86_64 x86_64\n',
 ['task_on_batch',
  b'Linux ip-172-31-138-172.us-west-2.compute.internal 4.14.232-176.381.amzn'
  b'2.x86_64 #1 SMP Wed May 19 00:31:54 UTC 2021 x86_64 x86_64 x86_64 GNU/Li'
  b'nux\n',
  ['task_on_default',
   b'Darwin MBP-MML7H006C8 20.5.0 Darwin Kernel Version 20.5.0: Sat May  8 05'
   b':10:33 PDT 2021; root:xnu-7195.121.3~9/RELEASE_X86_64 x86_64\n',
   15],
  10],
 ['lib_task_on_batch',
  b'Linux ip-172-31-138-172.us-west-2.compute.internal 4.14.232-176.381.amzn'
  b'2.x86_64 #1 SMP Wed May 19 00:31:54 UTC 2021 x86_64 x86_64 x86_64 GNU/Li'
  b'nux\n',
  10],
 ['task_on_batch_debug',
  b'Linux b3272306c25d 5.10.25-linuxkit #1 SMP Tue Mar 23 09:27:39 UTC 2021 '
  b'x86_64 x86_64 x86_64 GNU/Linux\n',
  ['task_on_default',
   b'Darwin MBP-MML7H006C8 20.5.0 Darwin Kernel Version 20.5.0: Sat May  8 05'
   b':10:33 PDT 2021; root:xnu-7195.121.3~9/RELEASE_X86_64 x86_64\n',
   15],
  10],
 {'colors-counts': File(path=s3://YOUR_BUCKET/redun-examples/aws_batch/color-counts.tsv, hash=cdd4bd24),
  'log': File(path=s3://YOUR_BUCKET/redun-examples/aws_batch/color-counts.log, hash=b7488501)}]
```

Take a closer look to understand what's happening here. First, we see the final output is a nested list with output from all the different tasks. We see a mix of operating system names such as 'Darwin' (i.e. Mac OSX) and 'Linux', which shows that we were able to execute tasks on our local machine (`task_on_default`), within a Docker container running in AWS Batch (`task_on_batch` and `lib_task_on_batch`), and within a local Docker container (`task_on_batch_debug`).

We also see output files on s3:

```
 {'colors-counts': File(path=s3://YOUR_BUCKET/redun-examples/aws_batch/color-counts.tsv, hash=cdd4bd24),
  'log': File(path=s3://YOUR_BUCKET/redun-examples/aws_batch/color-counts.log, hash=b7488501)}
```

Earlier, we saw some logs related to running AWS Batch jobs:

```
[redun] Run    Job d4d5dd00:  redun.examples.aws_batch.task_on_batch_debug(x=10) on batch_debug
[redun] AWSBatchExecutor: submit redun job 96e70822-c9a2-40ae-92a1-8b7dcf596071 as AWS Batch job d757ab69-a112-4a8f-bd74-42c222fe793b:
[redun]   job_id          = d757ab69-a112-4a8f-bd74-42c222fe793b
[redun]   job_name        = redun-example-5a8c87a70457cb3fb6df6481359245d9ca303268
[redun]   s3_scratch_path = s3://YOUR_BUCKET/redun/jobs/5a8c87a70457cb3fb6df6481359245d9ca303268
[redun]   retry_attempts  = 0
[redun]   debug           = False
```

This is telling us that a Batch job with id `d757ab69-a112-4a8f-bd74-42c222fe793b` and job name `redun-example-5a8c87a70457cb3fb6df6481359245d9ca303268` was submitted to AWS Batch. Also a job-specific scratch directory was created at `s3://YOUR_BUCKET/redun/jobs/5a8c87a70457cb3fb6df6481359245d9ca303268`. Let's a look what was stored in the scratch directory:

```
aws s3 ls s3://YOUR_BUCKET/redun/jobs/5a8c87a70457cb3fb6df6481359245d9ca303268/

2021-06-25 15:39:07         16 input
2021-06-25 15:42:26        358 output
```

Using one of redun's debugging scripts `view-pickle`, we can see how task arguments and outputs were transmitted through S3:

```
aws s3 cp s3://YOUR_BUCKET/redun/jobs/5a8c87a70457cb3fb6df6481359245d9ca303268/input - | ../../bin/view-pickle

[(10,), {}]
```

And the output should look something like:

```
aws s3 cp s3://YOUR_BUCKET/redun/jobs/5a8c87a70457cb3fb6df6481359245d9ca303268/output - | ../../bin/view-pickle

['task_on_batch',
 b'Linux ip-172-31-138-172.us-west-2.compute.internal 4.14.232-176.381.amzn2.x8'
 b'6_64 #1 SMP Wed May 19 00:31:54 UTC 2021 x86_64 x86_64 x86_64 GNU/Linux\n',
 TaskExpression('redun.examples.aws_batch.task_on_default', (15,), {}),
 10]
```

You can see how the output is partially evaluated. That is there are some expressions, such as `TaskExpression('redun.examples.aws_batch.task_on_default', (15,), {})`, that will undergo further evaluation by the scheduler.

## Code packaging

How did our Python code get inside the Docker container? It was not part of the image building we did during the setup. The answer is redun's feature called [code packaging](../../docs/source/executors.md#code-packaging).

Briefly, redun creates a tar file of all Python code found recursively from the working directory (`**/*.py`). This pattern is configurable by the configuration option [code_includes](../../docs/source/config.html#code-includes). This tar file is copied into the S3 scratch space. You can see the tar files using this command:

```
aws s3 ls s3://YOUR_BUCKET/redun/code/

2021-06-27 08:21:36       3944 00494152501e2dc191b66e03aa2d01ceb6f700d2.tar.gz
2021-01-28 04:49:01     836540 0063f85ba44fd695932cac56b6fc210e0fcf3083.tar.gz
2020-10-28 11:45:48     645496 0068bc93378411dbc5416d349e7f0950ab88d387.tar.gz
2020-11-18 10:52:07      96492 00910089d8d18d1a79d82938f0b6addd9284bf70.tar.gz

...
```

If you are interested, you can inspect the contents of a tar file using commands like this:

```
aws s3 cp s3://YOUR_BUCKET/redun/code/00494152501e2dc191b66e03aa2d01ceb6f700d2.tar.gz code.tar.gz
tar ztvf code.tar.gz

-rw-r--r--  0 rasmus staff   14400 Jun 27 08:21 ./workflow.py
```


## Data provenance in the cloud

As before, we can view the data provenance file, even those on s3:

```
redun log s3://YOUR_BUCKET/redun-examples/aws_batch/color-counts.tsv

File cdd4bd24 2021-06-26 10:30:59 w  s3://YOUR_BUCKET/redun-examples/aws_batch/color-counts.tsv
Produced by Job 2bf4256f

  Job 2bf4256f-24f3-4963-ab0e-3d2025ffbbab [ DONE ] 2021-06-26 10:36:47:  redun.postprocess_script([0, b'download: s3://YOUR_BUCKET/redun-examples/aws_batch/data.tsv to ./data\nuplo
  Traceback: Exec c1b6377d > (2 Jobs) > Job 1ba4cba2 script > Job 2bf4256f postprocess_script
  Duration: 0:00:00.24

    CallNode 9fcc6488130c75aa10887bc43209de313264c24e redun.postprocess_script
      Args:   [0, b'download: s3://YOUR_BUCKET/redun-examples/aws_batch/data.tsv to ./data\nupload: ./log.txt to s3://YOUR_BUCKET/redun-examples/aws_batch/col
      Result: {'colors-counts': File(path=s3://YOUR_BUCKET/redun-examples/aws_batch/color-counts.tsv, hash=cdd4bd24), 'log': File(path=s3://YOUR_BUCKET/redun-

    Task f6fac1c39779ca13e0f35723b46fba19c3f4ebb5 redun.postprocess_script

      def postprocess_script(result: Any, outputs: Any, temp_path: Optional[str] = None) -> Any:
          """
          Postprocess the results of a script task.
          """

          def get_file(value: Any) -> Any:
              if isinstance(value, File) and value.path == "-":
                  # File for script stdout.
                  return result
              elif isinstance(value, Staging):
                  # Staging files and dir turn into their remote versions.
                  cls = type(value.remote)
                  return cls(value.remote.path)
              else:
                  return value

          if temp_path:
              shutil.rmtree(temp_path)

          return map_nested_value(get_file, outputs)


    Upstream dataflow:

      result = {'colors-counts': File(path=s3://YOUR_BUCKET/redun-examples/aws_batch/color-counts.tsv, hash=cdd4bd24), 'log': File(path=s3://YOUR_BUCKET/redun

      result <-- <88d3fd62> count_colors_by_script(data, output_path)
        data        = <0b640108> File(path=s3://YOUR_BUCKET/redun-examples/aws_batch/data.tsv, hash=0b640108)
        output_path = <3f4340e6> 's3://YOUR_BUCKET/redun-examples/aws_batch/'
```

## Interactive debugging

To test out the pipeline using local Docker instead of AWS Batch use `debug=True` in the executor config in `redun.ini`. For example, in this example we have:

```ini
[executors.batch_debug]
type = aws_batch
image = YOUR_ACCOUN_ID.dkr.ecr.us-west-2.amazonaws.com/redun_aws_batch_example
queue = YOUR_QUEUE_NAME
s3_scratch = s3://YOUR_BUCKET/redun/
job_name_prefix = redun-example
debug = True
```

You should also be able to add a debugger breakpoint in the task as well:

```sh
@task(executor='batch_debug')
def task_on_batch_debug(x: int) -> list:
    import pdb; pdb.set_trace()
    return [
        'task_on_batch_debug',
        subprocess.check_output(['uname', '-a']),
        task_on_default(x + 5),
        x
    ]
```

## Conclusion

In this example, we saw how to run tasks on a remote cluster, such as AWS Batch. We can run both scripts as well as python tasks in Batch jobs. The scheduler takes care of routing arguments and return values between tasks.
