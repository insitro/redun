# AWS Batch array jobs

This example shows how redun's AWSBatchExecutor automatically forms [Array Jobs](https://docs.aws.amazon.com/batch/latest/userguide/array_jobs.html). To learn more about how it works, see a our recent [AWS HPC blog post](https://aws.amazon.com/blogs/hpc/how-insitro-redun-uses-advanced-aws-features/).


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

## Run the example

To run the example, use the command:

```sh
redun run workflow.py main
```

The workflow should complete as usual, however, in the logs, you'll see that redun automatically submitted arrays containing 10 jobs at a time (or whatever size `n` you specify).

```
[redun] Executor[batch]: submit 10 redun job(s) as AWS Batch job f40bce17-73e2-4516-8bad-5d25888f3305:
[redun]   array_job_id    = f40bce17-73e2-4516-8bad-5d25888f3305
[redun]   array_job_name  = redun-example-bad82b5348f443fc9f3300bf1632ed48-array
[redun]   array_size      = 10
[redun]   s3_scratch_path = s3://insitro-batch-workspace/rasmus/tmp/redun/array_jobs/bad82b5348f443fc9f3300bf1632ed48/
[redun]   retry_attempts  = 0
```
