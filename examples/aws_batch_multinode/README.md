# AWS Batch multi-node parallel jobs

This example shows how to run redun tasks that communicate with each other using [AWS Batch multi-node parallel jobs](https://docs.aws.amazon.com/batch/latest/userguide/multi-node-parallel-jobs.html).


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

Eventually, you should see an output something like:

```py
["Hi, I'm node 0.",
 "Hi, I'm node 4.",
 "Hi, I'm node 2.",
 "Hi, I'm node 3.",
 "Hi, I'm node 1."]
```
