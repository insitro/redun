Launch a remote Scheduler run
=====

This examples shows how to run a "fire-and-forget" workflow remotely. That is, instead of the central scheduler running locally on your machine (e.g. laptop) and launching individual tasks on remote executors, such as AWS Batch, `redun launch` specifies that the scheduler should also run in a remote executor.

## Setup

In the `docker` directory, run the following to make a local docker image

```shell
make build-local
```

This will create a docker image with the workflow.py file in it.

You can also setup the AWS Batch example by running the following commands:

```shell
make login
make build
make create-repo
make push
```

This will build the docker image and push it to AWS ECR.

## Running

Within this directory, run:

```shell
redun launch --executor docker_headnode redun run workflow.py main --x 1 --y 2
```

This should return after starting docker and give you the container id.  You can view the logs from the container using:
```shell
docker logs <container_id>
```

To run the redun scheduler from a AWS Batch job use the `batch` executor:

```shell
redun launch --executor batch redun run workflow.py main --x 1 --y 2
```
