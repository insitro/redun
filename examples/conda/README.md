# Run tasks using conda in a docker container

It is common, especially with scientific code, to use [conda](https://conda.io/) to install Python dependencies. In this example, we show how to use conda to install the dependencies within a Docker container. This example runs the Docker container locally, but once a Docker image is made it can be used to run elsewhere, such as [AWS Batch](../05_aws_batch) or [Google Batch](../gcp_batch).


## Setup

When running full redun tasks in a Docker container, we need to install redun (and any other dependencies) in a Docker image. This example provides an example Dockerfile and Makefile for creating an example Docker image. Use the following commands to make an example Docker image

```sh
cd docker/
make build
```

This will make a Docker image called `redun_example_conda`.


## Running the example

We can run the example with the command:

```sh
redun run workflow.py main
```

Refer to the configuration file [redun.ini](.redun/redun.ini) to see how the Docker image was configured.
