# Local Docker-based tasks

This example shows how to run redun tasks within their own Docker containers. This can be helpful for isolating the execution environment for jobs or just testing a Docker image locally before running in the cloud (e.g. AWS Batch Executor).

This example follows the same format as [../05_aws_batch](../05_aws_batch), so please see that example and [workflow.py](workflow.py) for more details.

## Setup

When running full redun tasks in Docker, we need to install redun in our Docker image. To avoid [credential complexities](http://blog.oddbit.com/post/2019-02-24-docker-build-learns-about-secr/) when installing from private github repos, in this example, we will just copy redun into the container. Use the following make command to do so:

```sh
cd docker/
make setup
```

Now, we can use the following make command to build our Docker image.

```sh
make build
```
