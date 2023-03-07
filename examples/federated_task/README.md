# Federated tasks

This example introduces federated tasks. There are three separate locations:

* The main directory is acting as the "local repository" wishing to call the remote code,
including `./workflow.py` and `.redun/redun.ini`
* `remote_repo` - contains code that we wish to call, but which we cannot import.
* `published_config` - the shared filesystem allowing the authors of the remote repository
to share the configuration for their entrypoints.

## Running the demo

1. Create the docker image. Run this at the root of the repository: `docker build -t federated_image_test -f ./examples/federated_task/Dockerfile .`
2. In this directory, run the task local repository: `redun run workflow.py main`
