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
3. Also in this directory, run the REST demo by running the workflow directly `python workflow.py`

The last outputs something like:

```
python workflow.py
[redun] Upgrading db from version -1.0 to 3.2...
[redun] Start Execution a3e82860-e032-4965-ad11-b3e73f18e225:  redun 'redun.root_task(QuotedExpression(redun.rest_federated_task(config_name='"'"'published_config/.redun'"'"', dryrun=True, entrypoint='"'"'entrypoint_name'"'"', scratch_prefix='"'"'/tmp/redun'"'"', url='"'"'fake'"'"', x=8, y=9)))'
[redun] Run    Job 92f3f715:  redun.root_task(expr=QuotedExpression(redun.rest_federated_task(config_name='published_config/.redun', dryrun=True, entrypoint='entrypoint_name', scratch_prefix='/tmp/redun', url='fake', x=8, y=9))) on default
[redun]
[redun] | JOB STATUS 2023/03/03 23:24:27
[redun] | TASK            PENDING RUNNING  FAILED  CACHED    DONE   TOTAL
[redun] |
[redun] | ALL                   0       0       0       0       1       1
[redun] | redun.root_task       0       0       0       0       1       1
[redun]
[redun]
[redun] Execution duration: 0.03 seconds
We would have posted: {'config_name': 'published_config/.redun', 'entrypoint': 'entrypoint_name', 'input_path': '/tmp/redun/execution/41876921-8dcf-47a5-88a0-dc902c5ba3eb/input', 'execution_id': '41876921-8dcf-47a5-88a0-dc902c5ba3eb'}
[redun] Upgrading db from version -1.0 to 3.2...
[redun] Run within Executor None: redun -c remote_repo/.redun run --input /tmp/redun/execution/41876921-8dcf-47a5-88a0-dc902c5ba3eb/input --execution-id 41876921-8dcf-47a5-88a0-dc902c5ba3eb remote_repo.workflow remote_namespace.published_task
[redun] Executor[headnode]: submit redun job 2bd7bd10-6ee5-4392-ac61-f1077846a187 as Docker container 6fb4fa2fb97a98a51ea6e2c38f0615f5d1fe54ccbb461f2d49d5a08ed3bc252e:
[redun]   container_id = 6fb4fa2fb97a98a51ea6e2c38f0615f5d1fe54ccbb461f2d49d5a08ed3bc252e
[redun]   scratch_path = /tmp/redun/jobs/64115d38b9358e919bef24bca29f0ecb9e0d16f1
[redun]
[redun] Upgrading db from version -1.0 to 3.2...
[redun] Run within Executor None: redun -c remote_repo/.redun run --input /tmp/redun/execution/df92329d-27a9-4791-b0e7-ad70c3b0a195/input --execution-id df92329d-27a9-4791-b0e7-ad70c3b0a195 remote_repo.workflow remote_namespace.published_task
[redun] Executor[headnode]: submit redun job 652f5980-02fb-44c3-89b9-859072e994b1 as Docker container 750e4f0dc63bc5dd6dac7e86d7aa52d7bbee89ee09c2006dcb6198a2842f2089:
[redun]   container_id = 750e4f0dc63bc5dd6dac7e86d7aa52d7bbee89ee09c2006dcb6198a2842f2089
[redun]   scratch_path = /tmp/redun/jobs/996db357f6ab17c6d9485e88a1bc246dd3f1dc2e
[redun]
[redun] Upgrading db from version -1.0 to 3.2...
[redun] Run within Executor None: redun -c remote_repo/.redun run --input /tmp/redun/execution/41509bb0-3c28-414c-b9a5-4be8d7b3a285/input --execution-id 41509bb0-3c28-414c-b9a5-4be8d7b3a285 remote_repo.workflow remote_namespace.published_task
[redun] Executor[headnode]: submit redun job 0ed8a624-0058-4e4e-95ac-235aab6aaa62 as Docker container 4f6dc01054c59e4a23676fca46a1bc71c46b1eebea8a200a815409a140c8c853:
[redun]   container_id = 4f6dc01054c59e4a23676fca46a1bc71c46b1eebea8a200a815409a140c8c853
[redun]   scratch_path = /tmp/redun/jobs/546840c2f68b04819128c44d666a3639388f7713
[redun]
```

The output can be found from the scratch paths:
```shell
cat /tmp/redun/jobs/546840c2f68b04819128c44d666a3639388f7713/output
25
```
