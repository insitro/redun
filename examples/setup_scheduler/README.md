# Setup scheduler example

This example shows how a project can use tasks from a workflow library, `workflow_lib`. The library configures tasks with task-specific details such as Docker image and memory usage. However, it specifically leaves the details of the executor undefined so that users can specify account-specific information such as queues and roles.

This example also shows how complex redun configuration can be defined using a `setup_scheduler()` function, which is an optional function used  by the redun CLI to configure the redun scheduler at runtime. This can be useful in situations where users prefer to use python to configure scheduler instead of the `.redun/redun.ini` configuration file.

To run this example, first create the necessary docker images following the instructions in [../05_aws_batch/README.md](../05_aws_batch/README.md). Then run the example workflow using:

```sh
redun run workflow.py main
```

To use the `account1` profile, run:

```sh
redun --setup profile=account1 run workflow.py main
```
