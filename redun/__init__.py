from typing import TYPE_CHECKING

from redun.file import Dir, File, ShardedS3Dataset
from redun.handle import Handle
from redun.namespace import get_current_namespace, namespace
from redun.scheduler import (
    Scheduler,
    apply_tags,
    catch,
    cond,
    get_current_scheduler,
    merge_handles,
    set_current_scheduler,
    throw,
)
from redun.scripting import script
from redun.task import PartialTask, Task, get_task_registry, task
from redun.version import version

from redun.context import get_context
from redun.executors.base import register_executor


if TYPE_CHECKING:
    from redun.executors.alias import AliasExecutor
    from redun.executors.aws_batch import AWSBatchExecutor
    from redun.executors.aws_glue import AWSGlueExecutor
    from redun.executors.docker import DockerExecutor

    try:
        from redun.executors.k8s import K8SExecutor
    except (ImportError, ModuleNotFoundError):
        # Skip k8s executor if kubernetes is not installed.
        pass
    try:
        from redun.executors.gcp_batch import GCPBatchExecutor
    except (ImportError, ModuleNotFoundError):
        # Skip gcp_batch executor if google-cloud-batch is not installed.
        pass
    from redun.executors.local import LocalExecutor
else:
    AliasExecutor = register_executor("alias", "redun.executors.alias.AliasExecutor")
    AWSBatchExecutor = register_executor("aws_batch", "redun.executors.aws_batch.AWSBatchExecutor")
    AWSGlueExecutor = register_executor("aws_glue", "redun.executors.aws_glue.AWSGlueExecutor")
    DockerExecutor = register_executor("docker", "redun.executors.docker.DockerExecutor")
    K8SExecutor = register_executor("k8s", "redun.executors.k8s.K8SExecutor")
    GCPBatchExecutor = register_executor("gcp_batch", "redun.executors.gcp_batch.GCPBatchExecutor")
    LocalExecutor = register_executor("local", "redun.executors.local.LocalExecutor")


__version__ = version
__all__ = [
    "Dir",
    "File",
    "Handle",
    "PartialTask",
    "Scheduler",
    "ShardedS3Dataset",
    "Task",
    "apply_tags",
    "catch",
    "cond",
    "get_current_namespace",
    "get_current_scheduler",
    "get_task_registry",
    "merge_handles",
    "namespace",
    "script",
    "set_current_scheduler",
    "task",
    "throw",
    "version",
]
