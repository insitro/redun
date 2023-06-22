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

__version__ = version
