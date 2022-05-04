import abc
import enum
import typing
from datetime import datetime
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

from redun.handle import Handle
from redun.hashing import hash_struct
from redun.utils import MultiMap
from redun.value import TypeRegistry, get_type_registry

if typing.TYPE_CHECKING:
    from redun.scheduler import Job
    from redun.scheduler import Task as BaseTask

NULL = object()


def calc_call_hash(
    task_hash: str, args_hash: str, result_hash: str, child_call_hashes: List[str]
) -> str:
    return hash_struct(["CallNode", task_hash, args_hash, result_hash, sorted(child_call_hashes)])


# Types.
TagMap = MultiMap[str, Any]
KeyValue = Tuple[str, Any]


class TagEntityType(enum.Enum):
    Execution = "Execution"
    Job = "Job"
    CallNode = "CallNode"
    Task = "Task"
    Value = "Value"
    Null = "Null"


class RedunBackend(abc.ABC):
    def __init__(self, type_registry: Optional[TypeRegistry] = None):
        self.type_registry: TypeRegistry = type_registry or get_type_registry()

    def load(self, migrate: Optional[bool] = None) -> None:
        pass

    def calc_current_nodes(self, task_hashes: Iterable[str]) -> None:
        pass

    def record_call_node(
        self,
        task_name: str,
        task_hash: str,
        args_hash: str,
        expr_args: Tuple[Tuple, dict],
        eval_args: Tuple[Tuple, dict],
        result_hash: str,
        child_call_hashes: List[str],
    ) -> str:
        pass

    def record_args(
        self, call_hash: str, expr_args: Tuple[Tuple, dict], eval_args: Tuple[Tuple, dict]
    ) -> None:
        pass

    def record_value(self, value: Any, data=None) -> str:
        pass

    def get_call_hash(self, task_hash: str, args_hash: str) -> Optional[str]:
        pass

    def get_eval_cache(self, eval_hash: str) -> Tuple[Any, bool]:
        pass

    def set_eval_cache(
        self, eval_hash: str, task_hash: str, args_hash: str, value: Any, value_hash: str = None
    ):
        pass

    def get_cache(self, call_hash: str) -> Any:
        pass

    def explain_cache_miss(self, task: "BaseTask", args_hash: str) -> Optional[Dict[str, Any]]:
        pass

    def get_value(self, value_hash: str) -> Any:
        pass

    def advance_handle(self, handles: List[Handle], child_handle: Handle):
        pass

    def rollback_handle(self, handle: Handle) -> None:
        pass

    def is_valid_handle(self, handle: Handle) -> bool:
        pass

    def record_execution(self, args: List[str]) -> str:
        pass

    def record_job_start(self, job: "Job", now: Optional[datetime] = None) -> Any:
        pass

    def record_job_end(
        self, job: "Job", now: Optional[datetime] = None, status: Optional[str] = None
    ) -> None:
        pass

    def record_tags(
        self,
        entity_type: TagEntityType,
        entity_id: str,
        tags: Iterable[Tuple[str, Any]],
        parents: Iterable = (),
        update: bool = False,
    ):
        pass

    def delete_tags(
        self, entity_id: str, tags: Iterable[KeyValue], keys: Iterable[str] = ()
    ) -> List[Tuple[str, str, str, Any]]:
        pass

    def update_tags(
        self,
        entity_type: TagEntityType,
        entity_id: str,
        old_keys: Iterable[str],
        new_tags: Iterable[KeyValue],
    ) -> List[Tuple[str, str, str, Any]]:
        pass

    def get_tags(self, entity_ids: List[str]) -> Dict[str, TagMap]:
        pass

    def iter_record_ids(self, root_ids: Iterable[str]) -> Iterator[str]:
        pass

    def get_records(self, ids: Iterable[str], sorted=True) -> Iterable[dict]:
        pass

    def put_records(self, records: Iterable[dict]) -> int:
        pass
