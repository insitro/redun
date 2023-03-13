import abc
import enum
import typing
from datetime import datetime
from typing import Any, Dict, Iterable, Iterator, List, Optional, Set, Tuple

from redun.handle import Handle
from redun.task import CacheCheckValid, CacheResult, CacheScope
from redun.utils import MultiMap
from redun.value import TypeRegistry, get_type_registry

if typing.TYPE_CHECKING:
    from redun.scheduler import Job
    from redun.scheduler import Task as BaseTask

NULL = object()

# Types.
TagMap = MultiMap[str, Any]
KeyValue = Tuple[str, Any]


class TagEntity(enum.Enum):
    """
    Entity types for a CallGraph Tag.
    """

    Execution = "Execution"
    Job = "Job"
    CallNode = "CallNode"
    Task = "Task"
    Value = "Value"
    Null = "Null"


class RedunBackend(abc.ABC):
    """
    A Backend manages the CallGraph (provenance) and cache for a Scheduler.

    This is the abstract backend interface. See `db/__init__.py` for example of
    a concrete interface for persisting data in a database.
    """

    def __init__(self, type_registry: Optional[TypeRegistry] = None):
        self.type_registry: TypeRegistry = type_registry or get_type_registry()

    def load(self, migrate: Optional[bool] = None) -> None:
        """
        Load the backend for use.

        Parameters
        ----------
        migrate : Optional[bool]
            If None, defer to automigration config options. If True, perform
            migration after establishing database connection.
        """
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
        """
        Record a completed CallNode.

        Parameters
        ----------
        task_name : str
            Fullname (with namespace) of task.
        task_hash : str
            Hash of task.
        args_hash : str
            Hash of all arguments of the call.
        expr_args : Tuple[Tuple, dict]
            Original expressions for the task arguments. These expressions are used
            to record the full upstream dataflow.
        eval_args : Tuple[Tuple, dict]
            The fully evaluated arguments of the task arguments.
        result_hash : str
            Hash of the result value of the call.
        child_call_hashes : List[str]
            call_hashes of any child task calls.

        Returns
        -------
        str
            The call_hash of the new CallNode.
        """
        pass

    def record_value(self, value: Any, data: Optional[bytes] = None) -> str:
        """
        Record a Value into the backend.

        Parameters
        ----------
        value : Any
            A value to record.
        data : Optional[bytes]
            Byte stream to record. If not given, usual value serialization is used.

        Returns
        -------
        str
            value_hash of recorded value.
        """
        pass

    def get_value(self, value_hash: str) -> Any:
        """
        Returns a Value from the datastore using the value content address (value_hash).

        Parameters
        ----------
        value_hash : str
            Hash of Value to fetch from ValueStore.

        Returns
        -------
        result, is_cached : Tuple[Any, bool]
            Returns the result `value` and `is_cached=True` if the value is in the
            ValueStore, otherwise returns (None, False).
        """
        pass

    def check_cache(
        self,
        task_hash: str,
        args_hash: str,
        eval_hash: str,
        execution_id: str,
        scheduler_task_hashes: Set[str],
        cache_scope: CacheScope,
        check_valid: CacheCheckValid,
        allowed_cache_results: Optional[Set[CacheResult]] = None,
    ) -> Tuple[Any, Optional[str], CacheResult]:
        """
        Check cache for recorded values.

        This methods checks for several types of cached values when performing
        graph reduction on the Expression Graph.

        1. Common Subexpression Elimination (CSE): If there is a previously recorded
           Job in the same Execution, `execution_id`, that is equivalent
           (same `eval_hash`), its final result is replayed. This check is
           always performed first.

        2. Evaluation cache: When `check_valid="CacheCheckValid.FULL", previous single
           reduction evaluations are consulted by their `eval_hash`. If a match is found,
           the resulting single reduction is replayed. This is the most common
           cache method.

        3. Call cache: When `check_valid="CacheCheckValid.SHALLOW"`, previous *current*
           CallNodes are consulted by their `eval_hash` (i.e. `task_hash` and `args_hash`).
           If a match is found, its final result is replayed. A CallNode is
           considered *current* only if its Task and all tasks for child CallNodes are current,
           as defined by having their task_hash in the provided set, `scheduler_task_hashes`.
           This cache method is typically used as an optimization, where the validity of all
           intermediate arguments and results within call subtree do not need to
           be checked (hence checking is "shallow").

        Parameters
        ----------
        task_hash : str
            Hash of Task used in the call.
        args_hash : str
            Hash of all arguments used in the call.
        eval_hash : str
            A hash of the combination of the task_hash and args_hash.
        execution_id : str
            ID of the current Execution. This is used to perform the CSE check.
        scheduler_task_hashes : Set[str]
            The set of task hashes known to the scheduler, used for checking the Call cache.
        cache_scope : CacheScope
            What scope of cache hits to try. `CacheScopeType.CSE` only allows CSE, and
            `CacheScopeType.NONE` disables all caching.
        check_valid : CacheCheckValid,
            If set to `CacheCheckValid.FULL` perform Evaluation cache check or if
            `CacheCheckValid.SHALLOW` perform Call cache check. See above for more details.
        allowed_cache_results : Optional[Set[CacheResult]]
            If provided, further restrict the allowed types of results.

        Returns
        -------
        (result, call_hash, cache_type) : Tuple[Any, Optional[str], CacheResult]
            `result` is the cached result, or None if no result was found.
            `call_hash` is the hash of the CallNode used if CSE or Call cache
            methods are used. `cache_type` specifies which cache method was used
            (CSE, SINGLE, ULTIMATE) or MISS if no cached result is found.
        """
        pass

    def set_eval_cache(
        self, eval_hash: str, task_hash: str, args_hash: str, value: Any, value_hash: str = None
    ) -> None:
        """
        Sets a new value in the Evaluation cache.

        Parameters
        ----------
        eval_hash : str
            A hash of the combination of the task_hash and args_hash.
        task_hash : str
            Hash of Task used in the call.
        args_hash : str
            Hash of all arguments used in the call.
        value : Any
            Value to record in cache.
        value_hash : str
            Hash of value to record in cache.
        """
        pass

    def explain_cache_miss(self, task: "BaseTask", args_hash: str) -> Optional[Dict[str, Any]]:
        """
        Determine the reason for a cache miss.
        """
        pass

    def advance_handle(self, handles: List[Handle], child_handle: Handle):
        """
        Record parent-child relationships between Handles.
        """
        pass

    def rollback_handle(self, handle: Handle) -> None:
        """
        Rollback all descendant handles.
        """
        pass

    def is_valid_handle(self, handle: Handle) -> bool:
        """
        A handle is valid if it current or ancestral to the current handle.
        """
        pass

    def record_execution(self, exec_id, args: List[str]) -> None:
        """
        Records an Execution to the backend.

        Parameters
        ----------
        exec_id : str
            The id of the execution.
        args : List[str]
            Arguments used on the command line to start Execution.

        Returns
        -------
        str
            UUID of new Execution.
        """
        pass

    def record_job_start(self, job: "Job", now: Optional[datetime] = None) -> Any:
        """
        Records the start of a new Job.
        """
        pass

    def record_job_end(
        self, job: "Job", now: Optional[datetime] = None, status: Optional[str] = None
    ) -> None:
        """
        Records the end of a Job.

        Create the job if needed, in which case the job will be recorded with
        `start_time==end_time`
        """
        pass

    def get_job(self, job_id: str) -> Optional[dict]:
        """
        Returns details for a Job.
        """
        pass

    def record_tags(
        self,
        entity_type: TagEntity,
        entity_id: str,
        tags: Iterable[Tuple[str, Any]],
        parents: Iterable = (),
        update: bool = False,
        new: bool = False,
    ):
        """
        Record tags for an entity (Execution, Job, CallNode, Task, Value).

        Parameters
        ----------
        entity_type : TagEntity
            The type of the tagged entity (Execution, Job, etc).
        entity_id : str
            The id of the tagged entity.
        tags : Iterable[KeyValue]
            An iterable of key-value pairs to create as tags.
        parents : Iterable[str]
            Ids of tags to be superseded by the new tags.
        update : bool
            If True, automatically supersede any existing tags with keys matching those in `tags`.
            This also implies `new=True`.
        new : bool
            If True, force tags to be current.

        Returns
        -------

        [(tag_hash, entity_id, key, value)] : List[Tuple[str, str, str, Any]]
            Returns a list of the created tags.
        """
        pass

    def delete_tags(
        self, entity_id: str, tags: Iterable[KeyValue], keys: Iterable[str] = ()
    ) -> List[Tuple[str, str, str, Any]]:
        """
        Delete tags.
        """
        pass

    def update_tags(
        self,
        entity_type: TagEntity,
        entity_id: str,
        old_keys: Iterable[str],
        new_tags: Iterable[KeyValue],
    ) -> List[Tuple[str, str, str, Any]]:
        """
        Update tags.
        """
        pass

    def get_tags(self, entity_ids: List[str]) -> Dict[str, TagMap]:
        """
        Get the tags of an entity (Execution, Job, CallNode, Task, Value).
        """
        pass

    def get_records(self, ids: Iterable[str], sorted=True) -> Iterable[dict]:
        """
        Returns serialized records for the given ids.

        Parameters
        ----------
        ids : Iterable[str]
            Iterable of record ids to fetch serialized records.
        sorted : bool
            If True, return records in the same order as the ids (Default: True).
        """
        pass

    def put_records(self, records: Iterable[dict]) -> int:
        """
        Writes records to the database and returns number of new records written.
        """
        pass

    def iter_record_ids(self, root_ids: Iterable[str]) -> Iterator[str]:
        """
        Iterate the record ids of descendants of root_ids.
        """
        pass
