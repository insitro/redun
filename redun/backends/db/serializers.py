import abc
import datetime
from base64 import b64decode, b64encode
from collections import defaultdict
from typing import Any, Dict, Iterator, List, Optional

from dateutil.parser import parse as parse_date
from sqlalchemy.orm import aliased, joinedload, selectinload
from sqlalchemy.orm.query import Query

from redun.backends import db
from redun.backends.base import TagEntity

# Serialization version.
VERSION = 1


def serialize_timestamp(timestamp: Optional[datetime.datetime]) -> Optional[str]:
    """
    Serialize a datetime value to a timestamp string.
    """
    if timestamp:
        return str(timestamp)
    else:
        return None


def deserialize_timestamp(date_str: Optional[str]) -> Optional[datetime.datetime]:
    """
    Deserialize timestamp string to datetime value.
    """
    if date_str:
        return parse_date(date_str)
    else:
        return None


class Serializer(abc.ABC):
    """
    Base class serializer for RedunBackendDb records.
    """

    pk_field: str = ""

    def __init__(self, db_version: "Optional[db.DBVersionInfo]" = None):
        self._db_version = db_version or db.REDUN_DB_VERSIONS[-1]

    def get_pk(self, spec: dict) -> str:
        return spec[self.pk_field]

    def serialize(self, row: "db.Base") -> dict:
        raise NotImplementedError()

    def serialize_query(self, query: Query) -> Iterator[dict]:
        for row in query.all():
            yield self.serialize(row)

    def deserialize(self, spec: dict) -> "List[db.Base]":
        raise NotImplementedError()


class ExecutionSerializer(Serializer):
    pk_field = "id"

    def serialize(self, execution: "db.Base") -> dict:
        return {
            "_version": VERSION,
            "_type": "Execution",
            "id": execution.id,
            "args": execution.args,
            "job_id": execution.job_id,
        }

    def deserialize(self, spec: dict) -> "List[db.Base]":
        assert spec["_version"] == VERSION
        assert spec["_type"] == "Execution"
        return [
            db.Execution(
                id=spec["id"],
                args=spec["args"],
                job_id=spec["job_id"],
            )
        ]


class JobSerializer(Serializer):
    pk_field = "id"

    def serialize(self, job: "db.Base") -> dict:
        return {
            "_version": VERSION,
            "_type": "Job",
            "id": job.id,
            "start_time": serialize_timestamp(job.start_time),
            "end_time": serialize_timestamp(job.end_time),
            "task_hash": job.task_hash,
            "cached": job.cached,
            "status": job.status,
            "call_hash": job.call_hash,
            "parent_id": job.parent_id,
            "children": [child.id for child in job.child_jobs],
            "execution_id": job.execution_id,
        }

    def serialize_query(self, query: Query) -> Iterator[dict]:
        # Fetch parent-child links.
        Job = aliased(db.Job, query.subquery())
        ChildJob = aliased(db.Job, query.subquery())

        parent_child_pairs = (
            query.session.query(Job.id, ChildJob.id)
            .join(ChildJob, ChildJob.parent_id == Job.id)
            .order_by(ChildJob.start_time)
            .all()
        )
        parent2children = defaultdict(list)
        for parent_id, child_id in parent_child_pairs:
            parent2children[parent_id].append(child_id)

        for job in query.all():
            yield {
                "_version": VERSION,
                "_type": "Job",
                "id": job.id,
                "start_time": serialize_timestamp(job.start_time),
                "end_time": serialize_timestamp(job.end_time),
                "task_hash": job.task_hash,
                "cached": job.cached,
                "status": job.status,
                "call_hash": job.call_hash,
                "parent_id": job.parent_id,
                "children": parent2children[job.id],
                "execution_id": job.execution_id,
            }

    def deserialize(self, spec: dict) -> "List[db.Base]":
        assert spec["_version"] == VERSION
        assert spec["_type"] == "Job"
        return [
            db.Job(
                id=spec["id"],
                start_time=deserialize_timestamp(spec["start_time"]),
                end_time=deserialize_timestamp(spec["end_time"]),
                task_hash=spec["task_hash"],
                cached=spec["cached"],
                call_hash=spec["call_hash"],
                parent_id=spec["parent_id"],
                execution_id=spec["execution_id"],
            )
        ]


class ValueSerializer(Serializer):
    pk_field = "value_hash"

    def serialize(self, value: "db.Base") -> dict:
        # Up to one special subtype should exist at a time
        assert not (bool(value.file) and bool(value.task))
        subtype: Optional[Dict[str, Any]] = None
        if value.file:
            subtype = {
                "_type": "File",
                "path": value.file.path,
            }
        elif value.task:
            subtype = {
                "_type": "Task",
                "name": value.task.name,
                "namespace": value.task.namespace,
                "source": value.task.source,
            }
        return {
            "_version": VERSION,
            "_type": "Value",
            "value_hash": value.value_hash,
            "type": value.type,
            "format": value.format,
            "data": b64encode(value.value).decode("utf8"),
            "subvalues": sorted(edge.value_hash for edge in value.child_edges),
            "subtype": subtype,
        }

    def serialize_query(self, query: Query) -> Iterator[dict]:
        query = query.options(
            joinedload(db.Value.file),
            joinedload(db.Value.task),
            joinedload(db.Value.child_edges),
        )
        for row in query.all():
            yield self.serialize(row)

    def deserialize(self, spec: dict) -> "List[db.Base]":
        assert spec["_version"] == VERSION
        assert spec["_type"] == "Value"
        return (
            [
                db.Value(
                    value_hash=spec["value_hash"],
                    type=spec["type"],
                    format=spec["format"],
                    value=b64decode(spec["data"]),
                ),
            ]
            + [
                db.Subvalue(parent_value_hash=spec["value_hash"], value_hash=child_id)
                for child_id in spec["subvalues"]
            ]
            + (
                [
                    db.File(
                        value_hash=spec["value_hash"],
                        path=spec["subtype"]["path"],
                    )
                ]
                if spec.get("subtype") and spec["subtype"]["_type"] == "File"
                else []
            )
            + (
                [
                    db.Task(
                        hash=spec["value_hash"],
                        name=spec["subtype"]["name"],
                        namespace=spec["subtype"]["namespace"],
                        source=spec["subtype"]["source"],
                    )
                ]
                if spec.get("subtype") and spec["subtype"]["_type"] == "Task"
                else []
            )
        )


class CallNodeSerializer(Serializer):
    pk_field = "call_hash"

    def serialize(self, call_node: "db.Base") -> dict:
        args = {
            arg.arg_key
            or str(arg.arg_position): {
                "arg_hash": arg.arg_hash,
                "value_hash": arg.value_hash,
                "upstream": sorted(arg_result.result_call_hash for arg_result in arg.arg_results),
            }
            for arg in call_node.arguments
        }

        return {
            "_version": VERSION,
            "_type": "CallNode",
            "call_hash": call_node.call_hash,
            "task_name": call_node.task_name,
            "task_hash": call_node.task_hash,
            "args_hash": call_node.args_hash,
            "value_hash": call_node.value_hash,
            "timestamp": serialize_timestamp(call_node.timestamp),
            "args": args,
            "children": [edge.child_id for edge in call_node.child_edges],
        }

    def serialize_query(self, query: Query) -> Iterator[dict]:
        query = query.options(
            selectinload(db.CallNode.arguments).joinedload(db.Argument.arg_results),
            selectinload(db.CallNode.child_edges),
        )
        for row in query.all():
            yield self.serialize(row)

    def deserialize(self, spec: dict) -> "List[db.Base]":
        assert spec["_version"] == VERSION
        assert spec["_type"] == "CallNode"

        return (
            [
                db.CallNode(
                    call_hash=spec["call_hash"],
                    task_name=spec["task_name"],
                    task_hash=spec["task_hash"],
                    args_hash=spec["args_hash"],
                    value_hash=spec["value_hash"],
                    timestamp=deserialize_timestamp(spec["timestamp"]),
                )
            ]
            + [
                db.CallEdge(
                    parent_id=spec["call_hash"],
                    child_id=child_hash,
                    call_order=call_index,
                )
                for call_index, child_hash in enumerate(spec["children"])
            ]
            + [
                db.Argument(
                    arg_hash=arg["arg_hash"],
                    call_hash=spec["call_hash"],
                    value_hash=arg["value_hash"],
                    arg_position=int(key) if key.isdigit() else None,
                    arg_key=key if not key.isdigit() else None,
                )
                for key, arg in spec["args"].items()
            ]
            + [
                db.ArgumentResult(
                    arg_hash=arg["arg_hash"],
                    result_call_hash=upstream,
                )
                for key, arg in spec["args"].items()
                for upstream in arg["upstream"]
            ]
        )


class TagSerializer(Serializer):
    pk_field = "tag_hash"

    def serialize(self, tag: "db.Base") -> dict:
        return {
            "_version": VERSION,
            "_type": "Tag",
            "tag_hash": tag.tag_hash,
            "entity_type": tag.entity_type.name,
            "entity_id": tag.entity_id,
            "key": tag.key,
            "value": tag.value,
            "parents": [edit.parent_id for edit in tag.parent_edits],
        }

    def serialize_query(self, query: Query) -> Iterator[dict]:
        query = query.options(
            selectinload(db.Tag.parent_edits).joinedload(db.TagEdit.child),
        )
        for row in query.all():
            yield self.serialize(row)

    def deserialize(self, spec: dict) -> "List[db.Base]":
        assert spec["_version"] == VERSION
        assert spec["_type"] == "Tag"

        return [
            db.Tag(
                tag_hash=spec["tag_hash"],
                entity_type=TagEntity(spec["entity_type"]),
                entity_id=spec["entity_id"],
                key=spec["key"],
                value=spec["value"],
            )
        ] + [db.TagEdit(parent_id=parent, child_id=spec["tag_hash"]) for parent in spec["parents"]]


class RecordSerializer(Serializer):
    def __init__(self, db_version: "Optional[db.DBVersionInfo]" = None):
        super().__init__(db_version)
        self._serializers = {
            "Execution": ExecutionSerializer(db_version),
            "Job": JobSerializer(db_version),
            "CallNode": CallNodeSerializer(db_version),
            "Value": ValueSerializer(db_version),
            "Tag": TagSerializer(db_version),
        }

    def get_pk(self, spec: dict) -> str:
        return self._serializers[spec["_type"]].get_pk(spec)

    def get_subserializer(self, type: str) -> Serializer:
        return self._serializers[type]

    def serialize(self, obj: "db.Base") -> dict:
        if isinstance(obj, db.Task):
            # Use Value-version of Task for serialization.
            obj = obj.value
        return self._serializers[type(obj).__name__].serialize(obj)

    def serialize_query(self, query: Query) -> Iterator[dict]:
        [column] = query.column_descriptions
        return self._serializers[column["name"]].serialize_query(query)

    def deserialize(self, spec: dict) -> "List[db.Base]":
        return self._serializers[spec["_type"]].deserialize(spec)
