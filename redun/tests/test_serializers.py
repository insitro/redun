import json
import pickle
from base64 import b64decode, b64encode
from collections import defaultdict
from datetime import datetime
from typing import Callable, Dict, Iterable, List, Tuple, cast
from unittest.mock import Mock, patch

from sqlalchemy.orm import Session

from redun import File as RedunFile
from redun import Scheduler, apply_tags, task
from redun.backends.base import TagEntity
from redun.backends.db import (
    Base,
    CallEdge,
    CallNode,
    Execution,
    Job,
    RedunBackendDb,
    Tag,
    TagEdit,
    Value,
    parse_db_version,
)
from redun.backends.db.serializers import RecordSerializer
from redun.tests.utils import listen_queries
from redun.utils import pickle_dumps

DB_VERSIONS = [
    parse_db_version("3.0"),
    parse_db_version("3.1"),
]


def serialize(obj: Base) -> dict:
    return RecordSerializer().serialize(obj)


def deserialize(spec: dict) -> List[Base]:
    return RecordSerializer().deserialize(spec)


def dump_record(record: dict) -> str:
    """
    Helper method for comparing records.
    """
    if "children" in record:
        record["children"].sort()
    if "parents" in record:
        record["parents"].sort()
    return json.dumps(record, sort_keys=True)


def group_edges(edges: Iterable[Tuple[str, Base, str]]) -> Dict[str, List[str]]:
    """
    Group serialization edges by edge type.
    """
    edge_groups = defaultdict(list)
    for edge_type, _, record_id in edges:
        edge_groups[edge_type].append(record_id)
    return edge_groups


def test_serializers():
    # Value.
    value = Value(
        value_hash="222abc",
        type="builtins.int",
        format="application/python-pickle",
        value=pickle_dumps(10),
    )
    expected_spec = {
        "_version": 1,
        "_type": "Value",
        "value_hash": "222abc",
        "type": "builtins.int",
        "format": "application/python-pickle",
        "data": b64encode(pickle_dumps(10)).decode("utf8"),
        "subvalues": [],
        "subtype": None,
    }
    assert serialize(value) == expected_spec
    assert serialize(deserialize(expected_spec)[0]) == expected_spec

    # CallNode.
    call_node = CallNode(
        call_hash="333abc",
        task_name="ns.task1",
        task_hash="123abc",
        args_hash="444abc",
        value_hash="222abc",
        timestamp=datetime(2020, 1, 1),
    )
    child_node = CallNode(
        call_hash="555abc",
        task_name="ns.task1",
        task_hash="123abc",
        args_hash="444abc",
        value_hash="222abc",
        timestamp=datetime(2020, 1, 1),
    )
    expected_spec = {
        "_version": 1,
        "_type": "CallNode",
        "call_hash": "333abc",
        "task_name": "ns.task1",
        "task_hash": "123abc",
        "args_hash": "444abc",
        "value_hash": "222abc",
        "timestamp": "2020-01-01 00:00:00",
        "args": {},
        "children": ["555abc"],
    }

    with patch(
        "redun.backends.db.CallNode.child_edges",
        new_callable=lambda: [Mock(child_id=child_node.call_hash)],
    ):
        spec = serialize(call_node)
    assert spec == expected_spec

    [call_node2, edge] = deserialize(expected_spec)
    assert isinstance(call_node2, CallNode)
    assert isinstance(edge, CallEdge)

    with patch(
        "redun.backends.db.CallNode.child_edges",
        new_callable=lambda: [Mock(child_id=child_node.call_hash)],
    ):
        spec = serialize(call_node2)
    assert spec == expected_spec


def test_special_value_serialization(scheduler: Scheduler) -> None:
    @task(version="2")
    def main(file: RedunFile):
        return file

    f = RedunFile("/tmp/file1.txt")

    scheduler.run(main(f))

    [file_spec, task_spec] = list(scheduler.backend.get_records([f.hash, main.hash], sorted=True))
    expected_file_spec = {
        "_version": 1,
        "_type": "Value",
        "value_hash": f.hash,
        "type": "redun.File",
        "format": "application/python-pickle",
        "data": b64encode(pickle_dumps(f)).decode("utf8"),
        "subvalues": [],
        "subtype": {"_type": "File", "path": "/tmp/file1.txt"},
    }
    expected_task_spec = {
        "_version": 1,
        "_type": "Value",
        "value_hash": main.hash,
        "type": "redun.Task",
        "format": "application/python-pickle",
        "data": b64encode(pickle_dumps(main)).decode("utf8"),
        "subvalues": [],
        "subtype": {
            "_type": "Task",
            "name": "main",
            "namespace": "",
            "source": "    def main(file: RedunFile):\n        return file\n",
        },
    }
    assert file_spec == expected_file_spec
    assert task_spec == expected_task_spec


def test_serialize_tags() -> None:
    """
    Ensure Tags can be serialized and deserialized.
    """
    tag = Tag(
        tag_hash="123",
        entity_type=TagEntity.Value,
        entity_id="1",
        key="env",
        value=["prod", "stg"],
    )
    parent_tag = Tag(
        tag_hash="222",
        entity_type=TagEntity.Value,
        entity_id="1",
        key="env",
        value=["prod"],
    )

    with patch(
        "redun.backends.db.Tag.parent_edits",
        new_callable=lambda: [Mock(parent_id=parent_tag.tag_hash)],
    ):
        spec = serialize(tag)
    assert spec == {
        "_type": "Tag",
        "_version": 1,
        "entity_id": "1",
        "entity_type": "Value",
        "key": "env",
        "tag_hash": "123",
        "value": ["prod", "stg"],
        "parents": ["222"],
    }

    [tag2, tag_edit] = deserialize(spec)
    assert isinstance(tag2, Tag)
    assert isinstance(tag_edit, TagEdit)

    with patch(
        "redun.backends.db.Tag.parent_edits",
        new_callable=lambda: [Mock(parent_id=parent_tag.tag_hash)],
    ):
        spec2 = serialize(tag2)
    assert spec == spec2


def test_get_records(scheduler: Scheduler) -> None:
    @task()
    def task1(x):
        return x + 1

    @task()
    def main(tsk: Callable[[int], int]):
        return [tsk(1), tsk(2)]

    # Record some tasks and calls.
    scheduler.run(main(task1))

    # Fetch records.
    assert isinstance(scheduler.backend, RedunBackendDb)
    [record] = list(scheduler.backend.get_records([main.hash]))
    assert record["value_hash"] == main.hash

    # Transfer record to another backend.
    scheduler2 = Scheduler()
    assert isinstance(scheduler2.backend, RedunBackendDb)
    assert list(scheduler2.backend.get_records([main.hash])) == []
    scheduler2.backend.put_records([record])
    [record2] = list(scheduler2.backend.get_records([main.hash]))
    assert record2 == record


def test_iter_record_ids(scheduler: Scheduler, backend: RedunBackendDb, session: Session) -> None:
    @task()
    def task1():
        return 1

    @task()
    def task2(x):
        return [x, task1()]

    @task()
    def main():
        return [task2(task1()), task2(2)]

    # Record some tasks and calls.
    scheduler.run(main())

    # Get execution.
    execution = session.query(Execution).one()
    [execution_record] = list(backend.get_records([execution.id]))
    assert execution_record["_type"] == "Execution"

    # Get the job edge.
    [(edge_name, model, job_id)] = [
        (edge_name, model, job_id)
        for edge_name, model, job_id in backend.get_child_record_ids([(Execution, execution.id)])
        if model == Job
    ]

    assert edge_name == "Execution.job"
    assert job_id == execution_record["job_id"]
    [job] = list(backend.get_records([job_id]))
    assert job["_type"] == "Job"

    [
        (_, _, task_hash),
        (_, _, call_hash),
        (_, _, job_id1),
        (_, _, job_id2),
        (_, _, job_id3),
    ] = list(backend.get_child_record_ids([(Job, job_id)]))

    assert task_hash == main.hash

    # Get root call node.
    call_node = session.query(CallNode).filter_by(task_name="main").first()
    assert call_hash == call_node.call_hash

    # Fetch records.
    edge_groups = group_edges(backend.get_child_record_ids([(CallNode, call_node.call_hash)]))

    [value_hash] = edge_groups["CallNode.result"]
    [value_record] = backend.get_records([value_hash])

    assert pickle.loads(b64decode(value_record["data"])) == [[1, 1], [2, 1]]

    # Get task2 children.
    child_id1, child_i2, child_id3 = sorted(edge_groups["CallNode.child_call_node"])
    edge_groups2 = group_edges(backend.get_child_record_ids([(CallNode, child_id1)]))
    edge_groups3 = group_edges(backend.get_child_record_ids([(CallNode, child_id3)]))

    # Upstream call_node should be a child record as well.
    assert (
        list(backend.get_records([edge_groups2["CallNode.child_call_node"][0]]))[0]["task_name"]
        == "task1"
    )

    # Grandchildren should be task1.
    assert (
        edge_groups2["CallNode.child_call_node"][0] == edge_groups3["CallNode.child_call_node"][0]
    )

    record_ids = list(backend.iter_record_ids([execution.id]))
    records = list(backend.get_records(record_ids))

    scheduler2 = Scheduler()
    backend2 = cast(RedunBackendDb, scheduler2.backend)
    backend2.put_records(reversed(records))

    record_ids2 = backend2.iter_record_ids([execution.id])
    records2 = list(backend2.get_records(record_ids2))

    def dumps(record):
        if "children" in record:
            record["children"].sort()
        if "parents" in record:
            record["parents"].sort()
        return json.dumps(record, sort_keys=True)

    # Deep equal.
    assert set(map(dumps, records)) == set(map(dumps, records2))

    # Puts are idempotent.
    backend2.put_records(records)

    record_ids2 = backend2.iter_record_ids([execution.id])
    records2 = list(backend2.get_records(record_ids2))

    # Deep equal.
    assert set(map(dumps, records)) == set(map(dumps, records2))


def test_get_records_queries(
    scheduler: Scheduler, backend: RedunBackendDb, session: Session
) -> None:
    """
    iter_record_ids and get_records should use O(1) queries.
    """

    @task()
    def task1(i):
        return i + 1

    @task()
    def task2(x):
        return [x, task1(x)]

    @task()
    def main(n):
        return [task2(task1(i)) for i in range(n)]

    # Record some tasks and calls.
    scheduler.run(main(1))

    # Get latest execution.
    execution1 = (
        session.query(Execution)
        .join(Job, Job.id == Execution.job_id)
        .order_by(Job.start_time.desc())
        .first()
    )

    with listen_queries(backend.engine) as id_queries1:
        record_ids = list(backend.iter_record_ids([execution1.id]))

    with listen_queries(backend.engine) as record_queries1:
        records1 = list(backend.get_records(record_ids))

    # Record 10x more tasks and calls.
    scheduler.run(main(10))

    # Get latest execution.
    execution2 = (
        session.query(Execution)
        .join(Job, Job.id == Execution.job_id)
        .order_by(Job.start_time.desc())
        .first()
    )

    with listen_queries(backend.engine) as id_queries2:
        record_ids = list(backend.iter_record_ids([execution2.id]))

    with listen_queries(backend.engine) as record_queries2:
        records2 = list(backend.get_records(record_ids))

    # Even though we have ~10x more data in the CallGraph, we should use O(1)
    # queries to fetch it.
    assert len(id_queries1) == len(id_queries2)
    assert len(record_queries1) == len(record_queries2)
    assert len(records2) > len(records1)


def test_sync_tags(scheduler: Scheduler, backend: RedunBackendDb, session: Session) -> None:
    @task(tags=[("env", "prod")])
    def task1():
        return apply_tags(10, [("user", "alice"), ("release", "final")])

    assert scheduler.run(task1(), tags=[("project", "acme")]) == 10
    execution = session.query(Execution).one()

    # Update a tag.
    backend.update_tags(TagEntity.Execution, execution.id, ["project"], [("project", "skunk")])

    # Delete a tag.
    value = session.query(Value).filter(Value.type == "builtins.int").one()
    backend.delete_tags(value.value_hash, [("release", "final")])

    assert backend.get_tags([execution.id])[execution.id]["project"] == ["skunk"]

    record_ids = list(backend.iter_record_ids([execution.id]))
    records = list(backend.get_records(record_ids))
    tags = [record for record in records if record["_type"] == "Tag"]

    assert any(
        tag["entity_type"] == "Execution"
        and tag["key"] == "project"
        and tag["value"] == "skunk"
        and len(tag["parents"]) == 1
        for tag in tags
    )
    assert any(
        tag["entity_type"] == "Execution"
        and tag["key"] == "project"
        and tag["value"] == "acme"
        and len(tag["parents"]) == 0
        for tag in tags
    )
    assert any(tag["entity_type"] == "Value" and tag["key"] == "user" for tag in tags)
    assert any(tag["entity_type"] == "Task" and tag["key"] == "env" for tag in tags)
    assert any(tag["entity_type"] == "Job" and tag["key"] == "env" for tag in tags)

    # Sync records to second backend.
    scheduler2 = Scheduler()
    backend2 = cast(RedunBackendDb, scheduler2.backend)
    backend2.put_records(reversed(records))

    # Export records from second backend.
    record_ids2 = backend2.iter_record_ids([execution.id])
    records2 = list(backend2.get_records(record_ids2))
    tags2 = [record for record in records2 if record["_type"] == "Tag"]

    # Ensure all tags are present.
    assert set(map(dump_record, tags)) == set(map(dump_record, tags2))

    # Tag update should also be synced.
    assert backend2.get_tags([execution.id])[execution.id]["project"] == ["skunk"]

    # Tag delete should also be synced.
    assert backend2.get_tags([value.value_hash])[value.value_hash] == {"user": ["alice"]}
