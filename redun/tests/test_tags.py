import inspect
from typing import Any

import pytest
from sqlalchemy.orm import Session

from redun import Scheduler, apply_tags, task
from redun.backends.base import TagEntity
from redun.backends.db import Execution, Job, RedunBackendDb, Tag, Task, Value
from redun.tags import (
    ANY_VALUE,
    format_tag_key_value,
    format_tag_value,
    parse_tag_key_value,
    parse_tag_value,
)


@pytest.mark.parametrize(
    "text,value",
    [
        ("10", 10),
        ("1.2", 1.2),
        ("1.2e3", 1.2e3),
        ("true", True),
        ("false", False),
        ("null", None),
        ("", None),
        (" ", " "),
        ('""', ""),
        ('"10"', "10"),
        ('"true"', "true"),
        ("[1, 2, 3]", [1, 2, 3]),
        ('{"a": 1, "b": 2}', {"a": 1, "b": 2}),
        ("hello", "hello"),
        ("10a", "10a"),
        ("one two", "one two"),
        ("one [two]", "one [two]"),
        # Invalid tag values.
        ("[hello]", ValueError),
        ("[1, 2, 3", ValueError),
        ('"1, 2" 3', ValueError),
    ],
)
def test_parse_tag_value(text: str, value: Any) -> None:
    """
    Should be able to parse custom formats for tag values.
    """
    if inspect.isclass(value) and issubclass(value, Exception):
        with pytest.raises(value):
            parse_tag_value(text)
    else:
        assert parse_tag_value(text) == value


@pytest.mark.parametrize(
    "text,key_value",
    [
        ("key=10", ("key", 10)),
        ("project=acme", ("project", "acme")),
        ("project", ("project", ANY_VALUE)),
        ("equation=x=y", ("equation", "x=y")),
        ("key=", ("key", None)),
        ("a b=c d", ("a b", "c d")),
        ("=10", ValueError),
        ("", ValueError),
        ("10=10", ("10", 10)),
    ],
)
def test_parse_tag_key_value(text: str, key_value: Any) -> None:
    """
    Should be able to parse tag key value pairs.
    """
    if inspect.isclass(key_value) and issubclass(key_value, Exception):
        with pytest.raises(key_value):
            parse_tag_key_value(text, value_required=False)
    else:
        assert parse_tag_key_value(text, value_required=False) == key_value


def test_parse_tag_key_value_required() -> None:
    """
    Error should be raised when tag is missing a required value.
    """
    with pytest.raises(ValueError):
        parse_tag_key_value("project", value_required=True)


@pytest.mark.parametrize(
    "value,text",
    [
        (10, "10"),
        (True, "true"),
        (False, "false"),
        (None, "null"),
        ("", '""'),
        ("hello", "hello"),
        ("10a", "10a"),
        ("true", '"true"'),
        ("false", '"false"'),
        ("null", '"null"'),
        ("10", '"10"'),
        ("10.2", '"10.2"'),
        ("[10]", '"[10]"'),
        ('{"a": 1}', r'"{\"a\": 1}"'),
        (" hello ", '" hello "'),
    ],
)
def test_format_tag_value(value: Any, text: str) -> None:
    """
    Should be able to format tag values correctly.
    """
    assert format_tag_value(value) == text


@pytest.mark.parametrize(
    "key,value,text",
    [
        ("project", "acme", "project=acme"),
        ("cost", 10, "cost=10"),
        ("doc", "a234567890123", "doc=a234567..."),
        ("a234567890123", "a234567890123", "a234567...=a234567..."),
    ],
)
def test_format_tag_key_value(key: str, value: Any, text: str) -> None:
    """
    Should be able to format tag values correctly.
    """
    assert format_tag_key_value(key, value, max_length=10) == text


def test_value_tags(scheduler: Scheduler, session: Session) -> None:
    """
    We should be able to add tags to a Value.
    """
    input_tags = {"env": "prod", "project": "acme", "data": [1, "a", True]}

    @task(cache=False)
    def task1():
        x = 10
        return apply_tags(x, list(input_tags.items()))

    scheduler.run(task1())

    # Tags should be recorded.
    tags = session.query(Tag).filter(Tag.key.in_(input_tags.keys()))
    tag_data = {tag.key: tag.value for tag in tags}
    assert tag_data == {
        "env": "prod",
        "project": "acme",
        "data": [1, "a", True],
    }

    # They should have Value as an entity.
    assert isinstance(tags[0].entity, Value)
    assert tags[0].entity.value_parsed == 10

    # Running the same task again should not create more of the same tags.
    scheduler.run(task1())
    tags = session.query(Tag).filter(Tag.key.in_(input_tags.keys()))
    assert tags.count() == 3

    value = session.query(Value).filter(Value.value_hash == tags[0].entity_id).one()
    assert {tag.key for tag in value.tags} == {"env", "project", "data"}


def test_apply_all_tags(scheduler: Scheduler, session: Session) -> None:
    """
    We should be able to add tags to a Value, Job, and Execution.
    """

    @task
    def task1():
        x = 10
        return apply_tags(x, [("a", 1)], job_tags=[("b", 2)], execution_tags=[("c", 3)])

    scheduler.run(task1())

    # Tags should be recorded on value.
    job = session.query(Job).one()
    value = job.call_node.value
    assert {tag.key: tag.value for tag in value.tags} == {"a": 1}

    # Tags should be recorded on job.
    assert {tag.key: tag.value for tag in job.tags} == {"b": 2}

    # Tags should be recorded on execution.
    execution = session.query(Execution).one()
    assert {tag.key: tag.value for tag in execution.tags} == {"c": 3}


def test_task_tags(scheduler: Scheduler, session: Session) -> None:
    """
    We should be able to add tags to a Task.
    """

    @task(tags=[("env", "dev")])
    def task2():
        return 10

    scheduler.run(task2.options(tags=[("user", "alice")])())

    # Task tags are defined in the decorator.
    task_tag = session.query(Tag).filter(Tag.entity_id == task2.hash).one()
    assert task_tag.key == "env"
    assert task_tag.value == "dev"

    # They should have Task as an entity.
    assert isinstance(task_tag.entity, Task)
    assert task_tag.entity.hash == task2.hash

    task_row = session.query(Task).one()
    task_row.tags[0].key == "env"

    # Job tags are defined at call time.
    job_tag = session.query(Tag).filter(Tag.entity_type == TagEntity.Job).one()
    assert job_tag.key == "user"
    assert job_tag.value == "alice"
    assert isinstance(job_tag.entity, Job)
    assert job_tag.entity.task.name == "task2"

    job = session.query(Job).one()
    assert job.tags[0].key == "user"


def test_namespace_tags(scheduler: Scheduler, session: Session) -> None:
    """
    We should be able to create tags with namespaces.
    """
    tags = {"redun.project": "acme", "redun.sub.user": "alice", "env": "prod"}

    @task(cache=False)
    def task1():
        x = 10
        return apply_tags(x, list(tags.items()))

    scheduler.run(task1())

    # Tags should be recorded.
    tag1, tag2, tag3 = session.query(Tag).filter(Tag.key.in_(tags.keys())).order_by(Tag.key)

    assert tag1.base_key == "env"
    assert tag1.namespace == ""

    assert tag2.base_key == "project"
    assert tag2.namespace == "redun"

    assert tag3.base_key == "user"
    assert tag3.namespace == "redun.sub"


def test_execution_tags(scheduler: Scheduler, session: Session) -> None:
    """
    We should be able to create tags for Executions.
    """

    @task(cache=False)
    def task1():
        return 10

    scheduler.run(task1(), tags=[("project", "acme")])
    tags = session.query(Tag).all()
    execution = session.query(Execution).one()

    expected_tag_keys = {"project", "git_commit", "git_origin_url"}
    for tag in tags:
        assert tag.entity_id == execution.id
        assert tag.entity_type == TagEntity.Execution
        assert tag.key in expected_tag_keys
        if tag.key == "project":
            assert tag.value == "acme"


def test_tag_edit(backend: RedunBackendDb, session: Session) -> None:
    """
    We should be able to edit a tag.
    """
    # Record a Value (10).
    entity = 10
    entity_id = backend.record_value(entity)

    # Tag the Value with env=dev.
    [(tag_hash, _, _, _)] = backend.record_tags(TagEntity.Value, entity_id, [("env", "dev")])
    assert backend.get_tags([entity_id]) == {entity_id: {"env": ["dev"]}}

    # Update the tag with env=prod.
    [tag2] = backend.record_tags(TagEntity.Value, entity_id, [("env", "prod")], parents=[tag_hash])
    assert backend.get_tags([entity_id]) == {entity_id: {"env": ["prod"]}}

    # Ensure TagEdit edges are recorded.
    tag_row = session.query(Tag).filter_by(is_current=True).one()
    assert tag_row.parents[0].value == "dev"
    assert session.query(Tag.value).filter_by(is_current=True).one()[0] == "prod"

    # Update the tag with env=prod2 and add tag user=alice.
    backend.record_tags(
        TagEntity.Value, entity_id, [("env", "prod2"), ("user", "alice")], update=True
    )
    assert backend.get_tags([entity_id]) == {entity_id: {"env": ["prod2"], "user": ["alice"]}}

    # Rename the tag key.
    backend.update_tags(TagEntity.Value, entity_id, ["env"], [("environ", "production")])
    assert backend.get_tags([entity_id]) == {
        entity_id: {"environ": ["production"], "user": ["alice"]}
    }


def test_tag_edit_idempotent(backend: RedunBackendDb, session: Session) -> None:
    """
    Adding tags should be idempotent.
    """
    # Record a Value (10).
    entity = 10
    entity_id = backend.record_value(entity)

    [(tag_hash, _, _, _)] = backend.record_tags(TagEntity.Value, entity_id, [("key", "value")])
    backend.record_tags(
        TagEntity.Value,
        entity_id,
        [("key", "value2")],
        parents=[tag_hash],
    )
    backend.record_tags(
        TagEntity.Value,
        entity_id,
        [("key", "value2")],
        parents=[tag_hash],
    )
    assert backend.get_tags([entity_id]) == {entity_id: {"key": ["value2"]}}


def test_tag_merge(backend: RedunBackendDb, session: Session) -> None:
    """
    We should be able to merge multiple values together for a tag.
    """
    # Record a Value (10).
    entity = 10
    entity_id = backend.record_value(entity)

    # Tag the Value with env=dev.
    backend.record_tags(TagEntity.Value, entity_id, [("env", "dev")])
    assert backend.get_tags([entity_id]) == {entity_id: {"env": ["dev"]}}

    # In parallel, tag the Value with env=dev.
    backend.record_tags(TagEntity.Value, entity_id, [("env", "prod")])
    assert backend.get_tags([entity_id]) == {entity_id: {"env": ["dev", "prod"]}}

    # Merge tags.
    backend.record_tags(TagEntity.Value, entity_id, [("env", "prod")], update=True)
    assert backend.get_tags([entity_id]) == {entity_id: {"env": ["prod"]}}


def test_tag_delete(backend: RedunBackendDb, session: Session) -> None:
    """
    We should be able to merge multiple values together for a tag.
    """
    # Record a Value (10).
    entity = 10
    entity_id = backend.record_value(entity)

    # Tag the Value with env=dev.
    backend.record_tags(TagEntity.Value, entity_id, [("env", "dev"), ("env", "prod")])
    assert backend.get_tags([entity_id]) == {entity_id: {"env": ["dev", "prod"]}}

    # Delete tag.
    backend.delete_tags(entity_id, [("env", "dev")])
    assert backend.get_tags([entity_id])[entity_id] == {"env": ["prod"]}


def test_tag_new(backend: RedunBackendDb, session: Session) -> None:
    """
    We should be able to recreate a tag after its deleted.
    """
    # Record a Value (10).
    entity = 10
    entity_id = backend.record_value(entity)

    # Tag the Value with env=dev.
    backend.record_tags(TagEntity.Value, entity_id, [("env", "dev"), ("env", "prod")])
    assert backend.get_tags([entity_id]) == {entity_id: {"env": ["dev", "prod"]}}

    # Delete tag.
    backend.delete_tags(entity_id, [("env", "dev")])
    assert backend.get_tags([entity_id])[entity_id] == {"env": ["prod"]}

    # Trying to make the same tag without parents, will not bring the tag back.
    backend.record_tags(TagEntity.Value, entity_id, [("env", "dev"), ("env", "prod")])
    assert backend.get_tags([entity_id]) == {entity_id: {"env": ["prod"]}}

    # Using new=True, we will automatically create the tag as a leaf in the TagGraph.
    backend.record_tags(TagEntity.Value, entity_id, [("env", "dev"), ("env", "prod")], new=True)
    assert backend.get_tags([entity_id]) == {entity_id: {"env": ["dev", "prod"]}}

    # Delete the tag again.
    backend.delete_tags(entity_id, [("env", "dev")])
    assert backend.get_tags([entity_id])[entity_id] == {"env": ["prod"]}

    # Add tag back again.
    backend.record_tags(TagEntity.Value, entity_id, [("env", "dev"), ("env", "prod")], new=True)
    assert backend.get_tags([entity_id]) == {entity_id: {"env": ["dev", "prod"]}}
