from functools import reduce
from itertools import islice
from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional, Set, Tuple

import sqlalchemy as sa
from sqlalchemy.orm.query import Query
from sqlalchemy.sql.expression import cast as sa_cast

from redun.backends.db import (
    JSON,
    Argument,
    Base,
    CallNode,
    Execution,
    File,
    Job,
    Subvalue,
    Tag,
    Task,
    Value,
)
from redun.tags import ANY_VALUE


class CallGraphQuery:
    """
    Query class for efficiently querying across all models in a CallGraph.
    """

    # Look up for model primary keys.
    MODEL_PKS = {
        Execution: "id",
        Job: "id",
        CallNode: "call_hash",
        Value: "value_hash",
        Task: "hash",
    }
    MODEL_NAMES = ["Execution", "Job", "CallNode", "Task", "Value"]

    ExecTag = sa.orm.aliased(Tag)

    def __init__(
        self,
        session: sa.orm.Session,
        joins: Optional[Set[str]] = None,
        execution_joins: Optional[List[Callable[[Query], Query]]] = None,
        filters: Optional[List] = None,
        order_by: Optional[str] = None,
        filter_types: Optional[Set] = None,
        executions: Optional[Query] = None,
        jobs: Optional[Query] = None,
        call_nodes: Optional[Query] = None,
        tasks: Optional[Query] = None,
        values: Optional[Query] = None,
        value_subqueries: Optional[List[Query]] = None,
    ):
        self._session = session
        self._joins: Set[str] = joins or set()
        self._execution_joins: List[Callable[[Query], Query]] = execution_joins or []
        self._filters: List = filters or []
        self._order_by = order_by
        self._filter_types = filter_types if filter_types is not None else set(self.MODEL_NAMES)

        # Subqueries.
        self._executions = executions or self._session.query(Execution)
        self._jobs = jobs or self._session.query(Job)
        self._call_nodes = call_nodes or self._session.query(CallNode)
        self._tasks = tasks or self._session.query(Task)
        self._values = values if values is not None else self._session.query(Value)
        self._value_subqueries = value_subqueries

    @property
    def subqueries(self) -> Iterable[Tuple[str, Query]]:
        """
        Iterates through all subqueries.

        Yields
        ------
        (type_name, subquery) : Tuple[str, Query]
        """
        return [
            ("Execution", self._executions),
            ("Job", self._jobs),
            ("CallNode", self._call_nodes),
            ("Task", self._tasks),
            ("Value", self._values),
        ]

    def clone(self, **kwargs: Any) -> "CallGraphQuery":
        """
        Returns a clone of the query with updates specified by `kwargs`.
        """
        clone_kwargs: Dict[str, Any] = {
            "joins": self._joins,
            "execution_joins": self._execution_joins,
            "filters": self._filters,
            "order_by": self._order_by,
            "filter_types": self._filter_types,
            "executions": self._executions,
            "jobs": self._jobs,
            "call_nodes": self._call_nodes,
            "tasks": self._tasks,
            "values": self._values,
            "value_subqueries": self._value_subqueries,
        }
        clone_kwargs.update(kwargs)
        return CallGraphQuery(self._session, **clone_kwargs)

    def filter_types(self, types: Iterable[str]) -> "CallGraphQuery":
        """
        Filter query by record type.
        """
        return self.clone(filter_types=set(types))

    def filter_ids(self, _ids: Iterable[str]) -> "CallGraphQuery":
        """
        Filter query by record ids.
        """
        ids = list(_ids)

        def filter(query):
            return query.clone(
                executions=query._executions.filter(Execution.id.in_(ids)),
                jobs=query._jobs.filter(Job.id.in_(ids)),
                call_nodes=query._call_nodes.filter(CallNode.call_hash.in_(ids)),
                tasks=query._tasks.filter(Task.hash.in_(ids)),
                values=query._values.filter(Value.value_hash.in_(ids)),
            )

        return self.clone(filters=self._filters + [filter])

    def like_id(self, id: str) -> "CallGraphQuery":
        """
        Filter query by record id prefix `id`.
        """

        def filter(query):
            pattern = id + "%"
            return query.clone(
                executions=query._executions.filter(Execution.id.like(pattern)),
                jobs=query._jobs.filter(Job.id.like(pattern)),
                call_nodes=query._call_nodes.filter(CallNode.call_hash.like(pattern)),
                tasks=(
                    self._tasks.filter(Task.hash.like(pattern)).union(
                        self._tasks.filter(Task.name == id)
                    )
                ),
                values=query._values.filter(Value.value_hash.like(pattern)),
            )

        return self.clone(filters=self._filters + [filter])

    def _join_files(self):
        return self.clone(values=self._values.join(File, File.value_hash == Value.value_hash))

    def _join_jobs(self):
        return self.clone(executions=self._executions.join(Job, Job.id == Execution.job_id))

    def _join_executions(self):
        """
        Join Executions to each subquery.
        """
        query = self

        # Values can be joined to Execution via four possible paths. Thus far,
        # we have found it most efficient to perform all four paths and union
        # their results.

        # Values can connect to CallNodes either directly as a result or
        # through Argument as an argument. Values can also be a subvalue of a
        # parent Value. These possibilities combine to give four join paths.
        value_results = query._values.join(CallNode, CallNode.value_hash == Value.value_hash).join(
            Job, Job.call_hash == CallNode.call_hash
        )
        value_args = (
            query._values.join(Argument, Argument.value_hash == Value.value_hash)
            .join(CallNode, CallNode.call_hash == Argument.call_hash)
            .join(Job, Job.call_hash == CallNode.call_hash)
        )
        subvalue_results = (
            query._values.join(Subvalue, Subvalue.value_hash == Value.value_hash)
            .join(CallNode, CallNode.value_hash == Subvalue.parent_value_hash)
            .join(Job, Job.call_hash == CallNode.call_hash)
        )
        subvalue_args = (
            query._values.join(Subvalue, Subvalue.value_hash == Value.value_hash)
            .join(Argument, Argument.value_hash == Subvalue.parent_value_hash)
            .join(CallNode, CallNode.call_hash == Argument.call_hash)
            .join(Job, Job.call_hash == CallNode.call_hash)
        )
        subqueries = [value_results, value_args, subvalue_results, subvalue_args]

        # Perform additional joins on execution.
        for i, _ in enumerate(subqueries):
            for join in self._execution_joins:
                subqueries[i] = join(subqueries[i])

        return query.clone(
            executions=query._executions,
            jobs=query._jobs,
            call_nodes=(query._call_nodes.join(Job, Job.call_hash == CallNode.call_hash)),
            tasks=(query._tasks.join(Job, Job.task_hash == Task.hash)),
            value_subqueries=subqueries,
        )

    def filter_execution_ids(self, execution_ids: Iterable[str]) -> "CallGraphQuery":
        """
        Filter query by Execution ids.
        """
        execution_ids = list(execution_ids)

        def filter(query):
            return query.clone(
                executions=query._executions.filter(Execution.id.in_(execution_ids)),
                jobs=query._jobs.filter(Job.execution_id.in_(execution_ids)),
                call_nodes=query._call_nodes.filter(Job.execution_id.in_(execution_ids)),
                tasks=query._tasks.filter(Job.execution_id.in_(execution_ids)),
                values=query._values.filter(Job.execution_id.in_(execution_ids)),
            )

        return self.clone(
            joins=self._joins | {"execution"},
            filters=self._filters + [filter],
        )

    def filter_execution_statuses(self, execution_statuses: Iterable[str]) -> "CallGraphQuery":
        """
        Filter by Execution status.
        """
        assert execution_statuses

        def term(status):
            if status == "DONE":
                return Job.end_time.isnot(None)
            elif status == "FAILED":
                return Job.end_time.is_(None)
            else:
                raise NotImplementedError(status)

        execution_clause = reduce(sa.or_, map(term, execution_statuses))

        def filter(query):
            return query.clone(executions=query._executions.filter(execution_clause))

        return self.clone(
            filter_types=self._filter_types & {"Execution"},
            joins=self._joins | {"job"},
            filters=self._filters + [filter],
            order_by="time",
        )

    def filter_job_statuses(self, job_statuses: Iterable[str]) -> "CallGraphQuery":
        """
        Filter by Job status.
        """
        assert job_statuses

        def job_term(status):
            if status == "FAILED":
                return Job.end_time.is_(None)
            elif status == "DONE":
                return Job.end_time.isnot(None) & Job.cached.is_(False)
            elif status == "CACHED":
                return Job.cached.is_(True)
            else:
                raise NotImplementedError(status)

        job_clause = reduce(sa.or_, map(job_term, job_statuses))

        def filter(query):
            return query.clone(jobs=query._jobs.filter(job_clause))

        return self.clone(
            filter_types=self._filter_types & {"Job"},
            filters=self._filters + [filter],
        )

    def filter_value_types(self, value_types: Iterable[str]) -> "CallGraphQuery":
        """
        Filter query by Value types.
        """

        def filter(query):
            return query.clone(values=query._values.filter(Value.type.in_(value_types)))

        return self.clone(
            filter_types=self._filter_types & {"Value"},
            filters=self._filters + [filter],
        )

    def filter_file_paths(self, paths: Iterable[str]) -> "CallGraphQuery":
        """
        Filter by File.path patterns.

        `paths` can contain "*" to perform wildcard matching.
        """
        # Convert path patterns into db LIKE patterns.
        like_patterns = [path.replace("*", "%") for path in paths]

        def filter(query):
            return query.clone(
                values=query._values.filter(
                    sa.or_(File.path.like(like_pattern) for like_pattern in like_patterns)
                )
            )

        return self.clone(
            filter_types=self._filter_types & {"Value"},
            joins=self._joins | {"file"},
            filters=self._filters + [filter],
        )

    def _query_filter_tags(
        self, query: Query, entity_id_col: Any, table: Any, tags: Iterable[Tuple[str, Any]]
    ) -> Query:
        """
        Build query for filtering tags.
        """
        for key, value in tags:
            tag_query = self._session.query(table).filter(
                table.is_current.is_(True), table.key == key
            )
            if value is not ANY_VALUE:
                tag_query = tag_query.filter(table.value == sa_cast(value, JSON))
            # We perform this filter as a subquery in order to find entities
            # with multiple matching tags.
            tag_query = tag_query.subquery()
            query = query.join(tag_query, tag_query.c.entity_id == entity_id_col)
        return query

    def filter_tags(self, tags: Iterable[Tuple[str, Any]]) -> "CallGraphQuery":
        """
        Filter by tags.
        """

        def filter(query):
            return query.clone(
                executions=self._query_filter_tags(query._executions, Execution.id, Tag, tags),
                jobs=self._query_filter_tags(query._jobs, Job.id, Tag, tags),
                call_nodes=self._query_filter_tags(
                    query._call_nodes, CallNode.call_hash, Tag, tags
                ),
                tasks=self._query_filter_tags(query._tasks, Task.hash, Tag, tags),
                values=self._query_filter_tags(query._values, Value.value_hash, Tag, tags),
            )

        return self.clone(
            filters=self._filters + [filter],
        )

    def filter_execution_tags(self, tags: Iterable[Tuple[str, Any]]) -> "CallGraphQuery":
        """
        Filter by tag on executions.
        """

        def filter(query):
            # Values will be filtered by the execution_join below.
            return query.clone(
                executions=self._query_filter_tags(
                    query._executions, Execution.id, self.ExecTag, tags
                ),
                jobs=self._query_filter_tags(query._jobs, Job.execution_id, self.ExecTag, tags),
                call_nodes=self._query_filter_tags(
                    query._call_nodes, Job.execution_id, self.ExecTag, tags
                ),
                tasks=self._query_filter_tags(query._tasks, Job.execution_id, self.ExecTag, tags),
            )

        def exec_join(sa_query: Query) -> Query:
            return self._query_filter_tags(sa_query, Job.execution_id, self.ExecTag, tags)

        return self.clone(
            joins=self._joins | {"execution"},
            execution_joins=self._execution_joins + [exec_join],
            filters=self._filters + [filter],
        )

    def order_by(self, order_by: str) -> "CallGraphQuery":
        """
        Order query.

        order_by: str
            The only supported value is 'time'.
        """
        return self.clone(order_by=order_by)

    def build(self):
        """
        Apply all joins and filters to subqueries.
        """
        query = self

        # Perform joins.
        if "file" in self._joins:
            query = query._join_files()
        if "job" in self._joins:
            query = query._join_jobs()
        if "execution" in self._joins:
            query = query._join_executions()

        # Perform filters.
        for filter in self._filters:
            query = filter(query)

        # Filter and union value subqueries.
        if query._value_subqueries:
            subqueries = [self.clone(values=subquery) for subquery in query._value_subqueries]
            for i, _ in enumerate(subqueries):
                for filter in self._filters:
                    subqueries[i] = filter(subqueries[i])
            values = subqueries[0]._values.union(*(q._values for q in subqueries[1:]))
            query = query.clone(values=values)

        # Perform order_by.
        if self._order_by == "time":
            Job2 = sa.orm.aliased(Job)
            query = query.clone(
                executions=(
                    query._executions.join(Job2, Execution.job_id == Job2.id).order_by(
                        Job2.start_time.desc()
                    )
                ),
                jobs=(query._jobs.order_by(Job.start_time.desc())),
            )
        elif self._order_by:
            raise NotImplementedError(self._order_by)

        # Remove filters, joins, etc since they are now built in.
        return query.clone(filters=[], joins={}, order_by=None)

    def empty(self) -> "CallGraphQuery":
        """
        Returns an empty query.
        """
        query = CallGraphQuery(self._session)
        return self.clone(
            executions=query._executions.filter(False),
            jobs=query._jobs.filter(False),
            call_nodes=query._call_nodes.filter(False),
            tasks=query._tasks.filter(False),
            values=query._values.filter(False),
        )

    def all(self) -> Iterator[Base]:
        """
        Yields all records matching query.
        """
        query = self.build()

        for record_type, subquery in query.subqueries:
            if record_type in self._filter_types:
                yield from subquery.distinct().all()

    def one(self) -> Base:
        """
        Returns exactly one record. Raises error if too few or too many.
        """
        [result] = self.all()
        return result

    def first(self) -> Optional[Base]:
        """
        Returns first record if it exists.
        """
        return next(self.all(), None)

    def limit(self, size) -> Iterator[Base]:
        """
        Yields at most `size` records from query.
        """
        query = self.build().clone(
            executions=self._executions.limit(size),
            jobs=self._jobs.limit(size),
            call_nodes=self._call_nodes.limit(size),
            tasks=self._tasks.limit(size),
            values=self._values.limit(size),
        )
        yield from islice(query.all(), 0, size)

    def count(self) -> Iterator[Tuple[str, int]]:
        """
        Returns counts for each record type.
        """
        query = self.build()
        for record_type, subquery in query.subqueries:
            if record_type in self._filter_types:
                yield (record_type, subquery.distinct().count())

    def select(self, *columns: Iterable[str], flat: bool = False) -> Iterator[Any]:
        """
        Select columns to return in query.
        """
        for record in self.all():
            row = []
            for column in columns:
                if column == "id":
                    pk = self.MODEL_PKS[type(record)]
                    row.append(getattr(record, pk))

                elif column == "type":
                    row.append(type(record).__name__)

                else:
                    raise ValueError(f"Unknown column '{column}'.")

            if flat:
                [value] = row
                yield value
            else:
                yield tuple(row)
