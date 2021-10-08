from typing import Any, Callable, Dict, Iterable, Optional, Tuple, TypeVar

from sqlalchemy import Column
from sqlalchemy.orm import Query, Session

ModelType = TypeVar("ModelType", bound=Callable)


def get_or_create(
    session: Session,
    Model: ModelType,
    filter: Dict[str, Any],
    update: Optional[Dict[str, Any]] = None,
) -> Tuple[ModelType, bool]:
    """
    Get or create a row for a sqlalchemy Model.
    """
    # An insert is the filter clause with update added on.
    insert: Dict[str, Any] = dict(filter)
    if update:
        insert.update(update)

    row = session.query(Model).filter_by(**filter).first()
    if not row:
        created = True
        row = Model(**insert)
        session.add(row)
    else:
        created = False
        if update:
            # Perform update if requested.
            for key, value in update.items():
                setattr(row, key, value)
            session.add(row)
    return row, created


def filter_in(query: Query, column: Column, values: Iterable, chunk: int = 100) -> Iterable[Any]:
    """
    Perform an IN-filter on a sqlalchemy query with an iterable of values.

    Returns an iterable of results.
    """
    values = list(values)
    for i in range(0, len(values), chunk):
        results = query.filter(column.in_(values[i : i + chunk]))
        for result in results:
            yield result


def query_filter_in(
    query: Query, column: Column, values: Iterable, chunk: int = 100
) -> Iterable[Query]:
    """
    Perform an IN-filter on a sqlalchemy query with an iterable of values.

    Returns an iterable of queries with IN-filter applied.
    """
    values = list(values)
    for i in range(0, len(values), chunk):
        yield query.filter(column.in_(values[i : i + chunk]))
