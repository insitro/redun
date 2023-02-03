from itertools import chain
from typing import Any

import pytest
from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.orm import Session, declarative_base, sessionmaker

from redun.db_utils import filter_in, get_or_create, query_filter_in

Base: Any = declarative_base()


class Person(Base):
    __tablename__ = "person"

    id = Column(Integer, primary_key=True)
    name = Column(String)


@pytest.fixture
def session():
    """
    Fixture for test database.
    """

    # Initialize in memory database.
    engine = create_engine("sqlite:///:memory:")
    Session = sessionmaker(bind=engine)
    session = Session()
    Base.metadata.create_all(bind=engine)

    # Create some example rows.
    session.add(Person(name="Alice"))
    session.add(Person(name="Bob"))
    session.add(Person(name="Claire"))
    session.add(Person(name="Dan"))
    session.commit()

    return session


def test_get_or_create(session: Session) -> None:
    """
    get_or_create should ensure records exist and update them if needed.
    """

    # Assert db initial state.
    assert session.query(Person.name).order_by(Person.name).all() == [
        ("Alice",),
        ("Bob",),
        ("Claire",),
        ("Dan",),
    ]

    # New row.
    row, created = get_or_create(session, Person, {"name": "Eve"})
    assert row.name == "Eve"
    assert created

    # Existing row.
    row, created = get_or_create(session, Person, {"name": "Alice"})
    assert row.name == "Alice"
    assert not created

    # Update existing row.
    row, created = get_or_create(session, Person, {"name": "Bob"}, update={"name": "Bobby"})
    assert row.name == "Bobby"
    assert not created

    # Update non-existent row.
    row, created = get_or_create(session, Person, {"name": "Fred"}, update={"name": "Freddie"})
    assert row.name == "Freddie"
    assert created


def test_filter_in(session: Session) -> None:
    """
    filter_in should allow large filter-IN clauses for sqlalchemy.
    """

    # Assert db initial state.
    assert session.query(Person.name).order_by(Person.name).all() == [
        ("Alice",),
        ("Bob",),
        ("Claire",),
        ("Dan",),
    ]

    query = session.query(Person.name).order_by(Person.name)

    rows = list(filter_in(query, Person.name, ["Alice", "Bob", "Eve"], chunk=1))
    assert rows == [("Alice",), ("Bob",)]

    queries = query_filter_in(query, Person.name, ["Alice", "Bob", "Eve"], chunk=1)
    rows = list(chain(*(subquery.all() for subquery in queries)))
    assert rows == [("Alice",), ("Bob",)]
