from redun import task
from redun.databases import DbHandle, register_db, get_db

import pandas as pd
import sqlalchemy as sa

redun_namespace = "redun.examples.databases"

# Register a database that can be used later in the workflow.
register_db("db", "sqlite:///example.db")


@task
def init_db(conn: DbHandle) -> DbHandle:
    with conn.begin() as trans:
        trans.execute("DROP TABLE IF EXISTS users")
        trans.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")

    return conn


@task
def import_data(conn: DbHandle) -> DbHandle:
    conn.execute("DELETE FROM users")
    conn.execute("""
      INSERT INTO users (name, age) VALUES
        ('Alice', 20),
        ('Bob', 21),
        ('Claire', 21),
        ('Dan', 22),
        ('Elaine', 23),
        ('Fred', 24)
    """)
    return conn


@task
def query_data(conn: DbHandle) -> pd.DataFrame:
    df = conn.query_df("select * from users")
    return df


@task
def main() -> dict:
    conn = get_db("db")

    # Use individual transactions for each query.
    conn.execute("DROP TABLE IF EXISTS people")
    conn.execute("CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
    conn.execute("INSERT INTO people (name, age) VALUES ('Alice', 20)")
    conn.execute("INSERT INTO people (name, age) VALUES ('Bob', 30)")

    df1 = conn.query_df("select * from people")

    # Use SQLAlchemy directly to perform one single transaction.
    with conn.session() as session:
        session.execute(sa.text("DROP TABLE IF EXISTS people"))
        session.execute(sa.text("CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)"))
        session.execute(sa.text("INSERT INTO people (name) VALUES ('Alice')"))
        session.execute(sa.text("INSERT INTO people (name) VALUES ('Bob')"))
        session.commit()

    df2 = conn.query_df("select * from people")

    # Use context to perform a transaction.
    with conn.begin() as trans:
        trans.execute("DROP TABLE IF EXISTS people")
        trans.execute("CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
        trans.execute("INSERT INTO people (name, age) VALUES ('Alice', 20)")
        trans.execute("INSERT INTO people (name, age) VALUES ('Bob', 30)")

    df3 = conn.query_df("select * from people")
    rows = conn.execute("select * from people")

    # Perform a workflow using a connection.
    conn = init_db(conn)
    conn = import_data(conn)
    df4 = query_data(conn)

    return {"df1": df1, "df2": df2, "df3": df3, "rows": rows, "df4": df4}
