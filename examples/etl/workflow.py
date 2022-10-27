"""
Example of using redun to work with stateful external systems such as a database.

redun uses Handles to represent stateful external systems as well as to
control serialization and serialization of ephemeral objects such as network
connections.
"""

import csv
import os
import sqlite3

from redun import File, Handle, task

redun_namespace = "redun.examples.etl"


class DbHandle(Handle):
    """
    Wrap a database connection in a redun Handle.

    - Only the arguments to the constructor are serialized during caching.
    - Unlike most objects, a Handle's constructor is re-executed during
      deserialization, so you can recreate state, such as connections.
    """

    def __init__(self, name, db_file):
        self.db_file = db_file

        # If the connection is stored on `self.instance`, attribute access
        # (e.g. execute(), commit()) is automatically proxied to the connection.
        self.instance = sqlite3.connect(self.db_file, check_same_thread=False)

    def is_valid(self) -> bool:
        # Custom DB validity check that is useful to detecting whether the file
        # has been deleted or truncated.
        if not os.path.exists(self.db_file) or os.stat(self.db_file).st_size == 0:
            return False
        return super().is_valid()


@task()
def make_csv(csv_path: str = "data.csv") -> File:
    """
    Make a CSV from example data.
    """
    data = [
        ["id", "name", "updated_at"],
        [1, "alice", 10],
        [2, "bob", 11],
        [3, "charlie", 12],
    ]
    csv_file = File(csv_path)

    with csv_file.open("w") as out:
        writer = csv.writer(out)
        for row in data:
            writer.writerow(row)

    return csv_file


@task()
def init_db(conn: DbHandle) -> DbHandle:
    """
    Initialize the tables in the database.
    """
    # Note, that conn.execute() proxies to conn.instance.execute().
    conn.execute(
        """
    CREATE TABLE IF NOT EXISTS people (
        id INTEGER,
        name TEXT,
        updated_at INTEGER
    );
    """
    )
    conn.commit()
    return conn


@task()
def sync_db(conn: DbHandle, csv_file: File) -> DbHandle:
    """
    Sync a CSV into a database.
    """
    conn.execute(
        """
    DELETE FROM people;
    """
    )

    with csv_file.open() as infile:
        for row in csv.DictReader(infile):
            conn.execute(
                """
                INSERT INTO people VALUES(?, ?, ?)
                """,
                (row["id"], row["name"], row["updated_at"]),
            )
    conn.commit()
    return conn


@task()
def close_db(conn: DbHandle) -> DbHandle:
    """
    Close the database connection to ensure all data is committed.
    """
    conn.close()
    return conn


@task()
def main(csv_path: str = "data.csv", db_file: str = "data.db") -> DbHandle:
    """
    Example workflow of:
    1. creating a CSV
    2. initializing a database
    3. syncing a CSV into a database
    4. closing the database connection
    """
    csv_file = make_csv(csv_path)

    # By passing the connection (conn) as input and output each of the tasks,
    # we inform redun that we would like these tasks to execute in serial.
    conn = DbHandle("conn", db_file)
    conn = init_db(conn)
    conn = sync_db(conn, csv_file)
    conn = close_db(conn)
    return conn
