# Postgres db example

Start a postgres database by running the following in the redun project root:

```sh
docker-compose up db
```

This uses the `docker-compose.yml` file to start a postgres database inside a docker container.

Let's initialize that database with the schema expected by your version of `redun`:

```sh
PGUSER=postgres PGPASSWORD=postgres redun init
```

You can now connect to this database directly using `psql`:

```
PGUSER=postgres PGPASSWORD=postgres psql postgresql://localhost:5432/redun

psql (11.5, server 9.6.16)
Type "help" for help.

redun=# \d
               List of relations
 Schema |       Name        | Type  |  Owner
--------+-------------------+-------+----------
 public | alembic_version   | table | postgres
 public | argument          | table | postgres
 public | argument_result   | table | postgres
 public | call_edge         | table | postgres
 public | call_node         | table | postgres
 public | call_subtree_task | table | postgres
 public | evaluation        | table | postgres
 public | execution         | table | postgres
 public | file              | table | postgres
 public | handle            | table | postgres
 public | handle_edge       | table | postgres
 public | job               | table | postgres
 public | redun_version     | table | postgres
 public | subvalue          | table | postgres
 public | task              | table | postgres
 public | value             | table | postgres
(16 rows)

redun=#
```

This example specifies the redun db in the `.redun/redun.ini` file as:

```ini
[backend]
db_uri = postgresql://postgres@localhost:5432/redun
```

Running the example workflow should start recording into the postgres database:

```
redun run workflow.py main
```

And now we can query postgres like this:

```
psql postgresql://postgres@localhost:5432/redun -c 'select * from task;'

                   hash                   | name  | namespace |        source
------------------------------------------+-------+-----------+----------------------
 af57a6c0ba79cf133c7f289f904065afe4ecc673 | main  |           | def main():         +
                                          |       |           |     return task1(10)+
                                          |       |           |
 df162a45cbd956ac6e9c357059277860a2e9df3a | task1 |           | def task1(x):       +
                                          |       |           |     return x + 1    +
                                          |       |           |
(2 rows)
```
