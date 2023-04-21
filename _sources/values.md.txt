---
tocpdeth: 3
---

# Values

Values is the general term redun uses to refer to task arguments and results. Below, we describe 
some of the special treatments redun uses to enable expressive value dataflow.

## Hashing and equality

All values in redun must be hashable. Hashes are used to define equality of values, without any 
fallback to deep equality checking. 

An important consequence is that redun ignores the difference between whether two entities 
actually references to the same underlying object, or merely hash identically. For example:

```python
class MyObject:
  def __init__(self, data):
    self.data = data

x = MyObject(1)
y = MyObject(1)
```

Since `x` and `y` hash identically, redun cannot distinguish between them.

Therefore, hashes provide a unique mechanism for indexing objects in the backend.

## Serialization

Redun primarily serializes `Value`s with `pickle` since it is native to python and provides powerful tools for 
serializing complex and nested Python objects. Users may customize this as usual, with 
`__getstate__` and `__setstate__`.

## Validity

In addition to handling ordinary values, i.e., data or objects represented in memory, complex
workflows often require reasoning about state that may be complex or external, such as
a database or filesystem. Redun accomplishes reasoning over this state by converting it into
value-like semantics, after which, redun can proceed without differentiating between values
and state. Specifically, the `Value` class provides that abstraction. 

After deserializing a regular data object, we could typically assume that the serialization
was successful and proceed to use the resulting object. However, state management requires
that after deserialization, we offer another lifecycle hook, `is_valid`, during which the
object can identify that the deserialized state no longer matches reality, and hence may not
be relied upon.

The simplest example of validity is a `Value` representing a file on a shared filesystem,
where we serialize both the path to the file (as a string) and hash of the file contents. When we
retrieve a cache value and deserialize the object, we can recheck the hash of the file. If the
file hash does not match, then the object can declare that its state assertion is not valid,
and that we should reassert it (i.e., re-run the task to rewrite the file with the intended
contents).

See (File reactivity)[design.md#File-reactivity] for more exposition on the file case.

## Extending standard types with `ProxyValue`s

Many task have inputs or outputs that are not redun `Value` types, either because they are 
builtins like `str` or `int`, or are other types, such as `np.array`. When the redun scheduler
works with these, it wraps them in `ProxyValue`, but then unwraps them before presenting them to 
user code. The default implementation derives a hash function from the pickle serialization
of the object, which is a good, general purpose choice.

The result is that for most basic data types, users don't need to do anything to teach redun 
to work with their types. As a result, this code works fine, even though redun doesn't know 
anything about `numpy`:

```python
import numpy as np

@task
def task(x: np.array, y: float) -> np.array:
    return x + y
```

If this default behavior is not desirable, users can implement either `Value` or `ProxyValue` types
to customize the behavior. These types also provide hooks to allow the CLI to parse them, 
which the `redun run` command relies on.

## Handles for ephemeral and stateful values

We'll introduce the features of Handles in several steps. The first feature that Handles provide is control over the data used for serialization and a mechanism for recreating ephemeral state after deserialization. Let's say we wanted to pass a database connection between tasks in a safe way. We could use a Handle class to do the following:

```py
from redun import task, Handle

class DbHandle(Handle):
    def __init__(self, name, db_file):
        self.db_file = db_file
        self.instance = sqlite3.connect(db_file)

@task()
def extract(conn):
    data = conn.execute('select * from my_table').fetchmany()
    return data

@task()
def transform(data):
    # result = transform data in some way...
    return result

@task()
def load(conn, data):
    conn.executemany('insert into another_table values(?, ?, ?)', data)
    return conn

@task()
def main():
    # Two databases we want to move data between
    src_conn = DbHandle('src_conn', 'src_data.db')
    dest_conn = DbHandle('dest_conn', 'dest_data.db')

    data = extract(src_conn)
    data2 = transform(data)
    dest_conn2 = load(dest_conn, data2)
    return dest_conn2
```

First, we use the `DbHandle` class to wrap a sqlite database connection. When Handles are serialized, only the arguments to their constructor are serialized. Therefore, you can define as much internal state as you wish for a Handle and it will not get caught up in the cache. redun's default serialization is Python's pickle framework. pickle uses the `__new__()` constructor and `__setstate__` (or `__dict__`) to directly set the state of an object, thus avoiding calling the `__init__` initializer. Handles modify this behavior in order to execute `__init__` even when deserializing. Therefore, we can use the constructor to reestablish ephemeral state such as the database connection.

Since Handles are often used to wrap another object, such as a connection, Handles provide a convenient mechanism for reducing boiler plate code. If a Handle defines a `self.instance` attribute, all attribute access to the Handle is automatically proxied to the `self.instance`. Above the `conn.execute()` and `conn.executemany()` calls make use of the attribute proxy.

### State tracking

Now let's take a closer look at the `load()` task. It uses a handle, `dest_conn`, as an input and output in order to perform a write. Ideally, we would like to represent that `dest_conn` is updated by the `load` task so that we can correctly determine which tasks need to be re-executed when input data or code changes.

As we stated before, hashing an entire database just to represent the database state is often not feasible, either because there is too much data to rehash frequently, or the database state contains too much ancillary data to hash consistently. As an alternative to hashing the whole database, we could just hash the sequence of operations (i.e. Tasks) we have applied to database Handle. This is the strategy redun takes.

Specifically, as a handle passes into or out of a Task, the redun scheduler duplicates the Handle and incorporates into its hash information specific to the task call, such as the hash of the corresponding call node. Therefore, in the example above, if we look at these specific lines:

```py
@task()
def load(conn_i, data):
    conn.executemany('insert into another_table values(?, ?, ?)', data)
    return conn_i

@task()
def main():
    # SNIP...
    dest_conn2 = load(dest_conn, data2)
    # SNIP ...
```

the handles `dest_conn`, `conn_i`, and `dest_conn2` are all distinct and have their own hashes. redun also records the lineage of such handles into a Handle Graph which represents the state transitions the handle has gone through:

```
dest_conn* --> conn_i* --> dest_conn2*
```

redun also records whether each handle state is currently valid or not, which we indicate above using `*` for valid handles. When re-executing a workflow, if a previously cached handle state is still valid we can safely fast forward through the corresponding task.

Now, let's consider what happens if we added an additional load step and re-executed this workflow:

```py
@task()
def load(conn_i, data):
    # Write data into conn_i...
    return conn_i

@task()
def load2(conn_ii, data):
    # Write date into conn_ii...
    return conn_ii

@task()
def main():
    # Two databases we want to move data between
    src_conn = DbHandle('src_conn', 'src_data.db')
    dest_conn = DbHandle('dest_conn', 'dest_data.db')

    data = extract(src_conn)
    data2 = transform(data)
    data3 = transform2(data)
    dest_conn2 = load(dest_conn, data2)
    dest_conn3 = load2(dest_conn2, data3)
    return dest_conn3
```

redun would notice that it can forward through `extract`, `transform`, and `load`, and would only execute `transform2`, and `load2`. The Handle Graph would be updated with:

```
dest_conn* --> conn_i* --> dest_conn2* --> conn_ii* --> dest_conn3*
```

Now consider what happens if we decided to change the code of task `load2()`. redun would fast forward through much of the workflow but would re-execute `load2()`. In doing so it would branch the Handle Graph, since `load2()` would contribute a different call node hash to the `dest_conn3`. Let's consider this new Handle `dest_conn3b`, and the Handle Graph would be:

```
dest_conn* --> conn_i* --> dest_conn2* --> conn_ii* --> dest_conn3
                                                  \
                                                   \--> dest_conn3b*
```

The old handle state `dest_conn3` is marked as invalid. What's important about this bookkeeping is that if we revert the change to `load2()`, it is not appropriate to just fast revert to dest_conn3. We must re-execute `load2()` again to update the database to be at state `dest_conn3` (instead of `dest_conn3b`).

### More advanced Handle uses

*This is currently explained very briefly*

Handles can be forked into instances that can be processed in parallel (parallel loading into a database). Forking might also be done to denote that there is no dependency between the tasks and limit the scope of rollbacks (i.e. Handle invalidation).

```py
from redun import merge_handles

# SNIP ...

@task()
def main():
    conn = DbHandle('conn', 'data.db')
    conn2a = load_a(conn)
    conn2b = load_b(conn)
    conn3 = merge_handles([conn2a, conn2b])
    conn4 = load_c(conn3)
    return conn4
```

Handles can also be merged to denote that the previous parallel steps must complete before continuing to the next step. The Handle Graph for the above code would be:

```
conn* --> conn_i* ---> conn2a* ----> conn3* --> conn_iii* --> conn4*
    \                            /
     \--> conn_ii* --> conn2b* -/
```

Using the above Handle Graph, redun can determine that updating the code for `load_b()` should trigger re-execution for `load_b()` and `load_c()`, but not `load_a()`.
