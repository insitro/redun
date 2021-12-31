---
tocpdeth: 3
---

# Type checking

redun supports type checking workflows using [Python's type hints](https://docs.python.org/3/library/typing.html) and [mypy](https://github.com/python/mypy).

Consider the following example workflow:

```py
from redun import task

redun_namespace = "example"

@task()
def add(a: int, b: int) -> int:
    return a + b

def main(x: int) -> int:
    y = add(x, 1)
    # y has type TaskExpression[int]

    # y can be used wherever int is allowed.
    z = add(x, y)
    # z has type TaskExpression[int]

    # z can be returned for a type int.
    return z
```

redun tasks are lazy functions, so when we call `add` it returns immediately with an `Expression` object which represents the desire to execute the `add` task. The actual execution will happen sometime later by the redun Scheduler. In this example, the fully parameterized type is `Expression[int]`, which represents an `Expression` that will eventually evaluate to an `int`. In terms of type checking, `Expression[T]` will be considered equivalent to `T` (for any type `T`), and can be used wherever `T`s are allowed.

## Overly permissive type checking

In general, the type checking rules defined above provide good type checking coverage, however there is one corner case that is not fully captured currently with redun. Since `Expression[T]` will be considered equivalent to `T`, it is possible to pass an `Expression[T]` to a plain python function (not decorated by `@task`) that only allows `T`. This will type check as allowed, but may fail at runtime.
