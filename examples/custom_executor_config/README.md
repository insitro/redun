# Custom executor via config `class` key

This example shows how to register a custom executor using only the `redun.ini` config file, without needing a `setup_scheduler` function.

## The problem

Custom executors that live outside the `redun` package need to be imported before the scheduler starts. Traditionally this requires a `setup_scheduler` function that imports the executor module for its side effects (see the [setup_scheduler example](../setup_scheduler/)).

## The solution

Add a `class` key to your executor config section with the fully-qualified class name. Redun will lazily import and register the executor automatically:

```ini
[executors.my_custom]
type = my_custom_type
class = my_library.executors.MyCustomExecutor
```

No `setup_scheduler` function needed. No import ordering to worry about.

## Running this example

```sh
redun run workflow.py main
```

This runs a simple workflow using a custom executor defined in `my_executors.py`, registered purely through config.

## How it works

1. `my_executors.py` defines `MyLocalExecutor`, a custom executor subclass
2. `.redun/redun.ini` declares the executor with `class = my_executors.MyLocalExecutor`
3. When redun starts, it sees `type = my_local` is not in the registry, finds the `class` key, and lazily imports the executor

## Comparison with setup_scheduler

| Approach | Pros | Cons |
|----------|------|------|
| `class` config key | Simple, declarative, no Python needed | One line per executor |
| `setup_scheduler` | Full programmatic control, conditional logic | Requires Python function, import ordering |

Use the `class` key for straightforward executor registration. Use `setup_scheduler` when you need conditional logic (e.g., selecting executors based on a profile argument).
