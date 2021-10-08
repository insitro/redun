from redun import task
from redun.tools import debug_task

redun_namespace = "redun.examples.debugging"


@task()
def with_breakpoint(x: int) -> int:
    print("before")

    # Feel free to use break points inside of tasks that run within their own thread.
    # Use 'c' to continue.
    import pdb; pdb.set_trace()
    print("after")
    return x + 1


@task()
def add(a: int, b: int) -> int:
    return a + b


@task()
def boom(num: float, denom: float) -> float:
    return num / denom


@task()
def will_boom(num: float = 10, denom: float = 0) -> float:
    return boom(num=num, denom=denom)


@task()
def main() -> dict:
    results = {}

    # You can use a usual breakpoint in tasks that run locally.
    results["breakpoint"] = with_breakpoint(10)

    # You can use debug_task() as a "lazy" breakpoint to inspect a dataflow.
    x = add(1, 2)
    x = add(x, 3)

    # Let's inspect x at this point.
    x = debug_task(x)

    x = add(4, x)
    results["debug_task"] = x

    return results
