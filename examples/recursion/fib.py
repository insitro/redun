from redun import task

redun_namespace = "redun.examples.recursion"


@task()
def add(a: int, b: int) -> int:
    return a + b


@task()
def fib(n: int) -> int:
    if n <= 1:
        return 1
    return add(fib(n - 1), fib(n - 2))
