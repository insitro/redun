from typing import List

from redun import task
from redun.task import Task


redun_namespace = "redun.examples.higher_order"


@task()
def my_sum(a: int, b: int) -> int:
    return a + b


@task()
def my_prod(a: int, b: int) -> int:
    return a * b


@task()
def my_reduce(my_task: Task, items: List[int]) -> int:
    """
    Recursively divides items and applies my_task to reduce them.
    """
    assert items

    if len(items) == 1:
        return items[0]
    else:
        midpoint = len(items) // 2
        result1 = my_reduce(my_task, items[:midpoint])
        result2 = my_reduce(my_task, items[midpoint:])
        return my_task(result1, result2)


@task()
def main() -> dict:
    items = list(range(1, 11))

    # Here we reduce the numbers 1 through 10 using sum and product.
    # We use a task (my_sum) as an argument to another task (my_reduce).
    total_sum = my_reduce(my_sum, items)
    total_prod = my_reduce(my_prod, items)

    return {
        "sum": total_sum,
        "prod": total_prod,
    }
