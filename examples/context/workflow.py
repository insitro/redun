from typing import List

from redun import get_context, task

redun_namespace = "redun.examples.context"


@task(executor=get_context("my_tool.executor", "default"), memory=get_context("my_tool.memory", 1))
def my_tool(
    value: int,
    flag: bool = get_context("my_tool.flag", False),
    param: int = get_context("my_tool.missing"),
) -> str:
    # Perform desired computation with executor, memory, flag and param set by context.
    return f"Ran my_tool with flag: {flag} and param: {param}"


@task
def process_values(values: List[int]) -> List[str]:
    # Call our tool.
    return [my_tool(value) for value in values]


@task
def main(size: int = 10) -> dict:
    # Generate some data.
    values = list(range(size))

    # Call an intermediate task to process data.
    return {
        "values": process_values(values),
    }
