"""
Example showing that we can pass a list of values as an arg to a task from the CLI.

redun run simple_list.py workflow --numbers 1 2 3 --multiplier 3
"""

from enum import Enum
from typing import List

from redun import task


redun_namespace = "redun.examples.simple"


class Color(Enum):
    red = 1
    green = 2
    blue = 3


@task()
def multiply(numbers: List[int], multiplier: int) -> List[int]:
    return [n * multiplier for n in numbers]


@task()
def workflow(numbers: List[int], multiplier: int) -> List[int]:
    return multiply(numbers, multiplier)


@task()
def apply_not(values: List[bool]) -> List[bool]:
    return [not v for v in values]


@task()
def color_to_string(colors: List[Color]) -> List[str]:
    return [str(c) for c in colors]
