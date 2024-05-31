import random
from dataclasses import dataclass
from typing import Optional

from redun import task

redun_namespace = "redun.examples.debugging"


@dataclass
class Cow:
    name: str
    favorite_number: Optional[int] = None

    def __str__(self):
        return f"{self.name} has favorite number {self.favorite_number}"


@task()
def assign_cow_favorite_number(cow: Cow, favorite_number: int):
    cow.favorite_number = favorite_number


@task()
def square_number(x: int) -> int:
    return x**2


@task()
def main() -> list[int]:
    numbers = [1, 3, 5]
    squared_numbers = [square_number(x) for x in numbers]

    even_squared_numbers = [x for x in squared_numbers if x % 2 == 0]

    cows = [Cow(name="Flora"), Cow(name="Oreo")]
    for cow in cows:
        assign_cow_favorite_number(cow, random.choice(even_squared_numbers))
    return cows
