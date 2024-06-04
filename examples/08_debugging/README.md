# Redun Debugging

Let's get some practice with running and debugging redun workflows!

Here I have a not-at-all contrived example of a workflow that is failing. I'm trying to write a task 
to assign some favorite numbers to my favorite pet cows. However, my cows are very picky and only like
even, square numbers. Your job is to run the redun workflow and figure out what is going wrong!

```py
from dataclasses import dataclass
from typing import Optional
import random

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

```

Give this workflow a run by running the following command, and seeing if you can get it to succeed!
```sh 
redun run workflow.py main
```

## Extension: Run it on AWS Batch!

Let's make a couple edits. Let's see if we can do the following things:

1. Add a task that writes a dataframe of my cows & their favorite numbers, and saves it to my scratch space in S3:`s3://insitro-scratch/my-name/cows.csv`
2. Update the code so that the task that is assigning cow names will run on AWS Batch.
