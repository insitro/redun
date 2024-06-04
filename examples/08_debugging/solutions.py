###################################################################################################
# Make new task solution:
#   - returning cow in assign_cow_favorite_number
#   - create list tasks: square_list, assign_cow_list_to_number_list, filter_out_odd_numbers
###################################################################################################

"""
import random
from dataclasses import dataclass
from typing import Optional

from redun import task
from redun.functools import eval_

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
    return cow


@task()
def square_number(x: int) -> int:
    return x**2

@task()
def square_list(l: list) -> list:
    return [square_number(x) for x in l]

@task()
def assign_cow_list_to_number_list(cow_list: list[Cow], number_list: list[int]) -> list[Cow]:
    return [assign_cow_favorite_number(cow, random.choice(number_list)) for cow in cow_list]

@task()
def filter_out_odd_numbers(l: list) -> list:
    return [x for x in l if x % 2 == 0]

@task()
def main() -> list[Cow]:
    numbers = [1, 2, 3, 5, 10]
    squared_numbers = square_list(numbers)

    even_squared_numbers = filter_out_odd_numbers(squared_numbers)

    cows = [Cow(name="Flora"), Cow(name="Oreo")]
    cows = assign_cow_list_to_number_list(cows, even_squared_numbers)

    return cows
"""




###################################################################################################
# Make NO new tasks solution (we've made all the pure python lazy):
#   - returning cow in assign_cow_favorite_number
#   - eval_ for quick list manipulation on TaskExpression, results passed into new task though
#       (not immediately used bc of "TypeError: object of type 'TaskExpression' has no len()" error)
#   - wrap random.choice in as_task to make it lazy
###################################################################################################

"""
import random
from dataclasses import dataclass
from typing import Optional

from redun import task
from redun.functools import eval_, as_task

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
    return cow


@task()
def square_number(x: int) -> int:
    return x**2

@task()
def main() -> list[Cow]:
    numbers = [1, 2, 3, 5, 10]
    squared_numbers = [square_number(number) for number in numbers]

    even_squared_numbers = eval_("[x for x in squared_numbers if x % 2 == 0]", squared_numbers=squared_numbers)

    cows = [Cow(name="Flora"), Cow(name="Oreo")]
    cows = [assign_cow_favorite_number(cow, as_task(random.choice)(even_squared_numbers)) for cow in cows]

    return cows
"""




###################################################################################################
# Make NO new tasks solution (we've made all the pure python lazy) w/functools BONUS:
#   - returning cow in assign_cow_favorite_number
#   - eval_ for quick list manipulation on TaskExpression
#   - wrap random.choice in as_task to make it lazy
#   - BONUS: USING map_ instead of list comprehension
#   - BONUS: USING starmap - but arguably lengthier...
###################################################################################################

"""
import random
from dataclasses import dataclass
from typing import Optional

from redun import task
from redun.functools import eval_, as_task, map_

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
    return cow

@task()
def square_number(x: int) -> int:
    return x**2

@task()
def main() -> list[Cow]:
    numbers = [1, 2, 3, 5, 10]
    squared_numbers = map_(square_number, numbers)

    even_squared_numbers = eval_("[x for x in squared_numbers if x % 2 == 0]", squared_numbers=squared_numbers)

    cows = [Cow(name="Flora"), Cow(name="Oreo")]
    cows = [assign_cow_favorite_number(cow, as_task(random.choice)(even_squared_numbers)) for cow in cows]
    
    # starmap implementation:
    # cows = starmap(assign_cow_favorite_number, [{"cow":cow, "favorite_number":as_task(random.choice)(even_squared_numbers)} for cow in cows])

    return cows
"""