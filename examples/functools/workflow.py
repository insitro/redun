from typing import Any, Dict, List

from redun import task
from redun.functools import (
    apply_func,
    as_task,
    compose,
    const,
    delay,
    eval_,
    flat_map,
    flatten,
    force,
    map_,
    starmap,
)
from redun.task import Task

redun_namespace = "redun.examples.functools"


@task()
def double(x: int) -> int:
    return 2 * x


@task()
def add(a: int, b: int) -> int:
    return a + b


@task()
def make_row(x: int) -> Dict[str, int]:
    return {"a": x - 1, "b": x + 1}


@task()
def printer(x: Any) -> Any:
    print(x)
    return x


@task()
def run_delay(delayed: Task) -> int:
    print("run_delay", delayed)
    return force(delayed)


@task()
def map_tree(a_task: Task, tree: Any) -> Any:
    """
    Map a task over a tree of values (list of lists of ...).

    This is an example of custom higher-order task (i.e. a task that takes another
    task as an argument).
    """

    def map_node(node):
        if isinstance(node, list):
            return [map_node(child) for child in node]
        else:
            return a_task(node)

    return map_node(tree)


@task()
def split_words(text: str) -> List[str]:
    return text.strip().split()


@task()
def range_(n: int) -> List[int]:
    return list(range(n))


@task()
def main() -> dict:
    results = {}

    # Use map_ to map a task across many values.
    values = list(range(10))
    results["map"] = map_(double, values)

    # map_ can also map a lazy list of values, `values2`.
    values2 = map_(double, values)
    results["map_map"] = map_(double, values2)

    # Let's make some rows of data.
    rows = map_(make_row, values)

    # Let's map a function by keyword arguments across the rows.
    results["starmap"] = starmap(add, rows)

    # Let's find all the words in some sentences.
    sentences = ["hello world", "not invented here"]
    word_lists = map_(split_words, sentences)
    results["word_lists"] = word_lists

    # If we want a flat list of words, we can use flatten.
    results["words"] = flatten(word_lists)

    # It's common to flatten after map, so we can use flat_map as well.
    results["words2"] = flat_map(split_words, sentences)

    # We can use partial application to make new tasks.
    inc = add.partial(1)
    results["inc"] = map_(inc, values)

    # We can use compose to build up new tasks as well.
    double_inc = compose(double, inc)
    results["compose"] = double_inc(2)

    # Sometimes you need to compute two things, but really only keep one result.
    results["const"] = const(printer("hello"), printer("bye"))

    # We can convert a plain Python function to task to apply it to lazy values.
    values3 = range_(10)
    results["max"] = as_task(max)(values3)

    # We can also apply a plain Python to lazy arguments using apply_func.
    results["len"] = apply_func(len, values3)

    # We create an expression and delay its execution until later.
    delayed = delay(add(4, 5))
    results["delay_force"] = run_delay(delayed)

    # Sometimes you need to define a small task, but want to do so inline for
    # convenience.
    results["eval"] = eval_("a * b", a=2, b=3)

    # eval_ can also accept positional args if you specify their names.
    results["eval_pos_args"] = eval_("x * 10", 4, pos_args=["x"])

    # Combined with partial, we could use eval_ to make new tasks that could be combined with map_.
    triple = eval_.partial("x * 3", pos_args=["x"])
    results["eval_map"] = map_(triple, values)

    # Using `map_.partial()` we can turn any task that processes a single value into one that
    # processes a list of values.
    make_triples = map_.partial(triple)
    results["make_triples"] = make_triples(values)

    # Tasks can be passed as arguments to other tasks. So we can define our own
    # higher-order tasks if we want. Here we make a "map" that works on trees.
    results["map_tree"] = map_tree(double, [1, 2, [3, 4]])

    return results
