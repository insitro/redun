from redun import task


redun_namespace = "redun.examples.simple"


@task()
def adder(a: int, b: int):
    return a + b

@task()
def multiplier(a: int, b: int):
    return a * b

@task()
def my_zip(my_task, pairs):
    results = []
    for a, b in pairs:
        results.append(my_task(a, b))
    return results

@task()
def workflow(method: str="adder"):
    pairs = [
        (1, 2),
        (10, 11),
        (3, 4),
    ]
    if method == "adder":
        my_task = adder
    elif method == "multiplier":
        my_task = multiplier
    else:
        raise ValueError("Unknown method")
    return my_zip(my_task, pairs)
