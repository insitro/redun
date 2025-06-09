from redun import task

redun_namespace = "redun.examples.eager_eval"


@task
def add(a, b):
    return a + b


@task
def is_odd(x):
    return x % 2 != 0


@task(cache=True, check_valid="shallow")
async def main():
    result = add(1, 2)
    odd_answer = await is_odd(result)

    # By using await, `odd_answer` is now a normal bool that is usable in an if-statement.
    if odd_answer:
        return "odd"
    else:
        return "even"
