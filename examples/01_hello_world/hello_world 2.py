from redun import task


redun_namespace = "redun.examples.hello_world"


@task()
def get_planet() -> str:
    return "World"


@task()
def greeter(greet: str, thing: str) -> str:
    return '{}, {}!'.format(greet, thing)


@task()
def main(greet: str = 'Hello') -> str:
    return greeter(greet, get_planet())
