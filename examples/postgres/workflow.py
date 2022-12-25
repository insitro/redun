from redun import task

redun_namespace = "redun.examples.postgres"


@task()
def task1(x):
    return x + 1


@task()
def main():
    return task1(10)
