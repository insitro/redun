import random
import time

from redun import catch, cond, task
from redun.functools import seq

redun_namespace = "redun.examples.control_flow"


@task()
def divide(a: float, b: float) -> float:
    """
    Divide two numbers. This will fail if b=0.
    """
    return a / b


@task()
def double(x: int) -> int:
    """
    Doubles a number.
    """
    return 2 * x


@task()
def recover(error: Exception, result: float) -> float:
    """
    This task recovers from an error.
    """
    print("Warning:", error)
    return result


@task()
def safe_divide(a: float, b: float, default: float = 0) -> float:
    """
    This task will recover from DivideByZeroErrors by using the default value.
    """
    # Use partial application to set the `default` argument. `error` will be
    # set to the exception raised.
    return catch(divide(a, b), ZeroDivisionError, recover.partial(result=default))


@task()
def boom(message: str) -> None:
    """
    This task raises an Exception.
    """
    raise ValueError(message)


@task()
def will_boom(message: str) -> None:
    """
    This task will indirectly raise an exception.
    """
    return boom(message)


@task()
def recover_reraise(error: Exception):
    """
    Reraise an error and change the exception type.
    """
    raise KeyError(f"Reraised from {error}") from error


@task()
def will_reraise(message: str):
    return catch(boom(message), ValueError, recover_reraise)


@task()
def printer(message: str) -> str:
    """
    Prints a string to the stdout.
    """
    # Use a random sleep to force printing order to be random, unless seq() is used.
    time.sleep(random.random())
    print(message)
    return message


@task()
def main() -> dict:
    results = {}

    # Example of recovering from exceptions like ZeroDivisionError.
    results["10/5"] = safe_divide(10, 5)
    results["10/0"] = safe_divide(10, 0)

    # Example of a raised error.
    results["boom"] = catch(boom("BOOM"), ValueError, recover.partial(result="Defused..."))
    results["will_boom"] = catch(
        will_boom("BOOM"), ValueError, recover.partial(result="Defused...")
    )
    results["reraise"] = catch(
        will_reraise("BOOM"), KeyError, recover.partial(result="Defused...")
    )

    # Print results in any order.
    # However, final return values will be in order.
    results["random_prints"] = [printer(str(i)) for i in range(10)]

    # Force sequential evaluation of printers.
    results["seq_prints"] = seq([printer(str(i)) for i in range(10, 20)])

    # cond is a lazy if-statement for testing the result of a task.
    # We are also using lazy operators below.
    results["cond_true"] = cond(divide(10, 5) == 2.0, double(10), double(20))
    results["cond_false"] = cond(double(2) + 1 == 4, double(10), double(20))

    return results
