from redun import task
from redun.functools import no_prov

redun_namespace = "redun.examples.no_prov"


@task()
def add(a: int, b: int) -> int:
    return a + b


@task()
def add4(a: int, b: int, c: int, d: int) -> int:
    return add(add(a, b), add(c, d))


@task(prov=False)
def add4_no_prov(a: int, b: int, c: int, d: int) -> int:
    return add(add(a, b), add(c, d))


@task()
def main() -> dict:
    results = {}

    # `add4` will record the providence of its child task calls (i.e. 3 `add` calls).
    results["prov"] = add(1, add4(1, 2, 3, 4))

    # Using `prov=False` as a task option, we can skip recording provenance of add4's child task calls.
    results["no_prov_task"] = add(1, add4_no_prov(5, 6, 7, 8))

    # Like any other task option, we can adjust `prov` at call-time.
    results["no_prov_options"] = add(1, add4.options(prov=False)(5, 6, 7, 8))

    # Alternatively, we can use the `no_prov()` expression to suppress provenance.
    results["no_prov"] = add(1, no_prov(add4(9, 10, 11, 12)))

    # `no_prov()` is useful when an expression is not just one task call.
    results["no_options_expr"] = add(1, no_prov(add(13, 14) + 15 + 16))

    return results
