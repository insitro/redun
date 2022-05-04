import os
from typing import List, Optional

from redun import task, File
from redun.functools import const


redun_namespace = "redun.examples.compile2"


# Custom DSL for describing targets, dependencies (deps), and commands.
rules = {
    "all": {
        "deps": ["prog", "prog2"],
    },
    "prog": {
        "deps": ["prog.o", "lib.o"],
        "command": "gcc -o prog prog.o lib.o",
    },
    "prog2": {
        "deps": ["prog2.o", "lib.o"],
        "command": "gcc -o prog2 prog2.o lib.o",
    },
    "prog.o": {
        "deps": ["prog.c"],
        "command": "gcc -c prog.c",
    },
    "prog2.o": {
        "deps": ["prog2.c"],
        "command": "gcc -c prog2.c",
    },
    "lib.o": {
        "deps": ["lib.c"],
        "command": "gcc -c lib.c",
    },
}


@task()
def run_command(command: str, inputs: List[File], output_path: str) -> File:
    """
    Run a shell command to produce a target.
    """
    # Ignore inputs. We pass it as an argument to simply force a dependency.
    assert os.system(command) == 0
    return File(output_path)


@task()
def make(target: str = "all", rules: dict = rules) -> Optional[File]:
    """
    Make a target (file) using a series of rules.
    """
    rule = rules.get(target)
    if not rule:
        # No rule. See if target already exists.
        file = File(target)
        if not file.exists():
            raise ValueError(f"No rule for target: {target}")
        return file

    # Recursively make dependencies.
    inputs = [
        make(dep, rules=rules)
        for dep in rule.get("deps", [])
    ]

    # Run command, if needed.
    if "command" in rule:
        return run_command(rule["command"], inputs, target)
    else:
        return const(None, inputs)
