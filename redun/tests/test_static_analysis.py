import os
import subprocess
import warnings
from inspect import isclass, signature

import pytest
from numpydoc.docscrape import ClassDoc, FunctionDoc

import redun
from redun.tests.utils import (
    docstring_owner_pretty_name,
    get_docstring_owners_in_module,
    import_all_modules,
)


@pytest.mark.parametrize(
    "docstring_owner",
    [
        owner
        for module in import_all_modules(redun)
        for owner in get_docstring_owners_in_module(module)
    ],
    ids=docstring_owner_pretty_name,
)
def test_docstrings(docstring_owner):
    """Check if the docstring of `docstring_owner` adheres to the numpy standard.

    Currently enforces
    - Docstring section names https://numpydoc.readthedocs.io/en/latest/format.html#sections
    - Documented `Parameters` match the defined arguments

    Notes
    -----
    This function naively assumes that if `docstring_owner` is not a class,
    then its docstring should be parsable using `FunctionDoc`.
    """
    doc_parser = ClassDoc if isclass(docstring_owner) else FunctionDoc
    # Raise warnings as errors during docstring parsing.
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        parsed_doc_string = doc_parser(docstring_owner)

    # Strip leading star(s) for before comparing.
    # https://numpydoc.readthedocs.io/en/latest/format.html#parameters
    documented_param_names = {param.name.lstrip("*") for param in parsed_doc_string["Parameters"]}
    defined_argument_names = set(signature(docstring_owner).parameters.keys()) - {"cls", "self"}
    if documented_param_names:
        assert defined_argument_names == documented_param_names, (
            f"In `{docstring_owner_pretty_name(docstring_owner)}`, "
            f"defined arguments do not match documented `Parameters`"
        )


def test_no_banned_pickle_usage():
    """
    Check that dumping of pickled content uses our internal functions, not built-in pickle.

    Python 3.8 changed the default protocol for pickle. To avoid any inconsistencies in the
    output of dumped pickle content across different python versions, we want to use our own
    functions for this as they pin the protocol and will be consistent across all python versions.
    This is important because dumping of pickled content is used in hashing.
    """
    completed_process = subprocess.run(
        [
            "git",
            "grep",
            "-n",
            "-e",
            "pickle\\.dump",
            "-e",
            "from pickle import dump",
            "--",
            "redun/*.py",
        ],
        stdout=subprocess.PIPE,
        universal_newlines=True,
    )

    # Make sure we exited with 0 exit code otherwise grep failed and there's no point going on.
    completed_process.check_returncode()

    # Bless the imports in redun.utils via their named imports and this file.
    def _is_allowed_occurrence(line):
        return (
            "allowed_dump_func" in line
            or "allowed_dumps_func" in line
            or f"{os.path.basename(__file__)}:" in line
        )

    banned_calls = [
        line for line in completed_process.stdout.splitlines() if not _is_allowed_occurrence(line)
    ]

    # We use os.linesep below because f-strings do not allow backslashes so we can't use \n.
    assert not banned_calls, (
        f"Found {len(banned_calls)} banned usages. Use redun.utils.pickle_dump(s) instead: "
        f"{os.linesep}{os.linesep.join(banned_calls)}"
    )
