from mypy import api

from redun.tests.utils import get_test_file


def test_task_types() -> None:
    """
    mypy should find type errors related to redun task calls.
    """
    workflow_file = get_test_file("test_data/typing/workflow_fail.py.txt")

    stdout, stderr, ret_code = api.run(
        [
            "--show-traceback",
            workflow_file,
            "redun",
        ]
    )
    print(stdout)
    assert ret_code == 1

    # Parse found type check errors.
    stdout_lines = stdout.split("\n")[:-2]
    found_errors = {line.split(":", 2)[1] for line in stdout_lines}

    # Get lines with expected errors.
    with open(workflow_file) as infile:
        expected_errors = {str(i) for i, line in enumerate(infile, 1) if "ERROR" in line}

    assert found_errors == expected_errors
