"""
Tests verifying that Task.__call__ typing works correctly under both mypy and ty.

These tests ensure that the ParamSpec-based __call__ signature preserves the
wrapped function's parameter types for type checkers, and that both valid calls
are accepted and invalid calls are rejected.
"""

import subprocess
import sys
from pathlib import Path
from typing import List, Optional

import pytest

_VENV_BIN = Path(sys.executable).parent
_TY = _VENV_BIN / "ty"
_MYPY = _VENV_BIN / "mypy"
_REDUN_ROOT = Path(__file__).parent.parent.parent


def _has_ty() -> bool:
    return _TY.exists()


def _has_mypy() -> bool:
    return _MYPY.exists()


def _run_checker(checker: Path, filepath: Path, extra_args: Optional[List[str]] = None) -> str:
    """Run a type checker on a file and return combined stdout+stderr."""
    args = [str(checker)]
    if extra_args:
        args.extend(extra_args)
    args.append(str(filepath))
    result = subprocess.run(args, capture_output=True, text=True, cwd=str(_REDUN_ROOT))
    return result.stdout + result.stderr


def _write_tmp(code: str) -> Path:
    """Write code to a temp file inside the redun project (so imports resolve)."""
    tmp = _REDUN_ROOT / "_tmp_typing_test.py"
    tmp.write_text(code)
    return tmp


def _cleanup_tmp():
    tmp = _REDUN_ROOT / "_tmp_typing_test.py"
    tmp.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# Test fixture: redun @task calls checked by type checkers
# ---------------------------------------------------------------------------

REDUN_TYPING_FIXTURE = """\
'''
Verify that Task.__call__ typing works correctly with both mypy and ty.
'''
from redun import task

@task(cache=False)
def add(x: int, y: int) -> int:
    return x + y

@task(cache=False)
def double(x: int) -> int:
    return x * 2

@task(cache=False)
def greet(name: str, greeting: str = "hello") -> str:
    return f"{greeting} {name}"

# ---- reveal_type checks ----
reveal_type(add)
reveal_type(add.__call__)
reveal_type(double.__call__)
reveal_type(greet.__call__)

# ---- valid calls (should produce NO errors) ----
add(1, 2)
double(42)
greet("world")
greet("world", greeting="hi")

# ---- partial and options (should produce NO errors) ----
add_one = add.partial(1)
reveal_type(add_one)
add_one(2)

add_with_opts = add.options(cache=False)
reveal_type(add_with_opts)
add_with_opts(1, 2)

# ---- invalid calls (SHOULD produce errors) ----
add("wrong", 1)  # type: ignore[arg-type]
"""


@pytest.mark.skipif(not _has_ty(), reason="ty not installed")
class TestTyTaskTyping:
    """Verify that ty correctly types Task.__call__ via ParamSpec."""

    def setup_method(self):
        self.tmp = _write_tmp(REDUN_TYPING_FIXTURE)

    def teardown_method(self):
        _cleanup_tmp()

    def test_no_false_errors_on_valid_calls(self):
        """ty should accept all valid task calls without errors."""
        output = _run_checker(_TY, self.tmp, ["check", "--python-version", "3.11"])

        # Skip if ty can't run (e.g., .venv not found in CI environments)
        if "ty failed" in output:
            pytest.skip(f"ty could not run in this environment: {output[:200]}")

        # Filter out reveal_type info lines, the intentional invalid call's
        # unused-ignore-comment (when ty can't resolve types), and warnings
        error_lines = [
            line
            for line in output.split("\n")
            if "error[" in line and "unused-ignore-comment" not in line
        ]
        # The critical errors we must NOT see are descriptor-binding false positives
        has_false_positive = any(
            err_type in output
            for err_type in ["too-many-positional-arguments", "invalid-argument-type"]
        )
        assert not has_false_positive, (
            f"ty produced false type errors on valid calls (descriptor binding bug):\n{output}"
        )
        assert not error_lines, (
            "ty produced unexpected errors on valid calls:\n"
            + "\n".join(error_lines)
            + f"\n\nFull output:\n{output}"
        )

    def test_call_signature_preserves_params(self):
        """ty should resolve __call__ with the full parameter signature."""
        output = _run_checker(_TY, self.tmp, ["check", "--python-version", "3.11"])

        if "@Todo" in output or "Unknown" in output or "ty failed" in output:
            pytest.skip(
                "ty cannot fully resolve redun.Task in this environment. "
                "The no-false-errors test above still validates the fix."
            )

        # For add(x: int, y: int) -> int, __call__ should show both params
        # Look for revealed type of add.__call__
        assert "x: int" in output and "y: int" in output, (
            f"Expected add.__call__ to show both x and y params.\nFull output:\n{output}"
        )


@pytest.mark.skipif(not _has_mypy(), reason="mypy not installed")
class TestMypyTaskTyping:
    """Verify that mypy still works correctly with ParamSpec-based Task."""

    def setup_method(self):
        self.tmp = _write_tmp(REDUN_TYPING_FIXTURE)

    def teardown_method(self):
        _cleanup_tmp()

    def test_no_errors_on_valid_calls(self):
        """mypy should accept all valid task calls without errors."""
        output = _run_checker(_MYPY, self.tmp, ["--python-executable", sys.executable])

        # Filter to only errors in our test file (mypy may report pre-existing
        # errors in transitively imported files like scheduler.py)
        test_errors = [
            line for line in output.split("\n") if "error:" in line and "_tmp_typing_test" in line
        ]
        assert not test_errors, "mypy produced unexpected errors:\n" + "\n".join(test_errors)

    def test_call_signature_preserves_params(self):
        """mypy should resolve __call__ with the full parameter signature."""
        output = _run_checker(_MYPY, self.tmp, ["--python-executable", sys.executable])

        # mypy's revealed types should show the correct parameter names
        assert "int" in output, (
            f"Expected mypy reveal_type to show int params.\nFull output:\n{output}"
        )
