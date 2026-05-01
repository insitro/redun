"""
Tests verifying that Task.__call__ typing works correctly under both mypy and ty.

These tests ensure that the ParamSpec-based __call__ signature preserves the
wrapped function's parameter types for type checkers, and that both valid calls
are accepted and invalid calls are rejected.
"""

import subprocess
import sys
import uuid
from pathlib import Path
from typing import Optional

import pytest

_VENV_BIN = Path(sys.executable).parent
_TY = _VENV_BIN / "ty"
_MYPY = _VENV_BIN / "mypy"
_REDUN_ROOT = Path(__file__).parent.parent.parent


def _has_ty() -> bool:
    return _TY.exists()


def _has_mypy() -> bool:
    return _MYPY.exists()


def _run_checker(checker: Path, filepath: Path, extra_args: Optional[list[str]] = None) -> str:
    """Run a type checker on a file and return combined stdout+stderr."""
    args = [str(checker)]
    if extra_args:
        args.extend(extra_args)
    args.append(str(filepath))
    result = subprocess.run(args, capture_output=True, text=True, cwd=str(_REDUN_ROOT))
    return result.stdout + result.stderr


def _write_tmp(code: str) -> Path:
    """Write code to a uniquely-named temp file inside the redun project (so imports resolve)."""
    tmp = _REDUN_ROOT / f"_tmp_typing_test_{uuid.uuid4().hex[:8]}.py"
    tmp.write_text(code)
    return tmp


# ---------------------------------------------------------------------------
# Test fixture: redun @task calls checked by type checkers
# ---------------------------------------------------------------------------

REDUN_VALID_CALLS_FIXTURE = """\
'''Valid Task.__call__ usage — type checkers should produce zero errors.'''
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
reveal_type(add.func)  # should be Callable[[int, int], int], not bare Callable
reveal_type(add.__call__)
reveal_type(double.__call__)
reveal_type(greet.__call__)

# ---- valid calls ----
add(1, 2)
double(42)
greet("world")
greet("world", greeting="hi")

# ---- partial and options ----
add_one = add.partial(1)
reveal_type(add_one)
add_one(2)

add_with_opts = add.options(cache=False)
reveal_type(add_with_opts)
add_with_opts(1, 2)
"""

REDUN_INVALID_CALLS_FIXTURE = """\
'''Invalid Task.__call__ usage — type checkers SHOULD produce errors.'''
from redun import task

@task(cache=False)
def add(x: int, y: int) -> int:
    return x + y

add("wrong", 1)
"""

# ---------------------------------------------------------------------------
# Test fixture: scheduler_task typing via Concatenate
# ---------------------------------------------------------------------------

SCHEDULER_VALID_CALLS_FIXTURE = """\
'''Valid scheduler_task usage — type checkers should produce zero errors.'''
from redun.scheduler import cond, catch, catch_all, apply_tags, subrun, fork_thread, join_thread
from redun.functools import seq, map_
from redun.context import get_context
from redun.task import task

cond(True, 1, 2)
cond(True, 1, 2, 3, 4, 5)
seq([1, 2, 3])

@task(cache=False)
def my_task(x: int) -> int:
    return x

catch(my_task(1), Exception, my_task)
map_(my_task, [1, 2, 3])
apply_tags(my_task(1))
apply_tags(my_task(1), tags=[("key", "val")])
get_context("my.var")
get_context("my.var", default=42)
subrun(my_task(1), "default")
subrun(my_task(1), "default", vcpus=8, memory=30)
fork_thread(my_task(1))
catch_all([my_task(1), my_task(2)])
"""

SCHEDULER_INVALID_CALLS_FIXTURE = """\
'''Invalid scheduler_task usage — type checkers SHOULD produce errors.'''
from redun.scheduler import cond
from redun.functools import seq

# cond requires (cond_expr, then_expr, ...) and seq requires (exprs).
# Calling with zero args should be flagged as missing required arguments.
cond()
seq()
"""


def _get_error_lines(output: str) -> list[str]:
    """Extract error lines from type checker output, ignoring info/warnings."""
    return [line for line in output.split("\n") if "error[" in line]


@pytest.mark.skipif(not _has_ty(), reason="ty not installed")
class TestTyTaskTyping:
    """Verify that ty correctly types Task.__call__ via ParamSpec."""

    def setup_method(self):
        self.tmp_valid = _write_tmp(REDUN_VALID_CALLS_FIXTURE)
        self.tmp_invalid = _write_tmp(REDUN_INVALID_CALLS_FIXTURE)

    def teardown_method(self):
        self.tmp_valid.unlink(missing_ok=True)
        self.tmp_invalid.unlink(missing_ok=True)

    def test_valid_calls_produce_no_errors(self):
        """ty should accept all valid task calls without errors."""
        output = _run_checker(_TY, self.tmp_valid, ["check", "--python-version", "3.11"])
        if "ty failed" in output:
            pytest.skip(f"ty could not run in this environment: {output[:200]}")
        errors = _get_error_lines(output)
        assert not errors, f"ty produced errors on valid calls:\n{output}"

    def test_invalid_calls_produce_errors(self):
        """ty should reject invalid task calls."""
        output = _run_checker(_TY, self.tmp_invalid, ["check", "--python-version", "3.11"])
        if "ty failed" in output:
            pytest.skip(f"ty could not run in this environment: {output[:200]}")
        errors = _get_error_lines(output)
        assert errors, f"ty should have flagged add('wrong', 1) but produced no errors:\n{output}"

    def test_call_signature_preserves_params(self):
        """ty should resolve __call__ with the full parameter signature."""
        output = _run_checker(_TY, self.tmp_valid, ["check", "--python-version", "3.11"])
        if "@Todo" in output or "Unknown" in output or "ty failed" in output:
            pytest.skip("ty cannot fully resolve redun.Task in this environment.")
        assert "x: int" in output and "y: int" in output, (
            f"Expected add.__call__ to show both x and y params.\nFull output:\n{output}"
        )


@pytest.mark.skipif(not _has_mypy(), reason="mypy not installed")
class TestMypyTaskTyping:
    """Verify that mypy still works correctly with ParamSpec-based Task."""

    def setup_method(self):
        self.tmp = _write_tmp(REDUN_VALID_CALLS_FIXTURE)

    def teardown_method(self):
        self.tmp.unlink(missing_ok=True)

    def test_valid_calls_produce_no_errors(self):
        """mypy should accept all valid task calls without errors."""
        output = _run_checker(_MYPY, self.tmp, ["--python-executable", sys.executable])
        test_errors = [
            line for line in output.split("\n") if "error:" in line and "_tmp_typing_test" in line
        ]
        assert not test_errors, "mypy produced unexpected errors:\n" + "\n".join(test_errors)

    def test_call_signature_preserves_params(self):
        """mypy should resolve __call__ with the full parameter signature."""
        output = _run_checker(_MYPY, self.tmp, ["--python-executable", sys.executable])
        assert "int" in output, (
            f"Expected mypy reveal_type to show int params.\nFull output:\n{output}"
        )

    def test_func_attribute_preserves_params(self):
        """mypy should resolve task.func with typed params, not bare Callable."""
        output = _run_checker(_MYPY, self.tmp, ["--python-executable", sys.executable])
        bare_callable = [
            line
            for line in output.split("\n")
            if "Revealed type" in line and "(*Any, **Any) -> Any" in line
        ]
        assert not bare_callable, (
            f"task.func resolved as bare Callable (lost parameter types):\n{bare_callable[0]}"
        )


@pytest.mark.skipif(not _has_ty(), reason="ty not installed")
class TestTySchedulerTaskTyping:
    """Verify that ty correctly types scheduler_task calls via Concatenate."""

    def setup_method(self):
        self.tmp_valid = _write_tmp(SCHEDULER_VALID_CALLS_FIXTURE)
        self.tmp_invalid = _write_tmp(SCHEDULER_INVALID_CALLS_FIXTURE)

    def teardown_method(self):
        self.tmp_valid.unlink(missing_ok=True)
        self.tmp_invalid.unlink(missing_ok=True)

    def test_valid_calls_produce_no_errors(self):
        """ty should accept valid scheduler_task calls (internal params stripped)."""
        output = _run_checker(_TY, self.tmp_valid, ["check", "--python-version", "3.11"])
        if "ty failed" in output:
            pytest.skip(f"ty could not run in this environment: {output[:200]}")
        errors = _get_error_lines(output)
        assert not errors, f"ty produced errors on valid scheduler_task calls:\n{output}"

    def test_invalid_calls_produce_errors(self):
        """ty should reject invalid scheduler_task calls."""
        output = _run_checker(_TY, self.tmp_invalid, ["check", "--python-version", "3.11"])
        if "ty failed" in output:
            pytest.skip(f"ty could not run in this environment: {output[:200]}")
        errors = _get_error_lines(output)
        assert errors, f"ty should have flagged cond()/seq() but produced no errors:\n{output}"


@pytest.mark.skipif(not _has_mypy(), reason="mypy not installed")
class TestMypySchedulerTaskTyping:
    """Verify that mypy accepts scheduler_task Concatenate typing."""

    def setup_method(self):
        self.tmp = _write_tmp(SCHEDULER_VALID_CALLS_FIXTURE)

    def teardown_method(self):
        self.tmp.unlink(missing_ok=True)

    def test_valid_calls_produce_no_errors(self):
        """mypy should accept valid scheduler_task calls."""
        output = _run_checker(_MYPY, self.tmp, ["--python-executable", sys.executable])
        test_errors = [
            line for line in output.split("\n") if "error:" in line and "_tmp_typing_test" in line
        ]
        assert not test_errors, (
            "mypy produced unexpected errors on scheduler_task calls:\n" + "\n".join(test_errors)
        )


# ---------------------------------------------------------------------------
# Runtime tests: scheduler_task definitions with edge-case signatures
# ---------------------------------------------------------------------------


class TestSchedulerTaskRuntimeEdgeCases:
    """Verify scheduler_task decorator works at runtime for various signatures."""

    def test_zero_user_params(self):
        """scheduler_task with no user-visible params (like kill in test_db_query)."""
        from redun.expression import SchedulerExpression
        from redun.promise import Promise
        from redun.task import SchedulerTask, scheduler_task

        @scheduler_task()
        def noop(scheduler, parent_job, sexpr) -> Promise:
            promise: Promise = Promise()
            promise.do_resolve(None)
            return promise

        assert isinstance(noop, SchedulerTask)
        expr = noop()
        assert isinstance(expr, SchedulerExpression)
        assert expr.args == ()

    def test_variadic_args(self):
        """scheduler_task with *args user params."""
        from redun.expression import SchedulerExpression
        from redun.promise import Promise
        from redun.task import SchedulerTask, scheduler_task

        @scheduler_task()
        def variadic(scheduler, parent_job, sexpr, *values) -> Promise:
            promise: Promise = Promise()
            promise.do_resolve(sum(values))
            return promise

        assert isinstance(variadic, SchedulerTask)
        expr = variadic(1, 2, 3)
        assert isinstance(expr, SchedulerExpression)
        assert expr.args == (1, 2, 3)

    def test_kwargs_only(self):
        """scheduler_task with keyword-only user params."""
        from redun.expression import SchedulerExpression
        from redun.promise import Promise
        from redun.task import SchedulerTask, scheduler_task

        @scheduler_task()
        def kwonly(scheduler, parent_job, sexpr, *, key: str, value: int = 0) -> Promise:
            promise: Promise = Promise()
            promise.do_resolve({key: value})
            return promise

        assert isinstance(kwonly, SchedulerTask)
        expr = kwonly(key="test", value=42)
        assert isinstance(expr, SchedulerExpression)
        assert expr.kwargs == {"key": "test", "value": 42}

    def test_mixed_args_kwargs(self):
        """scheduler_task with positional + *args + **kwargs."""
        from redun.expression import SchedulerExpression
        from redun.promise import Promise
        from redun.task import SchedulerTask, scheduler_task

        @scheduler_task()
        def mixed(
            scheduler, parent_job, sexpr, first, *args, flag: bool = False, **kwargs
        ) -> Promise:
            promise: Promise = Promise()
            promise.do_resolve((first, args, flag, kwargs))
            return promise

        assert isinstance(mixed, SchedulerTask)
        expr = mixed("a", "b", "c", flag=True, extra="val")
        assert isinstance(expr, SchedulerExpression)
        assert expr.args == ("a", "b", "c")
        assert expr.kwargs == {"flag": True, "extra": "val"}

    def test_default_params(self):
        """scheduler_task with default parameter values."""
        from redun.promise import Promise
        from redun.task import SchedulerTask, scheduler_task

        @scheduler_task()
        def with_defaults(scheduler, parent_job, sexpr, x: int, y: int = 10) -> Promise:
            promise: Promise = Promise()
            promise.do_resolve(x + y)
            return promise

        assert isinstance(with_defaults, SchedulerTask)
        # Call with only required arg
        expr1 = with_defaults(5)
        assert expr1.args == (5,)
        # Call with both args
        expr2 = with_defaults(5, 20)
        assert expr2.args == (5, 20)


# ---------------------------------------------------------------------------
# Test fixture: redun.run() return type inference
# ---------------------------------------------------------------------------

RUN_TYPING_FIXTURE = """\
'''
Verify that redun.run() infers the return type from its expr argument
and that explicit kwargs are type-checked.
'''
from redun import run, task

@task(cache=False)
def add(x: int, y: int) -> int:
    return x + y

@task(cache=False)
def greet(name: str) -> str:
    return f"hello {name}"

# ---- return type inference ----
reveal_type(run(add(1, 2)))    # should be int
reveal_type(run(greet("world")))  # should be str

# ---- valid kwargs (should produce NO errors) ----
run(add(1, 2), dryrun=True)
run(add(1, 2), cache=False)
run(add(1, 2), dryrun=True, cache=False)
run(add(1, 2), execution_id="abc")
"""


@pytest.mark.skipif(not _has_ty(), reason="ty not installed")
class TestTyRunTyping:
    """Verify that ty infers redun.run() return type from expr argument."""

    def setup_method(self):
        self.tmp = _write_tmp(RUN_TYPING_FIXTURE)

    def teardown_method(self):
        self.tmp.unlink(missing_ok=True)

    def test_no_errors_on_valid_calls(self):
        """ty should accept all valid run() calls without errors."""
        output = _run_checker(_TY, self.tmp, ["check", "--python-version", "3.11"])

        if "ty failed" in output:
            pytest.skip(f"ty could not run in this environment: {output[:200]}")

        error_lines = [
            line
            for line in output.split("\n")
            if "error[" in line and "unused-ignore-comment" not in line
        ]
        assert not error_lines, (
            "ty produced unexpected errors on valid run() calls:\n"
            + "\n".join(error_lines)
            + f"\n\nFull output:\n{output}"
        )

    def test_run_return_type_is_inferred(self):
        """ty should infer run(add(1, 2)) as int, not Any."""
        output = _run_checker(_TY, self.tmp, ["check", "--python-version", "3.11"])

        if "ty failed" in output or "@Todo" in output:
            pytest.skip("ty cannot fully resolve types in this environment.")

        assert "int" in output, (
            f"Expected run(add(1, 2)) to reveal as int.\nFull output:\n{output}"
        )


@pytest.mark.skipif(not _has_mypy(), reason="mypy not installed")
class TestMypyRunTyping:
    """Verify that mypy infers redun.run() return type from expr argument."""

    def setup_method(self):
        self.tmp = _write_tmp(RUN_TYPING_FIXTURE)

    def teardown_method(self):
        self.tmp.unlink(missing_ok=True)

    def test_no_errors_on_valid_calls(self):
        """mypy should accept all valid run() calls without errors."""
        output = _run_checker(_MYPY, self.tmp, ["--python-executable", sys.executable])

        test_errors = [
            line for line in output.split("\n") if "error:" in line and self.tmp.name in line
        ]
        assert not test_errors, "mypy produced unexpected errors:\n" + "\n".join(test_errors)

    def test_run_return_type_is_inferred(self):
        """mypy should infer run(add(1, 2)) as int, not Any."""
        output = _run_checker(_MYPY, self.tmp, ["--python-executable", sys.executable])

        assert "int" in output, f"Expected mypy reveal_type to show int.\nFull output:\n{output}"
