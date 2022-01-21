import ast
import os
from typing import cast
from unittest.mock import patch

import pytest
import sqlalchemy

from redun.cli import get_config_dir, setup_scheduler
from redun.executors.local import LocalExecutor
from redun.scheduler import Config, DryRunResult, Scheduler, subrun
from redun.task import task
from redun.tests.scripts.workflow_subrun_process_executor import main
from redun.tests.utils import use_tempdir

# This is needed to associate the separate scripts/workflow file with this unit test
redun_namespace = "test_subrun"


# All unit tests should use in-memory db
IN_MEMORY_DB_URI = "sqlite:///:memory:"

CONFIG_DICT = {
    "executors.default": {
        "type": "local",
        "mode": "thread",
    },
    "executors.process": {
        "type": "local",
        "mode": "process",
    },
}


# ==========================================================================================
# Verify various configuration are correctly passed to the sub-scheduler and
# subrun_root_task_executor.  All tasks use the default executor.
# ==========================================================================================


def test_missing_executor():
    # Forgetting to supply subrun_root_task_executor raises TypeError
    @task
    def foo(x):
        return x

    @task
    def local_main1(x):
        return subrun(foo(x), CONFIG_DICT)

    @task
    def local_main2(x):
        return subrun(foo(x), CONFIG_DICT, memory=32, vcpus=4)

    scheduler = Scheduler()
    scheduler.load()
    with pytest.raises(TypeError):
        scheduler.run(local_main1(5))

    with pytest.raises(TypeError):
        scheduler.run(local_main2(5))


def test_subscheduler_status_report():
    """sub-scheduler report must contain a line for each expected user task"""

    @task
    def bar(x):
        return x

    @task
    def foo(x):
        return bar(x)

    @task
    def local_main(x):
        return subrun(foo(x), "default", CONFIG_DICT)

    # Assert that the foo and bar tasks appear in local scheduler's log
    scheduler = Scheduler()
    scheduler.load()
    with patch.object(scheduler, "log") as mock_log:
        assert 5 == scheduler.run(local_main(5))
        assert any(["foo" in ",".join(call_args[0]) for call_args in mock_log.call_args_list])
        assert any(["bar" in ",".join(call_args[0]) for call_args in mock_log.call_args_list])


def _config_found_in_logs(mock_log, config_dict):
    """Given a patched scheduler.log(), verify the provided `config_dict` was logged

    Scan all logged lines. Use ast.literal_eval to parse a string into a python structure
    (e.g. dict) then compare the dict with the expected `config_dict`.
    """
    for call_args in mock_log.call_args_list:
        if len(call_args[0]) == 0:
            continue
        try:
            actual_config = ast.literal_eval(call_args[0][0])
            if isinstance(actual_config, dict) and actual_config == config_dict:
                print(f"Verified config: {actual_config}")
                return True
        except Exception:
            pass
    return False


def test_subscheduler_config():
    """Verify that the sub-scheduler uses the provided config"""

    @task
    def foo(x):
        return x

    @task
    def local_main(x):
        return subrun(foo(x), "default", CONFIG_DICT)

    scheduler = Scheduler(Config(CONFIG_DICT))
    scheduler.load()
    with patch.object(scheduler, "log") as mock_log:
        assert 5 == scheduler.run(local_main(5))
        assert _config_found_in_logs(mock_log, CONFIG_DICT)


def test_subscheduler_uses_local_config():
    # Not specifying config results in local Scheduler's config being propagated to
    # sub-scheduler
    @task
    def foo(x):
        return x

    @task
    def local_main(x):
        return subrun(foo(x), "default")

    scheduler = Scheduler(config=Config(CONFIG_DICT))
    scheduler.load()

    with patch.object(scheduler, "log") as mock_log:
        assert 5 == scheduler.run(local_main(5))
        local_config = scheduler.config.get_config_dict()
        assert _config_found_in_logs(mock_log, local_config)


@use_tempdir
def test_subscheduler_uses_config_with_replaced_dir():
    """
    Verify that the sub-scheduler can accept and use a `config_dict` that has config_dir replaced.

    This test simulates using an actual redun.ini file pointing to relative local sqlite database.
    Redun will replace relative paths in the local config with the machine-local config_dir.
    So we call config.get_config_dict(replace_config_dir=) to customize a copy of the local
    config for the sub-scheduler.  We include a test case where the replacement dir is
    non-existent and verify that the sub-scheduler fails.
    """
    os.makedirs(".redun")
    with open(".redun/redun.ini", "w") as out:
        out.write(
            """
[backend]
db_uri = sqlite:///redun.db
[executors.process]
type=local
mode=process
start_method=spawn
workers=10
"""
        )

    # Ensure initial config dir and files are created.
    scheduler = setup_scheduler()
    assert os.path.exists(".redun/redun.ini")
    assert os.path.exists(".redun/redun.db")

    @task
    def foo(x):
        return x

    @task
    def local_main(x, remote_config):
        return subrun(foo(x), "default", config=remote_config, cache=False)

    # Replace the local config's config_dir with a valid directory: "/tmp"
    # Verify that the local config_dir was actually used by the sub-scheduler
    local_conf = scheduler.config.get_config_dict()
    assert get_config_dir() in local_conf["backend"]["config_dir"]
    conf = scheduler.config.get_config_dict(replace_config_dir="/tmp")
    assert conf == {
        "backend": {"config_dir": "/tmp", "db_uri": "sqlite:////tmp/redun.db"},
        "executors.process": {
            "mode": "process",
            "start_method": "spawn",
            "type": "local",
            "workers": "10",
        },
        "repos.default": {"config_dir": "/tmp"},
    }
    with patch.object(scheduler, "log") as mock_log:
        assert 5 == scheduler.run(local_main(5, conf))
        assert _config_found_in_logs(mock_log, conf)

    # Replace the local config's config_dir with a valid directory: "."
    # Verify that the local config_dir was actually used by the sub-scheduler
    conf = scheduler.config.get_config_dict(replace_config_dir=".")
    assert conf == {
        "backend": {"config_dir": ".", "db_uri": "sqlite:///./redun.db"},
        "executors.process": {
            "mode": "process",
            "start_method": "spawn",
            "type": "local",
            "workers": "10",
        },
        "repos.default": {"config_dir": "."},
    }
    with patch.object(scheduler, "log") as mock_log:
        assert 5 == scheduler.run(local_main(5, conf))
        assert _config_found_in_logs(mock_log, conf)

    # Replace the local config's config_dir with a non-existent directory: "/tmp_missing"
    # Verify that the sub-scheduler raises exception because it cannot open the local database.
    conf = scheduler.config.get_config_dict(replace_config_dir="/tmp_missing")
    assert conf == {
        "backend": {"config_dir": "/tmp_missing", "db_uri": "sqlite:////tmp_missing/redun.db"},
        "executors.process": {
            "mode": "process",
            "start_method": "spawn",
            "type": "local",
            "workers": "10",
        },
        "repos.default": {"config_dir": "/tmp_missing"},
    }
    with pytest.raises(sqlalchemy.exc.OperationalError, match=r".*unable to open database file.*"):
        scheduler.run(local_main(5, conf))


def test_subscheduler_run_config():
    """Verify that run_config values are correctly adopted by sub-scheduler"""

    @task
    def foo(x):
        return x

    @task
    def local_main(x):
        return subrun(foo(x), executor="default")

    scheduler = Scheduler(config=Config(CONFIG_DICT))
    scheduler.load()
    with pytest.raises(DryRunResult):
        assert 5 == scheduler.run(local_main(5), dryrun=True)

    # Verify cache=False run config is adopted by sub-scheduler
    with patch.object(scheduler, "log") as mock_log:
        assert 5 == scheduler.run(local_main(5), cache=False)
        assert _config_found_in_logs(
            mock_log,
            {
                "dryrun": False,
                "cache": False,
            },
        )


def test_subrun_nested_list_of_tasks():
    """Verify that subrun() can evaluate a nested list of tasks"""

    @task
    def foo(x):
        return x

    @task
    def local_main(x):
        return subrun([foo(x), [foo(2 * x), foo(3 * x)]], "default", CONFIG_DICT)

    scheduler = Scheduler()
    scheduler.load()
    assert [5, [10, 15]] == scheduler.run(local_main(5))


# ==========================================================================================
# Caching-related tests
# ==========================================================================================


def test_subrun_cached():
    """Subrun "wrapper" task is cached"""
    task_calls = []

    @task(cache=False)
    def foo(x):
        task_calls.append("foo")
        return x ** 2

    @task()
    def local_main(x):
        task_calls.append("local_main")
        return subrun(foo(x), executor="default")

    scheduler = Scheduler()
    scheduler.load()
    assert 25 == scheduler.run(local_main(5))
    assert task_calls == ["local_main", "foo"]

    task_calls = []
    with patch.object(scheduler, "get_cache", wraps=scheduler.get_cache) as get_cache:
        assert 25 == scheduler.run(local_main(5))
        assert task_calls == []
        assert get_cache.call_count == 2
        assert get_cache.call_args_list[0][0][0].task_name == "test_subrun.local_main"
        assert get_cache.call_args_list[1][0][0].task_name == "redun.subrun_root_task"


def test_subrun_root_task_cached():
    """_subrun_root_task is cached by default.  This test disables caching for all other tasks."""
    task_calls = []

    @task(cache=False)
    def foo(x):
        task_calls.append("foo")
        return x ** 2

    @task(cache=False)
    def local_main(x):
        task_calls.append("local_main")
        return subrun(foo(x), executor="default")

    scheduler = Scheduler()
    scheduler.load()
    assert 25 == scheduler.run(local_main(5))
    assert task_calls == ["local_main", "foo"]

    task_calls = []
    with patch.object(scheduler, "get_cache", wraps=scheduler.get_cache) as get_cache:
        assert 25 == scheduler.run(local_main(5))
        # Since _subrun_root_task was cached, foo isn't called
        assert task_calls == ["local_main"]
        assert get_cache.call_count == 1
        assert get_cache.call_args_list[0][0][0].task_name == "redun.subrun_root_task"


def test_subrun_root_task_disabled_cached():
    """Invoker of subrun disables caching so _subrun_root_task is no longer cached."""
    task_calls = []

    @task(cache=False)
    def foo(x):
        task_calls.append("foo")
        return x ** 2

    @task(cache=False)
    def local_main(x):
        task_calls.append("local_main")
        return subrun(foo(x), executor="default", cache=False)  # disable caching

    scheduler = Scheduler()
    scheduler.load()
    assert 25 == scheduler.run(local_main(5))
    assert task_calls == ["local_main", "foo"]

    task_calls = []
    with patch.object(scheduler, "get_cache", wraps=scheduler.get_cache) as get_cache:
        assert 25 == scheduler.run(local_main(5))
        assert task_calls == ["local_main", "foo"]
        assert get_cache.call_count == 0


# ==========================================================================================
# Process executor test
# - A local task runs via `default` (thread) executor, calling subrun()
# - subrun() invokes _subrun_root_task() via `process_main` executor
# - _subrun_root_task() launches the sub-scheduler
# - sub-scheduler runs multiple tasks via `process_sub` executor
#
# Note, both the local and sub-scheduler share the same Config, but the local executor
# references `process_main` whereas the sub-scheduler references `process_sub`.
# ==========================================================================================


@pytest.mark.parametrize("start_method", ["fork", "forkserver", "spawn"])
def test_process_executor(start_method: str):
    """Verify subrun() supports process executor

    All tasks use the process executor so PIDs for each task must differ
    """
    config_dict = {
        "backend": {"db_uri": IN_MEMORY_DB_URI},
        "executors.process_main": {
            "type": "local",
            "mode": "process",
            "start_method": start_method,
        },
        "executors.process_sub": {
            "type": "local",
            "mode": "process",
            "start_method": start_method,
            "max_workers": "33",  # must be string for equality assertion in this test to work
        },
    }

    scheduler = Scheduler(Config(config_dict=config_dict))
    scheduler.load()
    assert cast(LocalExecutor, scheduler.executors["default"]).mode == "thread"

    # main and the all subrun tasks each run on separate spawned processes.
    with patch.object(scheduler, "log") as mock_log:
        pids = scheduler.run(main(start_method, config_dict))
        _subrun_root_task_pid = pids[0][-1]
        subtask_pids = pids[0][:-1]
        local_scheduler_pid = pids[1]

        # _subrun_root_task_pid must be different from local scheduler since we set
        # `executor` to a process executor
        assert _subrun_root_task_pid != local_scheduler_pid

        # The local scheduler pid and the _subrun_root_task_pid must differ from each of the
        # subtask pids (because the subtasks also use process executor.
        # Note: we cannot further assert that each of the subtask PIDs differ from each other
        # as this depends on the process executor's pooling mechanism which may reuse
        # processes.
        assert local_scheduler_pid not in subtask_pids
        assert _subrun_root_task_pid not in subtask_pids

        # Confirm that the sub-scheduler actually used the config_dict we passed it.
        assert _config_found_in_logs(mock_log, config_dict)
