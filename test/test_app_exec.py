"""
Tests :mod:`app` module
Command: exec
"""
import os

import pytest

from taro import util, cfg
from taro.jobs import persistence, runner
from taro.jobs.execution import ExecutionState
from taro.test.observer import TestStateObserver
from taro_test_util import run_app, TestWarningObserver, test_db_path
from test.taro_test_util import remove_test_db, run_app_as_process_and_wait


@pytest.fixture(autouse=True)
def observer():
    observer = TestStateObserver()
    runner.register_state_observer(observer)
    yield observer
    runner.deregister_state_observer(observer)


def test_successful(observer: TestStateObserver):
    dir_name = util.unique_timestamp_hex()
    run_app('exec -mc mkdir ' + dir_name)

    assert observer.exec_state(-1) == ExecutionState.COMPLETED
    os.rmdir(dir_name)  # Exc if not existed


def test_invalid_command(observer: TestStateObserver):
    run_app('exec -mc non_existing_command')
    assert observer.exec_state(-1) == ExecutionState.FAILED


def test_failed_command(observer: TestStateObserver):
    run_app('exec -mc ls --no-such-option')
    assert observer.exec_state(-1) == ExecutionState.FAILED


def test_invalid_command_print_to_stderr(capsys):
    run_app('exec -mc non_existing_command')
    assert 'No such file' in capsys.readouterr().err


def test_default_job_id(observer: TestStateObserver):
    run_app('exec -mc echo life is dukkha')
    assert observer.last_job().job_id == 'echo life is dukkha'


def test_explicit_job_id(observer: TestStateObserver):
    run_app('exec -mc --id this_is_an_id echo not an id')
    assert observer.last_job().job_id == 'this_is_an_id'


def test_no_overlap(observer: TestStateObserver):
    run_app_as_process_and_wait('exec -mc --id j1 sleep 2', wait_for=ExecutionState.RUNNING, daemon=True)

    run_app('exec -mc --no-overlap --id j1 echo I love JAVA!')
    assert observer.last_state('j1') == ExecutionState.SKIPPED


def test_job_persisted():
    cfg.persistence_enabled = True
    cfg.persistence_type = 'sqlite'
    cfg.persistence_database = str(test_db_path())

    try:
        run_app('exec --id persisted_job echo')
        assert next(iter(persistence.read_jobs(asc=True))).job_id == 'persisted_job'
    finally:
        remove_test_db()


def test_exec_time_warning():
    observer = TestWarningObserver()
    runner.register_warning_observer(observer)
    try:
        run_app("exec -mc --warn-time=1s sleep 1.2")
    finally:
        runner.deregister_warning_observer(observer)

    assert observer.warnings['exec_time>1.0s']
