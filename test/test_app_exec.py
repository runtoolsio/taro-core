"""
Tests :mod:`app` module
Command: exec
"""
import os

import pytest

from taro import runner, util, persistence
from taro.execution import ExecutionState
from taro.test.observer import TestObserver
from test.util import run_app, create_test_config, remove_test_config, remove_test_db, test_db_path


@pytest.fixture(autouse=True)
def observer():
    observer = TestObserver()
    runner.register_observer(observer)
    yield observer
    runner.deregister_observer(observer)
    remove_test_config()
    remove_test_db()


def test_successful(observer: TestObserver):
    dir_name = util.unique_timestamp_hex()
    run_app('exec mkdir ' + dir_name)

    assert observer.exec_state(-1) == ExecutionState.COMPLETED
    os.rmdir(dir_name)  # Exc if not existed


def test_invalid_command(observer: TestObserver):
    run_app('exec non_existing_command')
    assert observer.exec_state(-1) == ExecutionState.FAILED


def test_failed_command(observer: TestObserver):
    run_app('exec ls --no-such-option')
    assert observer.exec_state(-1) == ExecutionState.FAILED


def test_invalid_command_print_to_stderr(capsys):
    run_app('exec non_existing_command')
    assert 'No such file' in capsys.readouterr().err


def test_default_job_id(observer: TestObserver):
    run_app('exec echo life is dukkha')
    assert observer.last_job().job_id == 'echo life is dukkha'


def test_explicit_job_id(observer: TestObserver):
    run_app('exec --id this_is_an_id echo not an id')
    assert observer.last_job().job_id == 'this_is_an_id'


def test_job_persisted():
    run_app('exec --id persisted_job echo')
    assert persistence.read_jobs(chronological=True)[0].job_id == 'persisted_job'


def test_disable_job_id(observer: TestObserver):
    create_test_config({"persistence": {"enabled": True, "type": "sqlite", "database": str(test_db_path())}})
    run_app('job -C test.yaml disable job_to_disable')
    run_app('exec -C test.yaml --id job_to_disable echo')

    assert observer.last_job().job_id == 'job_to_disable'
    assert observer.exec_state(-1) == ExecutionState.DISABLED
