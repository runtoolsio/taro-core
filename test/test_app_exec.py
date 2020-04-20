"""
Tests :mod:`app` module
Command: exec
"""
import os

import pytest

from taro import runner, util, app
from taro.execution import ExecutionState
from taro.test.observer import TestObserver
from test_plugin_valid import LISTENER
from test.util import run_app, remove_test_config, create_test_config


@pytest.fixture
def observer():
    observer = TestObserver()
    runner.register_observer(observer)
    yield observer
    runner.deregister_observer(observer)


def test_successful(capsys, observer: TestObserver):
    dir_name = util.unique_timestamp_hex()
    run_app('exec mkdir ' + dir_name)

    assert observer.exec_state(-1) == ExecutionState.COMPLETED
    os.rmdir(dir_name)


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


def test_plugin_executed():
    plugin_prefix = app.PLUGIN_MODULE_PREFIX
    app.PLUGIN_MODULE_PREFIX = 'test_'
    try:
        create_test_config({"plugins": ["test_plugin_valid"]})  # Use testing plugin in module 'test'
        run_app('exec -C test.yaml --id run_with_test_plugin echo plugin')
    finally:
        app.PLUGIN_MODULE_PREFIX = plugin_prefix
        remove_test_config()

    assert LISTENER.last_job().job_id == 'run_with_test_plugin'
