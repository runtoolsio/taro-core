"""
Tests :mod:`app` module
Command: exec
"""
import pytest

from taro import runner
from taro.execution import ExecutionState
from taro.test.observer import TestObserver
from test.util import run_app


@pytest.fixture
def observer():
    observer = TestObserver()
    runner.register_observer(observer)
    yield observer
    runner.deregister_observer(observer)


def test_successful(observer: TestObserver):
    run_app('exec echo this binary universe')
    assert observer.exec_state(-1) == ExecutionState.COMPLETED


def test_invalid_command(observer: TestObserver):
    run_app('exec non_existing_command')
    assert observer.exec_state(-1) == ExecutionState.FAILED


def test_failed_command(observer: TestObserver):
    run_app('exec ls --no-such-option')
    assert observer.exec_state(-1) == ExecutionState.FAILED


def test_invalid_command_print_to_stderr(capsys):
    run_app('exec --log-stdout off non_existing_command')
    assert 'No such file' in capsys.readouterr().err


def test_default_job_id(observer: TestObserver):
    run_app('exec echo life is dukkha')
    assert observer.last_job().id == 'echo life is dukkha'


def test_explicit_job_id(observer: TestObserver):
    run_app('exec --id this_is_an_id echo not an id')
    assert observer.last_job().id == 'this_is_an_id'
