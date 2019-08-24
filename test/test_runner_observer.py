"""
Tests that :mod:`runner` sends correct notification to observers.
:class:`TestObserver` is used for verifying the behavior.
"""

import pytest

from taro.execution import ExecutionState
from taro.job import Job
from taro.test.execution import TestExecution  # TODO package import
from taro.test.observer import TestObserver
import taro.runner as runner


@pytest.fixture
def observer():
    observer = TestObserver(True)
    runner.register_observer(observer)
    return observer


def job(after_exec_state: ExecutionState = None, raise_exc: Exception = None):
    return Job('j1', TestExecution(after_exec_state, raise_exc))


def test_job_passed(observer: TestObserver):
    j = job(ExecutionState.COMPLETED)
    runner.run(j)

    assert observer.wait_for_terminal_state()
    assert observer.last_job() == j


def test_execution_completed(observer: TestObserver):
    runner.run(job(ExecutionState.COMPLETED))

    assert observer.wait_for_terminal_state()
    assert observer.exec_state(0) == ExecutionState.TRIGGERED
    assert observer.exec_state(1) == ExecutionState.COMPLETED


def test_execution_started(observer: TestObserver):
    runner.run(job(ExecutionState.STARTED))

    assert observer.wait_for_state(ExecutionState.STARTED)
    assert observer.exec_state(0) == ExecutionState.TRIGGERED
    assert observer.exec_state(1) == ExecutionState.STARTED


def test_execution_raises_exc(observer: TestObserver):
    exc_to_raise = Exception()
    runner.run(job(raise_exc=exc_to_raise))

    assert observer.wait_for_terminal_state()
    assert observer.exec_state(0) == ExecutionState.TRIGGERED
    assert observer.exec_state(1) == ExecutionState.ERROR
    assert not observer.exec_error(0)
    assert observer.exec_error(1).unexpected_error == exc_to_raise
