"""
Tests that :mod:`runner` sends correct notification to observers.
:class:`TestObserver` is used for verifying the behavior.
"""

import pytest

import taro.runner as runner
from taro.execution import ExecutionState
from taro.job import Job
from taro.test.execution import TestExecution  # TODO package import
from taro.test.observer import TestObserver


@pytest.fixture
def observer():
    observer = TestObserver()
    runner.register_observer(observer)
    yield observer
    runner.deregister_observer(observer)


def job(after_exec_state: ExecutionState = None, raise_exc: Exception = None):
    return Job('j1', TestExecution(after_exec_state, raise_exc))


def test_job_passed(observer: TestObserver):
    j = job(ExecutionState.COMPLETED)
    runner.run(j)

    assert observer.last_job().job_id == j.id


def test_execution_completed(observer: TestObserver):
    runner.run(job(ExecutionState.COMPLETED))

    assert observer.exec_state(0) == ExecutionState.CREATED
    assert observer.exec_state(1) == ExecutionState.TRIGGERED
    assert observer.exec_state(2) == ExecutionState.COMPLETED


def test_execution_started(observer: TestObserver):
    runner.run(job(ExecutionState.STARTED))

    assert observer.exec_state(0) == ExecutionState.CREATED
    assert observer.exec_state(1) == ExecutionState.TRIGGERED
    assert observer.exec_state(2) == ExecutionState.STARTED


def test_execution_raises_exc(observer: TestObserver):
    exc_to_raise = Exception()
    runner.run(job(raise_exc=exc_to_raise))

    assert observer.exec_state(0) == ExecutionState.CREATED
    assert observer.exec_state(1) == ExecutionState.TRIGGERED
    assert observer.exec_state(2) == ExecutionState.ERROR
    assert not observer.exec_error(0)
    assert observer.exec_error(2).unexpected_error == exc_to_raise
