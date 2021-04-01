"""
Tests that :mod:`runner` sends correct notification to state observers.
:class:`TestStateObserver` is used for verifying the behavior.
"""

import pytest

import taro.runner as runner
from taro.execution import ExecutionState
from taro.job import ExecutionStateObserver, JobInfo
from taro.runner import RunnerJobInstance
from taro.test.execution import TestExecution  # TODO package import
from taro.test.observer import TestStateObserver


@pytest.fixture
def observer():
    observer = TestStateObserver()
    runner.register_state_observer(observer)
    yield observer
    runner.deregister_state_observer(observer)


def test_job_passed(observer: TestStateObserver):
    runner.run('j1', TestExecution(ExecutionState.COMPLETED))

    assert observer.last_job().job_id == 'j1'


def test_execution_completed(observer: TestStateObserver):
    runner.run('j1', TestExecution(ExecutionState.COMPLETED))

    assert observer.exec_state(0) == ExecutionState.CREATED
    assert observer.exec_state(1) == ExecutionState.RUNNING
    assert observer.exec_state(2) == ExecutionState.COMPLETED


def test_execution_started(observer: TestStateObserver):
    runner.run('j1', TestExecution(ExecutionState.STARTED))

    assert observer.exec_state(0) == ExecutionState.CREATED
    assert observer.exec_state(1) == ExecutionState.RUNNING
    assert observer.exec_state(2) == ExecutionState.STARTED


def test_execution_raises_exc(observer: TestStateObserver):
    exc_to_raise = Exception()
    runner.run('j1', TestExecution(raise_exc=exc_to_raise))

    assert observer.exec_state(0) == ExecutionState.CREATED
    assert observer.exec_state(1) == ExecutionState.RUNNING
    assert observer.exec_state(2) == ExecutionState.ERROR
    assert not observer.exec_error(0)
    assert observer.exec_error(2).unexpected_error == exc_to_raise


def test_observer_raises_exception():
    """
    All exception raised by observer must be captured by runner and not to disrupt job execution
    """
    observer = ExceptionRaisingObserver(BaseException('Should be captured by runner'))
    execution = TestExecution(ExecutionState.COMPLETED)
    job_instance = RunnerJobInstance('j1', execution)
    job_instance.add_state_observer(observer)
    job_instance.run()
    assert execution.executed_count() == 1  # No exception thrown before


class ExceptionRaisingObserver(ExecutionStateObserver):

    def __init__(self, raise_exc: BaseException):
        self.raise_exc = raise_exc

    def state_update(self, job_info: JobInfo):
        raise self.raise_exc
