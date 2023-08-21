"""
Tests that :mod:`runner` sends correct notification to state observers.
:class:`TestStateObserver` is used for verifying the behavior.
"""

import pytest

import tarotools.taro.jobs.runner as runner
from tarotools.taro.jobs import lock
from tarotools.taro.jobs.execution import ExecutionState
from tarotools.taro.jobs.inst import InstanceStateObserver, JobInst
from tarotools.taro.jobs.runner import RunnerJobInstance
from tarotools.taro.test.execution import TestExecution
from tarotools.taro.test.observer import TestStateObserver


@pytest.fixture
def observer():
    observer = TestStateObserver()
    runner.register_state_observer(observer)
    yield observer
    runner.deregister_state_observer(observer)


def test_job_passed(observer: TestStateObserver):
    runner.run('j1', TestExecution(ExecutionState.COMPLETED), lock.NullStateLocker())

    assert observer.last_job().job_id == 'j1'


def test_execution_completed(observer: TestStateObserver):
    runner.run('j1', TestExecution(ExecutionState.COMPLETED), lock.NullStateLocker())

    assert observer.exec_state(0) == ExecutionState.CREATED
    assert observer.exec_state(1) == ExecutionState.RUNNING
    assert observer.exec_state(2) == ExecutionState.COMPLETED


def test_execution_raises_exc(observer: TestStateObserver):
    exc_to_raise = Exception()
    runner.run('j1', TestExecution(raise_exc=exc_to_raise), lock.NullStateLocker())

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
    job_instance = RunnerJobInstance('j1', execution, state_locker=lock.NullStateLocker())
    job_instance.add_state_observer(observer)
    job_instance.run()
    assert execution.executed_count() == 1  # No exception thrown before


class ExceptionRaisingObserver(InstanceStateObserver):

    def __init__(self, raise_exc: BaseException):
        self.raise_exc = raise_exc

    def new_instance_state(self, job_inst: JobInst, previous_state, new_state, changed):
        raise self.raise_exc
