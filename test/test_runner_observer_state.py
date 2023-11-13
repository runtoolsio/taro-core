"""
Tests that :mod:`runner` sends correct notification to state observers.
:class:`TestStateObserver` is used for verifying the behavior.
"""

import pytest

import tarotools.taro
import tarotools.taro.jobs.runner as runner
from tarotools.taro import TerminationStatus
from tarotools.taro.jobs import lock
from tarotools.taro.jobs.instance import PhaseTransitionObserver, JobInstanceDetail
from tarotools.taro.test.execution import TestExecution
from tarotools.taro.test.observer import TestPhaseObserver


@pytest.fixture
def observer():
    observer = TestPhaseObserver()
    runner.register_transition_callback(observer)
    yield observer
    runner.deregister_transition_callback(observer)


def test_job_passed(observer: TestPhaseObserver):
    tarotools.taro.run_uncoordinated('j1', TestExecution(TerminationStatus.COMPLETED))

    assert observer.last_job().job_id == 'j1'


def test_execution_completed(observer: TestPhaseObserver):
    tarotools.taro.run_uncoordinated('j1', TestExecution(TerminationStatus.COMPLETED))

    assert observer.exec_state(0) == TerminationStatus.CREATED
    assert observer.exec_state(1) == TerminationStatus.RUNNING
    assert observer.exec_state(2) == TerminationStatus.COMPLETED


def test_execution_raises_exc(observer: TestPhaseObserver):
    exc_to_raise = Exception()
    tarotools.taro.run_uncoordinated('j1', TestExecution(raise_exc=exc_to_raise))

    assert observer.exec_state(0) == TerminationStatus.CREATED
    assert observer.exec_state(1) == TerminationStatus.RUNNING
    assert observer.exec_state(2) == TerminationStatus.ERROR
    assert not observer.exec_error(0)
    assert observer.exec_error(2).unexpected_error == exc_to_raise


def test_observer_raises_exception():
    """
    All exception raised by observer must be captured by runner and not to disrupt job execution
    """
    observer = ExceptionRaisingObserver(Exception('Should be captured by runner'))
    execution = TestExecution(TerminationStatus.COMPLETED)
    job_instance = tarotools.taro.job_instance('j1', execution, state_locker=lock.NullStateLocker())
    job_instance.add_observer_phase_transition(observer)
    job_instance.run()
    assert execution.executed_count() == 1  # No exception thrown before


class ExceptionRaisingObserver(PhaseTransitionObserver):

    def __init__(self, raise_exc: Exception):
        self.raise_exc = raise_exc

    def new_phase(self, job_inst: JobInstanceDetail, previous_phase, new_phase, changed):
        raise self.raise_exc
