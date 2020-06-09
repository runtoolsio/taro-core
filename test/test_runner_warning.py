"""
Tests that :mod:`runner` sends correct notification to warning observers.
"""

import pytest

import taro.runner as runner
from taro.execution import ExecutionState
from taro.runner import RunnerJobInstance
from taro.test.execution import TestExecution  # TODO package import
from taro.warning import Warn
from test.util import TestJobWarningObserver


@pytest.fixture
def observer():
    observer = TestJobWarningObserver()
    runner.register_warning_observer(observer)
    yield observer
    runner.deregister_warning_observer(observer)


def test_warning_added(observer: TestJobWarningObserver):
    job = RunnerJobInstance('j1', TestExecution(ExecutionState.COMPLETED))
    warn = Warn('test_warn', None)
    job.add_warning(warn)

    assert next(iter(job.warnings)) == warn
    assert observer.warnings['test_warn'][0].job_id == 'j1'
    assert observer.warnings['test_warn'][1] == warn


def test_warning_removed(observer: TestJobWarningObserver):
    job = RunnerJobInstance('j1', TestExecution(ExecutionState.COMPLETED))
    warn1 = Warn('test_warn1', None)
    warn2 = Warn('test_warn2', None)
    job.add_warning(warn1)
    job.add_warning(warn2)
    job.remove_warning('test_warn1')

    assert len(job.warnings) == 1
    assert next(iter(job.warnings)) == warn2