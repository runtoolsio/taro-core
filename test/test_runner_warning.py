"""
Tests that :mod:`runner` sends correct notification to warning observers.
"""

import pytest

import taro.jobs.runner as runner
from taro import Warn
from taro.jobs import lock
from taro.jobs.execution import ExecutionState
from taro.jobs.runner import RunnerJobInstance
from taro.test.execution import TestExecution  # TODO package import
from taro_test_util import TestWarningObserver


@pytest.fixture
def observer():
    observer = TestWarningObserver()
    runner.register_warning_observer(observer)
    yield observer
    runner.deregister_warning_observer(observer)


def test_warning_added(observer: TestWarningObserver):
    job = RunnerJobInstance('j1', TestExecution(ExecutionState.COMPLETED), state_locker=lock.NullStateLocker())
    warn = Warn('test_warn', None)
    job.add_warning(warn)

    assert job.warnings[warn.name] == 1
    assert observer.warnings['test_warn'][0].job_id == 'j1'
    assert observer.warnings['test_warn'][1] == warn


def test_warning_repeated(observer: TestWarningObserver):
    job = RunnerJobInstance('j1', TestExecution(ExecutionState.COMPLETED), state_locker=lock.NullStateLocker())
    warn = Warn('test_warn1', None)
    updated = Warn('test_warn1', {'p': 1})
    job.add_warning(warn)
    job.add_warning(updated)

    assert job.warnings[warn.name] == 2
