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


def test_job_passed(observer: TestJobWarningObserver):
    job = RunnerJobInstance('j1', TestExecution(ExecutionState.COMPLETED))
    warn = Warn('test_warn', None)
    job.add_warning(warn)

    assert observer.warnings['test_warn'][0].job_id == 'j1'
    assert observer.warnings['test_warn'][1] == warn
