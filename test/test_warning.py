from collections import deque

import pytest

from taro import warning
from taro.runner import RunnerJobInstance
from taro.test.execution import TestExecution
from taro.test.observer import TestWarnObserver
from taro.warning import WarningCheck, Warn


@pytest.fixture
def execution():
    return TestExecution(wait=True)


@pytest.fixture
def job(execution):
    return RunnerJobInstance('j1', execution)


@pytest.fixture
def observer(job):
    observer = TestWarnObserver()
    job.add_warning_observer(observer)
    return observer


class TestWarning(WarningCheck):

    def __init__(self, execution: TestExecution, *warnings):
        self.execution = execution
        self.warnings = deque(warnings)

    def warning_type(self):
        return 'test'

    def next_check(self, job_instance) -> float:
        return 0.1 if self.warnings else -1

    def check(self, job_instance):
        warn = self.warnings.popleft()
        if not self.warnings:
            self.execution.release()
        return warn


def test_no_warning(execution, job, observer):
    test_warn = TestWarning(execution, None, None)
    warning.start_checking(job, test_warn)
    job.run()

    assert not test_warn.warnings
    assert len(job.warnings) == 0
    assert observer.is_empty()


def test_warning(execution, job, observer):
    warn = Warn('test', {})
    test_warn = TestWarning(execution, None, warn)
    warning.start_checking(job, test_warn)
    job.run()

    assert not test_warn.warnings
    assert len(job.warnings) == 1
    assert next(iter(job.warnings)) == warn
    assert len(observer.added) == 1
    assert observer.added[0][0].job_id == job.job_id
    assert observer.added[0][1] == warn
