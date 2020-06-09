from collections import deque
from typing import Union

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

    def __init__(self, w_type, execution: Union[TestExecution, None], *warnings):
        self.type = w_type
        self.execution = execution
        self.warnings = deque(warnings)

    def warning_type(self):
        return self.type

    def next_check(self, job_instance) -> float:
        return 0.1 if self.warnings else -1

    def check(self, job_instance):
        warn = self.warnings.popleft()
        if not self.warnings and self.execution:
            self.execution.release()
        return warn


def test_no_warning(execution, job, observer):
    test_warn = TestWarning('type', execution, None, None)
    warning.start_checking(job, test_warn)
    job.run()

    assert not test_warn.warnings
    assert len(job.warnings) == 0
    assert observer.is_empty()


def test_warning(execution, job, observer):
    warn = Warn('type', {})
    test_warn = TestWarning('type', execution, None, warn)
    warning.start_checking(job, test_warn)
    job.run()

    assert not test_warn.warnings
    assert len(job.warnings) == 1
    assert next(iter(job.warnings)) == warn
    assert len(observer.added) == 1
    assert observer.added[0][0].job_id == job.job_id
    assert observer.added[0][1] == warn


def test_warning_removed(execution, job, observer):
    warn = Warn('type', {})
    test_warn = TestWarning('type', execution, None, warn, None)
    warning.start_checking(job, test_warn)
    job.run()

    assert not test_warn.warnings
    assert len(job.warnings) == 0
    assert len(observer.added) == 1
    assert len(observer.removed) == 1
    assert observer.added[0][0].job_id == job.job_id
    assert observer.added[0][1] == warn
    assert observer.removed[0][0].job_id == job.job_id
    assert observer.removed[0][1] == warn


def test_more_warnings(execution, job, observer):
    warn1 = Warn('type1', {})
    warn2_1 = Warn('type2', {'id': 1})
    warn2_2 = Warn('type2', {'id': 2})
    test_warn1 = TestWarning('type1', execution, None, warn1, None, warn1, None, None)  # This one releases execution
    test_warn2 = TestWarning('type2', None, warn2_1, warn2_2)
    warning.start_checking(job, test_warn1, test_warn2)
    job.run()

    assert not test_warn1.warnings
    assert not test_warn2.warnings
    assert len(job.warnings) == 1
    assert len(observer.added) == 4
    assert len(observer.removed) == 2
    assert next(iter(job.warnings)) == warn2_2
