from collections import deque
from typing import Union

import pytest

from taro import warning
from taro.runner import RunnerJobInstance
from taro.test.execution import TestExecution
from taro.test.observer import TestWarnObserver
from taro.warning import WarningCheck


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

    def __init__(self, w_id, execution: Union[TestExecution, None], *warnings):
        self.id = w_id
        self.execution = execution
        self.warnings = deque(warnings)

    def warning_id(self):
        return self.id

    def next_check(self, job_instance) -> float:
        return 0.1 if self.warnings else -1

    def check(self, job_instance):
        warn = self.warnings.popleft()
        if not self.warnings and self.execution:
            self.execution.release()
        return warn


def test_no_warning(execution, job, observer):
    test_warn = TestWarning('w1', execution, False, False)
    warning.start_checking(job, test_warn)
    job.run()

    assert not test_warn.warnings
    assert len(job.warnings) == 0
    assert observer.is_empty()


def test_warning(execution, job, observer):
    test_warn = TestWarning('w1', execution, False, True)
    warning.start_checking(job, test_warn)
    job.run()

    assert not test_warn.warnings
    assert len(job.warnings) == 1
    assert next(iter(job.warnings)).id == 'w1'
    assert len(observer.added) == 1
    assert observer.added[0][0].job_id == job.job_id
    assert observer.added[0][1].id == 'w1'


def test_warning_removed(execution, job, observer):
    test_warn = TestWarning('w1', execution, False, True, False)
    warning.start_checking(job, test_warn)
    job.run()

    assert not test_warn.warnings
    assert len(job.warnings) == 0
    assert len(observer.added) == 1
    assert len(observer.removed) == 1
    assert observer.added[0][0].job_id == job.job_id
    assert observer.added[0][1].id == 'w1'
    assert observer.removed[0][0].job_id == job.job_id
    assert observer.removed[0][1].id == 'w1'


def test_more_warnings(execution, job, observer):
    test_warn1 = TestWarning('w1', execution, False, True, False, True, False, False)  # This one releases execution
    test_warn2 = TestWarning('w2', None, True, True)
    warning.start_checking(job, test_warn1, test_warn2)
    job.run()

    assert not test_warn1.warnings
    assert not test_warn2.warnings
    assert len(job.warnings) == 1
    print(observer.added)
    assert len(observer.added) == 3
    assert len(observer.removed) == 2
    assert next(iter(job.warnings)).id == 'w2'
