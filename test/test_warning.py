from collections import deque
from threading import Thread

import time
from typing import Union

import pytest

from taro import warning
from taro.runner import RunnerJobInstance
from taro.test.execution import TestExecution
from taro.test.observer import TestWarnObserver
from taro.warning import WarningCheck, ExecTimeWarning, Warn


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

    def __init__(self, execution: Union[TestExecution, None], *warnings):
        self.execution = execution
        self.warnings = deque(warnings)

    def next_check(self, job_instance) -> float:
        return 0.1 if self.warnings else -1

    def check(self, job_instance, last_check: bool):
        warn = self.warnings.popleft()
        if not self.warnings and self.execution:
            self.execution.release()
        return warn


def test_no_warning(execution, job, observer):
    test_warn = TestWarning(execution, None, None)
    checking = warning.start_checking(job, test_warn)
    job.run()
    checking.wait_for_finish()

    assert not test_warn.warnings
    assert len(job.warnings) == 0
    assert observer.is_empty()


def test_warning(execution, job, observer):
    test_warn = TestWarning(execution, None, Warn('w1', None), None)
    checking = warning.start_checking(job, test_warn)
    job.run()
    checking.wait_for_finish()

    assert not test_warn.warnings
    assert len(job.warnings) == 1
    assert next(iter(job.warnings)).id == 'w1'
    assert len(observer.events) == 1
    assert observer.events[0][0].job_id == job.job_id
    assert observer.events[0][1].id == 'w1'


def test_more_warnings(execution, job, observer):
    # This one releases execution:
    test_warn1 = TestWarning(execution, None, Warn('w1', {'p': 1}), None, Warn('w1', {'p': 2}), None, None)
    test_warn2 = TestWarning(None, Warn('w2', None))
    checking = warning.start_checking(job, test_warn1, test_warn2)
    job.run()
    checking.wait_for_finish()

    assert not test_warn1.warnings
    assert not test_warn2.warnings
    assert len(job.warnings) == 2
    assert len(observer.events) == 3


def test_exec_time_warning(execution, job, observer):
    warning.start_checking(job, ExecTimeWarning('wid', 0.5))
    run_thread = Thread(target=job.run)
    run_thread.start()

    assert not observer.events
    time.sleep(0.1)
    assert not observer.events
    time.sleep(0.5)

    execution.release()
    run_thread.join(1)
    assert len(observer.events) == 1
