from collections import deque

from taro import warning
from taro.runner import RunnerJobInstance
from taro.test.execution import TestExecution
from taro.test.observer import TestWarnObserver
from taro.warning import WarningCheck, Warn


def test_no_warning():
    execution = TestExecution(wait=True)
    job = RunnerJobInstance('j1', execution)
    warn_observer = TestWarnObserver()
    job.add_warning_observer(warn_observer)
    test_warn = TestWarning(execution, None, None)

    warning.start_checking(job, test_warn)
    job.run()

    assert not test_warn.warnings
    assert len(job.warnings) == 0
    assert warn_observer.is_empty()


def test_warning():
    execution = TestExecution(wait=True)
    job = RunnerJobInstance('j1', execution)
    warn_observer = TestWarnObserver()
    job.add_warning_observer(warn_observer)
    warn = Warn('test', {})
    test_warn = TestWarning(execution, None, warn)

    warning.start_checking(job, test_warn)
    job.run()

    assert not test_warn.warnings
    assert len(job.warnings) == 1
    assert next(iter(job.warnings)) == warn
    assert len(warn_observer.added) == 1
    assert warn_observer.added[0][0].job_id == job.job_id
    assert warn_observer.added[0][1] == warn


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
