from taro import warning
from taro.runner import RunnerJobInstance
from taro.test.execution import TestExecution
from taro.warning import WarningCheck


def test_checking():
    execution = TestExecution(wait=True)
    job = RunnerJobInstance('j1', execution)
    warn = TestWarning(2, execution)
    warning.start_checking(job, warn)
    job.run()
    assert warn.checked == 2


class TestWarning(WarningCheck):

    def __init__(self, max_checks: int, execution: TestExecution):
        self.max_checks = max_checks
        self.execution = execution
        self.checked = 0

    def warning_type(self):
        return 'test'

    def next_check(self, job_instance) -> float:
        return 0.1

    def check(self, job_instance):
        self.checked += 1
        if self.checked >= self.max_checks:
            self.execution.release()
        return None
