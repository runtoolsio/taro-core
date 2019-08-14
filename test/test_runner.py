"""
Tests :mod:`runner` module
"""

import taro.runner as runner
from taro.job import Job
from taro.test.execution import TestExecution  # TODO package import


def test_executed():
    execution = TestExecution()
    assert execution.executed_count() == 0

    runner.run(Job('c', 'n', execution))
    assert execution.executed_count() == 1
