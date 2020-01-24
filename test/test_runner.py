"""
Tests :mod:`runner` module
"""
from threading import Thread

import time

import taro.runner as runner
from taro.execution import ExecutionState
from taro.job import Job
from taro.runner import RunnerJobInstance
from taro.test.execution import TestExecution  # TODO package import


def test_executed():
    execution = TestExecution()
    assert execution.executed_count() == 0

    runner.run(Job('j1', execution))
    assert execution.executed_count() == 1


def test_state_changes():
    instance = runner.run(Job('j1', TestExecution()))
    assert instance.state == ExecutionState.COMPLETED
    assert instance.state_changes[0][0] == ExecutionState.CREATED
    assert instance.state_changes[1][0] == ExecutionState.TRIGGERED
    assert instance.state_changes[2][0] == ExecutionState.COMPLETED


def test_state_created():
    instance = RunnerJobInstance(Job('j1', TestExecution()))
    assert instance.state == ExecutionState.CREATED


def test_waiting():
    instance = RunnerJobInstance(Job('j1', TestExecution(), wait='w1'))
    t = Thread(target=instance.run)
    t.start()

    wait_count = 0
    while instance.state != ExecutionState.WAITING:
        time.sleep(0.1)
        if wait_count > 10:
            assert False  # Hasn't reached WAITING state

    assert not instance.release('w2')
    assert instance.state == ExecutionState.WAITING

    assert instance.release('w1')
    t.join(timeout=1)
    assert instance.state == ExecutionState.COMPLETED

    assert instance.state_changes[0][0] == ExecutionState.CREATED
    assert instance.state_changes[1][0] == ExecutionState.WAITING
    assert instance.state_changes[2][0] == ExecutionState.TRIGGERED
    assert instance.state_changes[3][0] == ExecutionState.COMPLETED
