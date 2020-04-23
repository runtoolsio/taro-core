"""
Tests :mod:`runner` module
"""
from threading import Thread

import time

import taro.runner as runner
from taro.execution import ExecutionState as ExSt, ExecutionError
from taro.job import Job
from taro.runner import RunnerJobInstance
from taro.test.execution import TestExecution  # TODO package import


def test_executed():
    execution = TestExecution()
    assert execution.executed_count() == 0

    runner.run(Job('j'), execution)
    assert execution.executed_count() == 1


def test_state_changes():
    instance = runner.run(Job('j'), TestExecution())
    assert instance.lifecycle.state() == ExSt.COMPLETED
    assert instance.lifecycle.states() == [ExSt.CREATED, ExSt.RUNNING, ExSt.COMPLETED]


def test_state_created():
    instance = RunnerJobInstance(Job('j'), TestExecution())
    assert instance.lifecycle.state() == ExSt.CREATED


def test_pending():
    instance = RunnerJobInstance(Job('j', pending='w1'), TestExecution())
    t = Thread(target=instance.run)
    t.start()

    wait_for_pending_state(instance)

    # Different wait label -> keep waiting
    assert not instance.release('w2')
    assert instance.lifecycle.state() == ExSt.PENDING

    # Release and wait for completion
    assert instance.release('w1')
    t.join(timeout=1)

    assert instance.lifecycle.state() == ExSt.COMPLETED
    assert instance.lifecycle.states() == [ExSt.CREATED, ExSt.PENDING, ExSt.RUNNING, ExSt.COMPLETED]


def test_cancellation_after_start():
    instance = RunnerJobInstance(Job('j', pending='w1'), TestExecution())
    t = Thread(target=instance.run)
    t.start()

    wait_for_pending_state(instance)
    instance.release('w1')

    instance.stop()
    t.join(timeout=1)

    assert instance.lifecycle.state() == ExSt.CANCELLED
    assert instance.lifecycle.states() == [ExSt.CREATED, ExSt.PENDING, ExSt.CANCELLED]


def test_cancellation_before_start():
    instance = RunnerJobInstance(Job('j', pending='w1'), TestExecution())
    t = Thread(target=instance.run)

    instance.stop()
    t.start()
    t.join(timeout=1)

    assert instance.lifecycle.state() == ExSt.CANCELLED
    assert instance.lifecycle.states() == [ExSt.CREATED, ExSt.CANCELLED]


def test_error():
    execution = TestExecution()
    exception = Exception()
    execution.raise_exception(exception)
    instance = runner.run(Job('j'), execution)

    assert instance.lifecycle.state() == ExSt.ERROR
    assert isinstance(instance.exec_error, ExecutionError)
    assert instance.exec_error.exec_state == ExSt.ERROR
    assert instance.exec_error.unexpected_error == exception


def wait_for_pending_state(instance: RunnerJobInstance):
    """
    Wait for the job to reach waiting state
    """
    wait_count = 0
    while instance.lifecycle.state() != ExSt.PENDING:
        time.sleep(0.1)
        wait_count += 1
        if wait_count > 10:
            assert False  # Hasn't reached PENDING state
