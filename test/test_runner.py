"""
Tests :mod:`runner` module
"""
import time
from threading import Thread

import taro.runner as runner
from taro.execution import ExecutionState as ExSt, ExecutionError
from taro.job import Job
from taro.runner import RunnerJobInstance
from taro.test.execution import TestExecution  # TODO package import


def test_executed():
    execution = TestExecution()
    assert execution.executed_count() == 0

    runner.run(Job('j', execution))
    assert execution.executed_count() == 1


def test_state_changes():
    instance = runner.run(Job('j', TestExecution()))
    assert instance.state == ExSt.COMPLETED
    assert instance.states() == [ExSt.CREATED, ExSt.TRIGGERED, ExSt.COMPLETED]


def test_state_created():
    instance = RunnerJobInstance(Job('j', TestExecution()))
    assert instance.state == ExSt.CREATED


def test_waiting():
    instance = RunnerJobInstance(Job('j', TestExecution(), wait='w1'))
    t = Thread(target=instance.run)
    t.start()

    # Wait for the job to reach waiting state
    wait_count = 0
    while instance.state != ExSt.WAITING:
        time.sleep(0.1)
        wait_count += 1
        if wait_count > 10:
            assert False  # Hasn't reached WAITING state

    # Different wait label -> keep waiting
    assert not instance.release('w2')
    assert instance.state == ExSt.WAITING

    # Release and wait for completion
    assert instance.release('w1')
    t.join(timeout=1)

    assert instance.state == ExSt.COMPLETED
    assert instance.states() == [ExSt.CREATED, ExSt.WAITING, ExSt.TRIGGERED, ExSt.COMPLETED]


def test_cancellation():
    instance = RunnerJobInstance(Job('j', TestExecution(), wait='w1'))
    t = Thread(target=instance.run)
    t.start()

    instance.stop()
    t.join(timeout=1)

    assert instance.state == ExSt.CANCELLED
    assert instance.states() == [ExSt.CREATED, ExSt.WAITING, ExSt.CANCELLED]


def test_error():
    execution = TestExecution()
    exception = Exception()
    execution.raise_exception(exception)
    instance = runner.run(Job('j', execution))

    assert instance.state == ExSt.ERROR
    assert isinstance(instance.exec_error, ExecutionError)
    assert instance.exec_error.exec_state == ExSt.ERROR
    assert instance.exec_error.unexpected_error == exception
