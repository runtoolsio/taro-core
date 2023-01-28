"""
Tests :mod:`runner` module
"""
import time
from threading import Thread

import taro.jobs.runner as runner
from taro.jobs import lock
from taro.jobs.execution import ExecutionState as ExSt, ExecutionError
from taro.jobs.program import ProgramExecution
from taro.jobs.runner import RunnerJobInstance
from taro.jobs.sync import Latch
from taro.test.execution import TestExecution  # TODO package import


def test_executed():
    execution = TestExecution()
    assert execution.executed_count() == 0

    runner.run('j', execution, lock.NullStateLocker())
    assert execution.executed_count() == 1


def test_state_changes():
    instance = runner.run('j', TestExecution(), lock.NullStateLocker())
    assert instance.lifecycle.state == ExSt.COMPLETED
    assert instance.lifecycle.states == [ExSt.CREATED, ExSt.RUNNING, ExSt.COMPLETED]


def test_state_created():
    instance = RunnerJobInstance('j', TestExecution(), state_locker=lock.NullStateLocker())
    assert instance.lifecycle.state == ExSt.CREATED


def test_pending():
    latch = Latch(ExSt.PENDING)
    instance = RunnerJobInstance('j', TestExecution(), latch, state_locker=lock.NullStateLocker())
    t = Thread(target=instance.run)
    t.start()

    wait_for_pending_state(instance)

    assert instance.lifecycle.state == ExSt.PENDING

    latch.release()
    t.join(timeout=1)

    assert instance.lifecycle.state == ExSt.COMPLETED
    assert instance.lifecycle.states == [ExSt.CREATED, ExSt.PENDING, ExSt.RUNNING, ExSt.COMPLETED]


def test_latch_cancellation():
    latch = Latch(ExSt.PENDING)
    instance = RunnerJobInstance('j', TestExecution(), latch, state_locker=lock.NullStateLocker())
    t = Thread(target=instance.run)
    t.start()

    wait_for_pending_state(instance)
    instance.stop()

    t.join(timeout=1)

    assert instance.lifecycle.state == ExSt.CANCELLED
    assert instance.lifecycle.states == [ExSt.CREATED, ExSt.PENDING, ExSt.CANCELLED]


def test_cancellation_before_start():
    latch = Latch(ExSt.PENDING)
    instance = RunnerJobInstance('j', TestExecution(), latch, state_locker=lock.NullStateLocker())
    t = Thread(target=instance.run)

    instance.stop()
    t.start()
    t.join(timeout=1)

    assert instance.lifecycle.state == ExSt.CANCELLED
    assert instance.lifecycle.states == [ExSt.CREATED, ExSt.CANCELLED]


def test_error():
    execution = TestExecution()
    exception = Exception()
    execution.raise_exception(exception)
    instance = runner.run('j', execution, lock.NullStateLocker())

    assert instance.lifecycle.state == ExSt.ERROR
    assert isinstance(instance.exec_error, ExecutionError)
    assert instance.exec_error.exec_state == ExSt.ERROR
    assert instance.exec_error.unexpected_error == exception


def wait_for_pending_state(instance: RunnerJobInstance):
    """
    Wait for the job to reach waiting state
    """
    wait_count = 0
    while instance.lifecycle.state != ExSt.PENDING:
        time.sleep(0.1)
        wait_count += 1
        if wait_count > 10:
            assert False  # Hasn't reached PENDING state


def test_last_output():
    execution = ProgramExecution('echo', "3\n2\n1\neveryone\nin\nthe\nworld\nis\ndoing\nsomething\nwithout\nme",
                                 read_output=True)
    instance = RunnerJobInstance('j', execution, state_locker=lock.NullStateLocker())
    instance.run()
    assert [out for out, _ in instance.last_output] == "1 everyone in the world is doing something without me".split()
