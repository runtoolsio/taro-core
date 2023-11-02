"""
Tests :mod:`runner` module
"""
import time
from threading import Thread

import tarotools.taro
from tarotools.taro import ProcessExecution, JobInstance, TerminationStatus as ExSt
from tarotools.taro.jobs import lock
from tarotools.taro.jobs.coordination import Latch
from tarotools.taro.run import FailedRun
from tarotools.taro.test.execution import TestExecution
from tarotools.taro.test.observer import TestOutputObserver


def test_executed():
    execution = TestExecution()
    assert execution.executed_count() == 0

    tarotools.taro.run_uncoordinated('j', execution)
    assert execution.executed_count() == 1


def test_state_changes():
    instance = tarotools.taro.run_uncoordinated('j', TestExecution())
    assert instance.lifecycle.phase == ExSt.COMPLETED
    assert instance.lifecycle.phases == [ExSt.CREATED, ExSt.RUNNING, ExSt.COMPLETED]


def test_state_created():
    instance = tarotools.taro.job_instance_uncoordinated('j', TestExecution())
    assert instance.lifecycle.phase == ExSt.CREATED


def test_pending():
    latch = Latch(ExSt.PENDING)
    instance = tarotools.taro.job_instance('j', TestExecution(), latch, state_locker=lock.NullStateLocker())
    t = Thread(target=instance.run)
    t.start()

    wait_for_pending_state(instance)

    assert instance.lifecycle.phase == ExSt.PENDING

    latch.release()
    t.join(timeout=1)

    assert instance.lifecycle.phase == ExSt.COMPLETED
    assert instance.lifecycle.phases == [ExSt.CREATED, ExSt.PENDING, ExSt.RUNNING, ExSt.COMPLETED]


def test_latch_cancellation():
    latch = Latch(ExSt.PENDING)
    instance = tarotools.taro.job_instance('j', TestExecution(), latch, state_locker=lock.NullStateLocker())
    t = Thread(target=instance.run)
    t.start()

    wait_for_pending_state(instance)
    instance.stop()

    t.join(timeout=1)

    assert instance.lifecycle.phase == ExSt.CANCELLED
    assert instance.lifecycle.phases == [ExSt.CREATED, ExSt.PENDING, ExSt.CANCELLED]


def test_cancellation_before_start():
    latch = Latch(ExSt.PENDING)
    instance = tarotools.taro.job_instance('j', TestExecution(), latch, state_locker=lock.NullStateLocker())
    t = Thread(target=instance.run)

    instance.stop()
    t.start()
    t.join(timeout=1)

    assert instance.lifecycle.phase == ExSt.CANCELLED
    assert instance.lifecycle.phases == [ExSt.CREATED, ExSt.CANCELLED]


def test_error():
    execution = TestExecution()
    exception = Exception()
    execution.raise_exception(exception)
    instance = tarotools.taro.run_uncoordinated('j', execution)

    assert instance.lifecycle.phase == ExSt.ERROR
    assert isinstance(instance.run_error, FailedRun)
    assert instance.run_error.termination_status == ExSt.ERROR
    assert instance.run_error.unexpected_error == exception


def wait_for_pending_state(instance: JobInstance):
    """
    Wait for the job to reach waiting state
    """
    wait_count = 0
    while instance.lifecycle.phase != ExSt.PENDING:
        time.sleep(0.1)
        wait_count += 1
        if wait_count > 10:
            assert False  # Hasn't reached PENDING state


def test_output_observer():
    def print_it():
        print("Hello, lucky boy. Where are you today?")

    execution = ProcessExecution(print_it)
    instance = tarotools.taro.job_instance_uncoordinated('j', execution)
    observer = TestOutputObserver()
    instance.add_output_observer(observer)

    instance.run()

    assert observer.outputs[0][1] == "Hello, lucky boy. Where are you today?"


def test_last_output():
    def print_it():
        text = "3\n2\n1\neveryone\nin\nthe\nworld\nis\ndoing\nsomething\nwithout\nme"
        lines = text.split('\n')

        for line in lines:
            print(line)

    execution = ProcessExecution(print_it)
    instance = tarotools.taro.job_instance_uncoordinated('j', execution)
    instance.run()
    assert [out for out, _ in instance.last_output] == "1 everyone in the world is doing something without me".split()
