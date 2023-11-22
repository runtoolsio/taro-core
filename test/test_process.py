"""
Tests :mod:`process` module
"""
from multiprocessing import Pipe
from threading import Thread
from time import sleep

import pytest

from tarotools.taro.process import ProcessExecution
from tarotools.taro.run import TerminationStatus, FailedRun


def test_exec():
    parent, child = Pipe()
    e = ProcessExecution(exec_hello, (child,))
    term_state = e.execute()
    assert parent.recv() == ['hello']
    assert term_state == TerminationStatus.COMPLETED


def exec_hello(pipe):
    pipe.send(['hello'])
    pipe.close()


def test_failure_error():
    e = ProcessExecution(exec_failure_error, ())
    with pytest.raises(FailedRun):
        e.execute()


def exec_failure_error():
    raise AssertionError


def test_failure_exit():
    e = ProcessExecution(exec_failure_exit, ())
    with pytest.raises(FailedRun):
        e.execute()


def exec_failure_exit():
    exit(1)


# @pytest.mark.skip(reason="Hangs tests executed for all project")  # TODO
def test_stop():
    e = ProcessExecution(exec_never_ending_story, ())
    t = Thread(target=stop_after, args=(0.5, e))
    t.start()
    term_state = e.execute()
    assert term_state == TerminationStatus.STOPPED


def exec_never_ending_story():
    while True:
        sleep(0.1)


def stop_after(sec, execution):
    sleep(sec)
    execution.stop()
