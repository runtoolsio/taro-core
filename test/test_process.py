"""
Tests :mod:`process` module
"""
from multiprocessing import Pipe

import pytest

from taro import ExecutionState, ExecutionError
from taro.process import ProcessExecution


def test_exec():
    parent, child = Pipe()
    e = ProcessExecution(exec_hello, (child,))
    term_state = e.execute()
    assert parent.recv() == ['hello']
    assert term_state == ExecutionState.COMPLETED


def exec_hello(pipe):
    pipe.send(['hello'])
    pipe.close()


def test_failure_error():
    e = ProcessExecution(exec_failure_error, ())
    with pytest.raises(ExecutionError):
        e.execute()


def exec_failure_error():
    raise AssertionError


def test_failure_exit():
    e = ProcessExecution(exec_failure_exit, ())
    with pytest.raises(ExecutionError):
        e.execute()


def exec_failure_exit():
    exit(1)
