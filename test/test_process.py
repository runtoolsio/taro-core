"""
Tests :mod:`process` module
"""
from multiprocessing import Pipe

from taro import ExecutionState
from taro.process import ProcessExecution


def test_exec():
    parent, child = Pipe()
    e = ProcessExecution(exec_, (child,))
    term_state = e.execute()
    assert parent.recv() == ['hello']
    assert term_state == ExecutionState.COMPLETED


def exec_(pipe):
    pipe.send(['hello'])
    pipe.close()
