"""
Tests :mod:`app` module
Command: ps
"""
from multiprocessing.context import Process

import time

from taro.execution import ExecutionState
from test.util import run_app


def test_ps(capsys):
    Process(target=run_app, args=('exec sleep 1',), daemon=True).start()
    run_and_assert(capsys, lambda out: 'sleep 1' in out and ExecutionState.TRIGGERED.name.casefold() in out.casefold())


def run_and_assert(capsys, assertion):
    for _ in range(0, 10):
        run_app('ps')
        output = capsys.readouterr().out
        if assertion(output):
            return
        time.sleep(0.1)
    assert False
