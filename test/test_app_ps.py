"""
Tests :mod:`app` module
Command: ps
"""

import time

from taro.execution import ExecutionState
from test.util import run_app, run_app_as_process


def test_job_running(capsys):
    run_app_as_process('exec sleep 1', daemon=True)
    run_ps_and_assert(capsys,
                      lambda out: 'sleep 1' in out and ExecutionState.RUNNING.name.casefold() in out.casefold())


def test_job_waiting(capsys):
    run_app_as_process('exec -w val sleep 1', daemon=True)
    run_ps_and_assert(capsys, lambda out: 'sleep 1' in out and ExecutionState.WAITING.name.casefold() in out.casefold())


def run_ps_and_assert(capsys, assertion):
    for _ in range(0, 10):
        run_app('ps')
        output = capsys.readouterr().out
        if assertion(output):
            return
        time.sleep(0.1)
    assert False
