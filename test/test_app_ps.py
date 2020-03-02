"""
Tests :mod:`app` module
Command: ps
"""

from taro.execution import ExecutionState
from test.util import run_app, run_app_as_process, run_wait


def test_job_running(capsys):
    pw = run_wait(ExecutionState.RUNNING)
    run_app_as_process('exec sleep 1', daemon=True)
    pw.join()

    run_app('ps')
    output = capsys.readouterr().out

    assert 'sleep 1' in output
    assert ExecutionState.RUNNING.name.casefold() in output.casefold()


def test_job_waiting(capsys):
    pw = run_wait(ExecutionState.WAITING)
    run_app_as_process('exec -w val sleep 1', daemon=True)
    pw.join()

    run_app('ps')
    output = capsys.readouterr().out

    assert 'sleep 1' in output
    assert ExecutionState.WAITING.name.casefold() in output.casefold()
